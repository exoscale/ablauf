ablauf: long-running workflow management
========================================

[![cljdoc badge](https://cljdoc.xyz/badge/exoscale/ablauf)](https://cljdoc.org/d/exoscale/ablauf/CURRENT/api/exoscale.ablauf)
[![Clojars Project](https://img.shields.io/clojars/v/exoscale/ablauf.svg)](https://clojars.org/exoscale/ablauf)

Ablauf is intended to provide a simple way to manage long-running
workflows of actions reaching out to multiple different systems.  It
provides a way to **express**, **run**, and **inspect**
workflows. Workflows consist of a series of sequential of parallel
steps, with minimal flow control.

- **express**: Workflows are defined through either data or a
  simplistic Clojure DSL. See [Defining
  workflows](#defining_workflows).
- **run**: Ablauf workflows can be ran in multiple manners. See
  [Workflow runners](#workflow_runners).
- **inspect**: Workflow current state can be stored in a secondary
  archival store for inspecting currently running and completed
  workflows.

### What would I use this for?

Ablauf is interesting if you want to

- Need to decouple complex workflow declaration from your code, to
  more easily inspect them.
- Want to decouple workflow instantiation from workflow execution.
- Want to have a simple way to mock workflow execution in testing
  environments.

### Basics

#### An example language

Most asynchronous workflows rely on basic flow control:

- Ability to execute long running tasks in parallel
- Sequential execution
- Basic rescue mechanism

Resulting ASTs for such programs are extremely limited. Syntax
trees can be modelled with four node types:

- Leaves (actions to be performed)
- Sequential branches (actions in sequence)
- Parallel branches (actions in parallel)
- Try branches (try/rescue/finally triples)

So a simple program like:

```clojure
(dopar!!
  (log!! "a")
  (do!!
    (log!! "b")
	(log!! "c"))
  (try!!
    (log!! "d")
	(rescue!! (log!! "e"))
	(finally!! (log!! "f"))))
```

Would result in the following tree:

![AST view](doc/ast.png)

This is what `ablauf.ast` provides, with a corresponding spec.

### Workflow execution

Now that a simplistic but sufficient AST exists, comes the question of its
execution. It would be trivial to walk the above tree and execute things
as they are found. The notion of parallel nodes makes things a bit less
obvious.

Long running workflows bring three additional requirements to the table:

- Execution should be able to restart from a previous known-state
- Workflows might need to execute in different contexts
- Workflow statuses should be inspectable

The first requirement mandates that workflows should do their best to
provide high availability, the second mandates that workflows should
be decoupled from their execution environment.

#### An abstract workflow execution library

To fulfill the above requirements, it was assumed that workflows would
either be processed from a manifold-based single-process environment
or from a durable queue, depending on the workflow type.
So can a simple AST's execution be separated from its execution environment?

The proposed model here takes inspiration from *Continuation Passing Style*
(CPS), but proposes passing the resulting syntax tree instead of a procedure.

Execution of a program results in feeding the result of previously dispatched
actions to a reducer which generates new potential actions to dispatch. This
can be provided with the following signatures

```haskell
make_job    :: AST -> Job
restart_job :: Job -> [Results] -> Job -> [Actions]
```

Here `make_job`, generates a workflow ready for execution.
`restart_job` given a set of results would yield an updated
workflow and follow-up actions to take. The namespace in `ablauf.job`
provides exactly this functionality for the AST described above.

This was simplified by `clojure.zip`'s zippers, a data structure which
stores a tree and the position in that tree, making walking and
storing AST state that much easier. Gérard Huet wrote a
[paper](https://www.st.cs.uni-saarland.de/edu/seminare/2005/advanced-fp/docs/huet-zipper.pdf)
which inspired `clojure.zip`, a recommended read.

### Defining workflows

Workflows consist of a data structure which honors the
`:ablauf.job.ast/ast` spec. Functions in the `ablauf.job.ast` namespace
provide a simple DSL to produce valid workflows.

Let's say that you want to implement a workflow which creates a git
repository, then a declare a CI job and sentry project in parallel,
and finally sends a notification to indicate success.

``` clojure
(defn create-deployable [name]
  (ast/do!!
    (ast/action!! :git/create-repository {:name name})
    (ast/dopar!!
      (ast/action!! :jenkins/create-build {:name name})
      (ast/action!! :sentry/create-project {:name name}))
    (ast/action!! :chat/send-message {:message (str "project " name " succesfully created.")})))
```

#### Workflow actions and context

Workflow leaves are abstract actions and depend on an **action
function** to be performed. They all require a payload, and produce an
output. Throughout the workflow execution, a **context** map is provided
which can be augmented by the result of individual actions.

To pull data out of an action into the context, `ablauf.job.ast/with-augment` can be used:

``` clojure
;; Store the created DSN at the `:sentry/dsn` key in the context.
(with-augment [:sentry/dsn :sentry/dsn]
  (action!! :sentry/create-project {:name name}))
```

### Workflow runners

Workflow runners walk through a workflow and invoke an action function
as necessary.  Ablauf comes with two bundled runners:

- `ablauf.job.manifold`: An in-memory processor based on
  [manifold](aleph.io).
- `ablauf.job.sql`: A SQL-backed processor which decouples job
  submission and job running.

### Terminology

#### AST

AST or abstract syntax tree, is the representation of the ablauf
program. It can consist of the following nodes:

* `:ast/leaf`: A node without children. Represents an action the
  program should take.
* `:ast/seq`: Represents a list of actions that will be executed
  sequentially.
* `:ast/par`: Represents a list of actions that will be executed in
  parallel.
* `:ast/try`: A node that contains at most 3 children:
    * Forms: An `:ast/seq` containing the actions to be tried
    * Rescue: An `:ast/seq` containing the actions to be executed if
      the ast in form returns an error
    * Finally: An `:ast/seq` containing actions that will be executed
      after either forms or rescue completes.

#### Dispatcher

An `:ast/leaf`, which represents an action performed by the program,
is defined by the following spec

``` clojure
(defmethod spec-by-ast-type :ast/leaf
  [_]
  (s/keys :req [:ast/action :ast/payload]))


(s/def :ast/action    keyword?)
(s/def :ast/payload   any?)
```

The `::ast/action` is a keyword that represents the action to be
performed. A dispatcher matches this keyword to the actual
implementation.  By default, the manifold runner uses the
`dispatch-action` multimethod to route the action to a function that
knows how to handle it, based on the `::ast/action` key. It is
possible to supply your own dispatcher implementation using the
`:action-fn` key in `manifold/runner`.

### Caveats

This solution has a few obvious problems:

#### Storage bloat

Storing the full AST tree, including results, for each step will limit
the size of workflows that can be created.
