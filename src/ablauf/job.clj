(ns ablauf.job
  "
  An abstract program runtime for ASTs produced by `ablauf.job.ast`.
  This exposes two functions for the outside world: `make`
  and `restart`

  You are expected to create a *program* to be ran with the functions
  available in the `ablauf.job.ast` namespace. This program is then
  initialized with the rough equivalent of an instruction pointer and
  a context.

  This namespace makes no guess as to how program instructions are
  actually performed, instead, for each position of the program,
  it yields the instructions to carry out. There might be more than
  one if parallelism is used.

  The logical flow is thus:

  - call restart on a program with an empty result list
  - carry out resulting actions
  - call restart on the result of the actions

  For an actual full fledged program runner, a runner using
  **manifold** as the underlying dispatching engine is provided
  in `ablauf.job.manifold`.
"
  (:require [clojure.zip     :as zip]
            [ablauf.job.ast  :as ast]
            [ablauf.job.node :as node]))

(defn ast-zip
  "Given a well-formed AST (as per `ablauf.job.ast`), yield a zipper"
  [ast]
  (zip/zipper ast/branch? :ast/nodes #(assoc %1 :ast/nodes (vec %2)) ast))

(defn augmentable?
  "Predicate to test for an augmentable node"
  [{:ast/keys [augment] :as node}]
  (and (some? node)
       (some?
        (:augment/source augment))
       (some?
        (:augment/dest augment))))

(defn errored?
  ""
  [{:exec/keys [result] :as node}]
  (and (some? node)
       (not= :result/success result)))

(defn lift-error
  ""
  [context {:exec/keys [output]}]
  (assoc context :exec/last-error output))

(defn augment
  "When an AST node contains an `augment` key, process it to
   augment the resulting context. Augments have a source:
   a function of the output, a keyword or vector, of keyword
   pointing to a path in the output. Augments also have a destination
   a key or key vector of the position in which to augment the context."
  [context {:ast/keys [augment] :exec/keys [result output]}]
  (let [{:augment/keys [source dest]} augment
        dest-vec                      (if (sequential? dest) dest [dest])]
    (cond-> context
      (= :result/success result)
      (assoc-in dest-vec
                (cond
                  (sequential? source) (get-in output source)
                  (keyword? source)    (get output source)
                  (fn? source)         (source output))))))

(defn merge-results
  "Updates a job given a list of node updates. Node updates
   either come from an action dispatch return, or from newly
   found dispatchs"
  [job context nodes]
  (if (empty? nodes)
    [job context]
    (loop [context context
           pos     job
           nodes   nodes]
      (let [node    (first nodes)
            context (cond-> context
                      (augmentable? node)
                      (augment node)
                      (errored? node)
                      (lift-error node))]
        (cond
          (nil? node)
          [(ast-zip (zip/root pos)) context]

          (zip/end? pos)
          (throw (ex-info (format "unknown job node: %s" (:ast/id node))
                          {:type :error/illegal-state
                           :pos  pos
                           :node node}))

          (= (:ast/id node) (:ast/id (zip/node pos)))
          (recur context
                 (-> pos (zip/edit merge node) (zip/next))
                 (rest nodes))

          :else
          (recur context
                 (zip/next pos)
                 nodes))))))

(defn merge-dispatchs
  [job nodes]
  (if (empty? nodes)
    job
    (loop [pos   job
           nodes nodes]
      (let [node (first nodes)]
        (cond
          (nil? node)
          (ast-zip (zip/root pos))

          (zip/end? pos)
          (throw (ex-info (format "unknown job node: %s" (:ast/id node))
                          {:type :error/illegal-state
                           :pos  pos
                           :node node}))

          (= (:ast/id node) (:ast/id (zip/node pos)))
          (recur (-> pos (zip/edit merge node) (zip/next)) (rest nodes))

          :else
          (recur (zip/next pos) nodes))))))

(defn restart
  "Given a job, and node updates for it, figure
   out the next course of action to take.

   Yields an updated job and potential side-effects.
   The result has the following structure:

       [job context dispatchs]

   This allows using the return of `restart` as an accumulator
   for `reductions` or similar functions."
  [[job context] results]
  (let [[job context] (merge-results job context results)
        dispatchs     (node/find-dispatchs (zip/node job))]
    [(merge-dispatchs job dispatchs) context dispatchs]))

(defn abort
  "Given a job, aborts the job. Yields an updated job.
  Dispatches are aborted. If there are no dispatches, then pending nodes are aborted."
  [job]
  (let [dispatchs (node/find-dispatchs (zip/node job))
        pending   (node/find-pending (zip/node job))
        to-abort  (concat pending dispatchs)
        aborted   (map node/abort to-abort)]
    (merge-dispatchs job aborted)))

(defn index-ast
  "Uniquely identifies job nodes, for later merging"
  [pos]
  (loop [i   0
         pos pos]
    (cond
      (zip/end? pos)
      [(ast-zip (zip/node pos))]

      (nil? (zip/node pos))
      (recur i (zip/next pos))

      :else
      (recur (inc i) (zip/next (zip/edit pos assoc :ast/id i))))))

(def make
  "Creates a job, suitable for `restart` from a valid AST as
   produced by functions in `ablauf.job.ast`"
  (comp index-ast ast-zip))

(defn make-with-context
  "Creates a job, attaching an initial context map, as for `make`,
   this creates a tuple suitable for `restart`"
  [ast context]
  (conj (make ast) context))

(defn done?
  "Predicate to test for completion of a (sub)job"
  [job]
  (node/done? (zip/node job)))

(defn failed?
  "Predicate to test for failure of a (sub)job"
  [job]
  (node/failed? (zip/node job)))

(defn pending?
  "Predicate to test for pending completion of a (sub)job"
  [job]
  (node/pending? (zip/node job)))

(defn eligible?
  "Predicate to test for pending completion of a (sub)job"
  [job]
  (node/eligible? (zip/node job)))

(defn aborted?
  "Predicate to test for abortion of a (sub)job"
  [job]
  (node/aborted? (zip/node job)))

(defn status
  "Get the job status from an ast"
  [ast]
  (cond
    ;; note that this relies on the order of the checks
    ;; a aborted job is also a done job
    (aborted? ast)  :job/aborted
    (pending? ast)  :job/pending
    (failed? ast)   :job/failure
    (done? ast)     :job/success
    (eligible? ast) :job/pending
    :else           (throw (ex-info "Wrong AST job state"
                                    {}))))

(defn prune
  "Remove empties :ast/nodes leaves"
  [ast]
  (loop [zipper (zip/next (ast-zip ast))]
    (if (zip/end? zipper)
      (zip/root zipper)
      (let [{:ast/keys [nodes]} (zip/node zipper)]
        (cond
          (and (some? nodes)
               (zip/path zipper)
               (empty? nodes))
          (recur (zip/remove zipper))

          :else
          (recur (zip/next zipper)))))))
