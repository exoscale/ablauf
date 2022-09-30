(ns ablauf.job.sql
  "A minimalist job queue worker for SQL. This assumes
   multiple worker off of a main job queue. In the following
   code, each worker is assumed to be able to process both
   actions and workflow evaluation. The processing is based off
   of two tables: `task` and `workflow_run`. `workflow_run` holds
   information about a full workflow run and its current status,
   while `task` is used for both calls to `job/restart` and action
   dispatches."
  (:require [ablauf.job  :as job]
            [clojure.edn :as edn]
            [next.jdbc   :as jdbc]
            [ablauf.job.ast :as ast]))

;; This could be parametered to be protobuf + compression or any other
;; fast and efficient method
(def ^:private serialize   pr-str)
(def ^:private deserialize edn/read-string)

;; A few functions to get things in and out of the database, with proper
;; serialization

(defn- next-task!
  "A function to fetch the next actionable item out of the database within a
   transaction, skipping locked items."
  [tx]
  (some-> (jdbc/execute!
           tx
           [(str "select * from task"
                 " where process_at <= now()"
                 " order by process_at asc"
                 " limit 1"
                 " for update skip locked")])
          (first)
          (update :task/payload deserialize)))

(defn- workflow-by-id
  "Fetch workflow from the database and restore stored job data to
   a usable form."
  [tx id]
  (let [[{:workflow_run/keys [reason ast context] :as wrun}]
        (jdbc/execute! tx ["select * from workflow_run where id=?" id])]
    (-> wrun
        (dissoc :workflow_run/ast :workflow_run/context)
        (cond-> (some? reason) (assoc :workflow_run/reason reason))
        (assoc :workflow_run/job (job/reload (deserialize ast)
                                             (deserialize context))))))

(defn- insert-action
  "Persist a new action to be handled immediately"
  [tx workflow-id action]
  (jdbc/execute! tx ["insert into task(type,wid,payload) values(?, ?, ?)"
                     "action" workflow-id (serialize action)]))

(defn- clean-task
  "Delete a task from the database once it has been processed"
  [tx id]
  (jdbc/execute! tx ["delete from task where id=?" id]))

(defn- update-workflow
  "After a job restart, store the new state of a workflow"
  [tx id ast context]
  (jdbc/execute! tx ["update workflow_run set status=?, ast=?, context=? where id=?"
                     (-> ast job/status name)
                     (-> ast job/unzip serialize)
                     (serialize context)
                     id]))

(defn- insert-workflow
  "Add a new workfow"
  [tx [ast context]]
  (-> (jdbc/execute! tx ["insert into workflow_run(ast,context) values(?,?)"
                         (-> ast job/unzip serialize)
                         (serialize context)]
                     {:return-keys true})
      (first)
      :GENERATED_KEY))

(defn- insert-workflow-restart
  "Schedule a job/restart call"
  [tx id payload]
  (jdbc/execute! tx ["insert into task(type,wid,payload) values(?,?,?)"
                     "workflow" id (serialize payload)]))

(defn- safe-run
  "Reliably produce a *success* or *failure* output from a call to `action-fn`.
  
   XXX: too primitive, should allow for restarts and capturing more data out
   of exceptions."
  [action-fn task]
  (try
    (assoc task :exec/result :result/success :exec/output (action-fn task))
    (catch Exception e
      (assoc task :exec/result
             :result/failure
             :exec/reason (ex-message e)))))

(defn- restart-workflow
  "Handler for tasks of type `workflow`. Retrieves current version of the
   AST, calls `job/restart` on it and then schedules new `action` tasks
   if applicable. This can be thought of as the function to advance the
   instruction pointer within the program, results in a new program and
   updated context."
  [tx {:task/keys [id wid payload] :or {payload []}}]
  (let [[ast context actions]
        (job/restart (:workflow_run/job (workflow-by-id tx wid)) payload)]
    (update-workflow tx wid ast context)
    (doseq [action actions]
      (insert-action tx wid action))))

(defn- process-task
  "Handler for tasks of type `action`:

   Produces a *success* or *failure* output from a payload,
   then creates a workflow restart task to process the result.

   Runs the payload through `action-fn`, handling exceptions through
   `safe-run`."
  [tx action-fn {:task/keys [id wid payload]}]
  (let [result (safe-run action-fn payload)]
    (insert-workflow-restart tx wid [result])))

(defn- process-one
  "Process a new task within a transaction, this function ends up either
   processing a result (when encountering a task of type `workflow`), or
   performing a side-effect (for `action` type tasks).

   See `restart-workflow` and `process-task` for details on each individual
   action."
  [db action-fn]
  (jdbc/with-transaction [tx db]
    (when-let [task (next-task! tx)]
      (if (= "workflow" (:task/type task))
        (restart-workflow tx task)
        (process-task tx action-fn task))
      ;; Delete task so it does not get picked up
      ;; by a new worker
      (clean-task tx (:task/id task))
      true)))

(defn- process-available
  "Loop over all immediately available items and process them in separate transactions"
  [db action-fn stop-fn]
  (loop []
    (when (and (process-one db action-fn)
               (not (true? (stop-fn))))
      (recur))))

(defn worker
  "Until `stop-fn` returns true, process items until the queue is exhausted, then
   wait for a random amount of time (between `wait-ms` and `wait-ms` + `wait-allowance`).

   As many worker threads as necessary can be started on a single host."
  [db action-fn wait-ms wait-allowance stop-fn]
  (loop []
    (process-available db action-fn stop-fn)
    (when-not (true? (stop-fn))
      (Thread/sleep (+ wait-ms (rand-int wait-allowance)))
      (recur))))

(defn submit
  "Create a new job to be processed. Only registers the job entry, no processing
   will be performed unless at least one separate thread concurrently runs `worker`."
  [db job context]
  (jdbc/with-transaction [tx db]
    (let [wid (insert-workflow tx (job/make-with-context job context))]
      (insert-workflow-restart tx wid []))))

(comment
  (def env {:connection-uri (System/getenv "MARIA_JDBC_URI")})

  (require '[ablauf.job.ast :as ast])

  (defn dumb-fn [task] (:ast/payload task))

  (submit env (ast/log!! "hello") {:a :b})

  (def f (future (worker env dumb-fn 100 900 (constantly false)))))
