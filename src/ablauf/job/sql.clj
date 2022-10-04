(ns ablauf.job.sql
  "A minimalist job queue worker for SQL. This assumes
   multiple worker off of a main job queue. In the following
   code, each worker is assumed to be able to process both
   actions and workflow evaluation. The processing is based off
   of two tables: `task` and `workflow_run`. `workflow_run` holds
   information about a full workflow run and its current status,
   while `task` is used for both calls to `job/restart` and action
   dispatches."
  (:require [clojure.edn :as edn]
            [next.jdbc :as jdbc]
            [ablauf.job :as job]
            [ablauf.job.ast :as ast]
            [ablauf.job.store :as store]
            [clojure.tools.logging :as log]))

;; Safe version of functions performing side effects to avoid breaking
;; the processing loop.
;;
;; We apply this logic wherever control is delegated to a third-party
;; implementation of an action (here `action-fn` for `safe-run` and
;; `Jobstore#persist` for `safe-persist`
(defn- safe-run
  "Reliably produce a *success* or *failure* output from a call to `action-fn`.

   XXX: too primitive, should allow for restarts."
  [action-fn task]
  (try
    (let [res       (action-fn task)
          deferred? (instance? clojure.lang.IDeref res)]
      (assoc task
             :exec/result :result/success
             :exec/output (cond-> res deferred? deref)))
    (catch Exception e
      (let [data (ex-data e)]
        (assoc task
               :exec/result :result/failure
               :exec/output (if-let [output (:error data)]
                              output
                              (ex-message e)))))))

(defn- clock
  "Retrieve the current millisecond epoch"
  []
  (System/currentTimeMillis))

;; This could be parametered to be protobuf + compression or any other
;; fast and efficient method
(def serialize pr-str)
(def deserialize edn/read-string)

;; A few functions to get things in and out of the database, with proper
;; serialization

(defn- next-task!
  "A function to fetch the next actionable item out of the database within a
   transaction, skipping locked items."
  [tx]
  (let [task (jdbc/execute-one!
              tx
              ["select * from task where status = 'new' limit 1 for update skip locked"])]
    (some-> task
            (update :task/payload deserialize)
            (assoc :task/status "pending"))))

(defn- workflow-by-id
  "Fetch workflow from the database by id and restore stored job data to
   a usable form."
  [tx id]
  (let [{:workflow_run/keys [reason ast context] :as wrun}
        (jdbc/execute-one! tx [(str "select id,uuid,status,reason,ast,context "
                                    "from workflow_run where id=?")
                               id])]
    (-> wrun
        (dissoc :workflow_run/ast :workflow_run/context)
        (cond-> (some? reason) (assoc :workflow_run/reason reason))
        (assoc :workflow_run/job (job/reload (deserialize ast)
                                             (deserialize context))))))

(defn workflow-by-uuid
  "Fetch workflow from the database by UUID and restore stored job data to
   a usable form."
  [tx uuid]
  (let [{:workflow_run/keys [reason ast context] :as wrun}
        (jdbc/execute-one! tx [(str "select id,uuid,status,reason,ast,context "
                                    "from workflow_run where uuid=?")
                               (str uuid)])]
    (-> wrun
        (dissoc :workflow_run/ast :workflow_run/context)
        (cond-> (some? reason) (assoc :workflow_run/reason reason))
        (assoc :workflow_run/job (job/reload (deserialize ast)
                                             (deserialize context))))))

(defn- workflow-context-by-id
  [tx id]
  (-> (jdbc/execute-one! tx ["select context from workflow_run where id=?" id])
      :workflow_run/context
      deserialize))

(defn- insert-action
  "Persist a new action to be handled immediately"
  [tx workflow-id workflow-uuid action]
  ;; default status is "new"
  (jdbc/execute!
   tx
   ["insert into task(type,wid,wuuid,payload) values(?,?,?,?)"
    "action" workflow-id workflow-uuid (serialize action)]))

(defn- update-workflow
  "After a job restart, store the new state of a workflow"
  [tx id ast context]
  (jdbc/execute! tx ["update workflow_run set status=?, ast=?, context=? where id=?"
                     (-> ast job/status name)
                     (-> ast job/unzip serialize)
                     (serialize context)
                     id]))

(defn- insert-workflow
  "Register new workfow as ready to be processed"
  [tx jobstore [ast context] uuid owner system type metadata]
  (store/sync-persist jobstore uuid context ast)
  (-> (jdbc/execute!
       tx [(str "INSERT INTO "
                "workflow_run(uuid,ast,context,job_owner,job_system,job_type,metadata) "
                "VALUES(?,?,?,?,?,?,?)")
           (str uuid)
           (-> ast job/unzip serialize)
           (serialize context)
           owner
           system
           type
           (serialize metadata)]
       {:return-keys true})
      (first)
      :GENERATED_KEY))

(defn- insert-workflow-restart
  "Schedule a job/restart call"
  [tx id uuid payload]
  (jdbc/execute! tx ["INSERT INTO task(type,wid,wuuid,payload) VALUES(?,?,?,?)"
                     "workflow" id (str uuid) (serialize payload)]))

(defn- clean-task
  "Updates the task status"
  [tx id]
  (jdbc/execute! tx ["delete from task where id=?" id]))

(defn- restart-workflow
  "Handler for tasks of type `workflow`. Retrieves current version of the
   AST, calls `job/restart` on it and then schedules new `action` tasks
   if applicable. This can be thought of as the function to advance the
   instruction pointer within the program, results in a new program and
   updated context."
  [tx jobstore {:task/keys [wid wuuid payload] :or {payload []}}]
  (let [workflow (workflow-by-id tx wid)
        [ast context actions]
        (job/restart (:workflow_run/job workflow) payload)]
    (try
      (update-workflow tx wid ast context)
      (doseq [action actions]
        (insert-action tx wid wuuid action))
      (store/sync-persist jobstore wuuid context ast)
      (catch Exception e
        (log/error e "cannot restart worklow" wuuid)))))

(defn- process-task
  "Handler for tasks of type `action`:

   Produces a *success* or *failure* output from a payload,
   then creates a workflow restart task to process the result.

   Runs the payload through `action-fn`, handling exceptions through
   `safe-run`."
  [tx action-fn {:task/keys [wid wuuid payload]}]
  ;; TODO will we need the context?
  (let [start       (clock)
        context     (workflow-context-by-id tx wid)
        ctx+payload (assoc payload :exec/context context)
        result      (safe-run action-fn ctx+payload)
        result+ts   (-> result
                        (assoc :exec/timestamp start
                               :exec/duration  (- (clock) start))
                        (dissoc :exec/context))]
    (try
      (insert-workflow-restart tx wid wuuid [result+ts])
      (catch Exception e
        (log/error e "cannot process task" wuuid)))))

(defn- process-one
  "Process a new task within a transaction, this function ends up either
   processing a result (when encountering a task of type `workflow`), or
   performing a side-effect (for `action` type tasks).

   See `restart-workflow` and `process-task` for details on each individual
   action."
  [db jobstore action-fn]
  (jdbc/with-transaction [tx db]
    (when-let [task (next-task! tx)]
      (if (= "workflow" (:task/type task))
        (restart-workflow tx jobstore task)
        (process-task tx action-fn task))
      (clean-task tx (:task/id task))
      true)))

(defn- process-available
  "Loop over all immediately available items and process them in
  separate transactions"
  [db jobstore action-fn stop-fn]
  (loop []
    (when (and (process-one db jobstore action-fn)
               (not (true? (stop-fn))))
      (recur))))

(defn worker
  "Until `stop-fn` returns true, process items until the queue is
  exhausted, then wait for a random amount of time (between `wait-ms`
  and `wait-ms` + `wait-allowance`).

   As many worker threads as necessary can be started on a single
  host."
  [db jobstore {:keys [action-fn]} wait-ms wait-allowance stop-fn]
  (loop []
    (try
    ;; TODO: try/catch, mark job as failed
      (process-available db jobstore action-fn stop-fn)
      (catch Exception e
        (log/error e "cannot process")))
    (when-not (true? (stop-fn))
      (Thread/sleep (+ wait-ms (rand-int wait-allowance)))
      (recur))))

(defn submit
  "Create a new job to be processed. Only registers the job entry, no
  processing will be performed unless at least one separate thread
  concurrently runs `worker`."
  [db jobstore job {:keys [context type system uuid owner metadata]
                    :or   {context  {}
                           metadata {}
                           owner    "unknown"
                           type     "unknown"
                           system   "unknown"}}]
  (jdbc/with-transaction [tx db]
    (let [ast (job/make-with-context job (dissoc context :exec/runtime))
          wid (insert-workflow tx jobstore ast uuid owner system type metadata)]
      (insert-workflow-restart tx wid uuid []))))

(comment
  (def db (atom {}))

  (def jobstore
    (reify store/JobStore
      (persist [_ uuid context state]
        (swap! db assoc uuid {:state state :context context}))))

  (def dbenv
    {:connection-uri (or (System/getenv "MARIA_JDBC_URI")
                         "jdbc:mysql://root:root@127.0.0.1:3306/ablauf")})

  (defn dumb-fn [task]
    (:ast/payload task))

  (reset! db {})
  (pr @db)

  (def ast (ast/do!!
            (ast/dopar!!
             (ast/log!! "hello1")
             (ast/log!! "hello2"))
            (ast/log!! "final")))

  (submit dbenv jobstore (ast/log!! "hello") {:context {:a :b}})

  (submit dbenv jobstore (ast/do!! (ast/log!! "hello1") (ast/log!! "hello2")) {:context {:a :b}})

  (submit dbenv jobstore ast {:context {:a :b}})

  (def f (future (worker dbenv jobstore {:action-fn #'dumb-fn} 100 900 (constantly false))))

  "")
