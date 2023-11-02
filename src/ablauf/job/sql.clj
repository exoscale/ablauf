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
            [clojure.string :as str]
            [manifold.deferred :as d]
            [next.jdbc :as jdbc]
            [ablauf.job :as job]
            [ablauf.job.store :as store]
            [clojure.tools.logging :as log])
  (:import [clojure.lang ExceptionInfo]))

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
            (update :task/wuuid parse-uuid)
            (assoc :task/status "pending"))))

(defn- workflow-by-id-for-update
  "Fetch workflow from the database by id and restore stored job data to
   a usable form."
  [tx id]
  (let [{:workflow_run/keys [reason ast context] :as wrun}
        (jdbc/execute-one! tx [(str "select id,uuid,status,reason,ast,context "
                                    "from workflow_run where id=? for update skip locked")
                               id])]
    (if (nil? wrun)
      ;; some other worker was updating the workflow (eg: dopar!! leafs finished at same time)
      ;; let's throw and retry up the stack
      (throw (ex-info (format "Could not acquire lock on workflow id %s" id) {:workflow_run/id id
                                                                              :cause           :workflow/locked}))
      (some-> wrun
              (dissoc :workflow_run/ast :workflow_run/context)
              (update :workflow_run/uuid parse-uuid)
              (cond-> (some? reason) (assoc :workflow_run/reason reason))
              (assoc :workflow_run/job (job/reload (deserialize ast)
                                                   (deserialize context)))))))

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
        (update :workflow_run/uuid parse-uuid)
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
    "action" workflow-id (str workflow-uuid) (serialize action)]))

(defn- update-workflow
  "After a job restart, store the new state of a workflow"
  [tx id ast context]
  (jdbc/execute! tx ["update workflow_run set status=?, ast=?, context=? where id=?"
                     (-> ast job/status name)
                     (-> ast job/unzip serialize)
                     (serialize context)
                     id]))

(defn- update-task-status
  "Updates the task. Mostly for updating state to 'pending'"
  [tx {:task/keys [status id]}]
  (jdbc/execute! tx ["update task set status=? where id=?"
                     status
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
                     "workflow" id (str uuid) (serialize payload)] {:return-keys true}))

(defn- clean-task
  "Updates the task status"
  [tx id]
  (log/debugf "Deleting task with id: %s" id)
  (jdbc/execute! tx ["delete from task where id=?" id]))

(defn- restart-workflow
  "Handler for tasks of type `workflow`. Retrieves current version of the
   AST, calls `job/restart` on it and then schedules new `action` tasks
   if applicable. This can be thought of as the function to advance the
   instruction pointer within the program, results in a new program and
   updated context."
  [tx jobstore {:task/keys [wid wuuid payload] :or {payload []}}]
  ;; NOTE: a dopar!! ast can be picked up by two different threads
  ;; when they both want to update the same workflow, only one of them may win
  ;; either we
  ;; - run with serializable TX AND retry AND account for idempotency on retry
  ;; - OR just ensure a single worker can restart the workflow AT A TIME
  ;;   so, workflow-by-id-for-update will try and acquire a lock in order to update the job

  (let [workflow (workflow-by-id-for-update tx wid)
        [ast context actions] (job/restart (:workflow_run/job workflow) payload)]
    (try
      (update-workflow tx wid ast context)
      (doseq [action actions]
        (insert-action tx wid wuuid action))
      (store/sync-persist jobstore wuuid context ast)
      (catch Exception e
        (log/error e "cannot restart worklow" wuuid)
        (throw e)))))

(defn- process-task
  "Handler for tasks of type `action`:
   Produces a *success* or *failure* output from a payload.
   Runs the payload through `action-fn`, handling exceptions through
   `safe-run`."
  [context action-fn {:task/keys [payload]}]
  (let [start       (clock)
        ctx+payload (assoc payload :exec/context context)
        result      (safe-run action-fn ctx+payload)
        result+ts   (-> result
                        (assoc :exec/timestamp start
                               :exec/duration (- (clock) start))
                        (dissoc :exec/context))]
    result+ts))

(defn- process-one
  "Process a new task. This function ends up either
   processing a result (when encountering a task of type `workflow`), or
   performing a side-effect (for `action` type tasks).

   Side-effects are performed outside of transactions.
   See `restart-workflow` and `process-task` for details on each individual action."
  [db jobstore action-fn]
  (let [[nxt context task] (jdbc/with-transaction [tx db]
                             ;; acquire lock, get next task, release lock
                             (when-let [task (next-task! tx)]
                               (if (= "workflow" (:task/type task))
                                 ;; insert a restart, delete the current workflow task
                                 (do
                                   (log/debugf "Restarting workflow: %s" task)
                                   (restart-workflow tx jobstore task)
                                   (clean-task tx (:task/id task))
                                   [:workflow nil nil])
                                 ;; mark task as pending, exit tx so we release the conn
                                 (do
                                   (log/debugf "Updating task status: %s" task)
                                   (update-task-status tx task)
                                   [:task (workflow-context-by-id tx (:task/wid task)) task]))))]
    ;; we return true to tell if caller if should check for more tasks
    (condp = nxt
      :workflow true
      :task (let [{:task/keys [wid wuuid id]} task
                  ;; process task never throws
                  _         (log/debugf "Starting task processing: %s" task)
                  result+ts (process-task context action-fn task)]
              (jdbc/with-transaction [tx db]
                (log/debugf "Task processing done, proceeding: %s" result+ts)
                (insert-workflow-restart tx wid wuuid [result+ts])
                (clean-task tx id))
              true)
      nil)))

(defn- process-available
  "Loop over all immediately available items and process them in
  separate transactions"
  [db jobstore action-fn stop-fn]
  (loop []
    (when (and (try
                 (process-one db jobstore action-fn)
                 (catch ExceptionInfo e
                   ;; if we aborted due to workflow locked, just let it recur
                   ;; otherwise preserve previous behaviour
                   ;; dont throw up so we don't exhaust runner threads
                   (if (= :workflow/locked (-> e ex-data :cause))
                     (log/tracef "Workflow run with id %s locked, retrying" (-> e ex-data :workflow_run/id))
                     (log/error e "Cannot process task"))
                   true)
                 ;; any other exceptions we proceed
                 (catch Exception e
                   (log/error e "Cannot process task")))
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
        (log/error e "Cannot process")))
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

(defn submit-retry
  "Retries a given job by its id. If the job is in a replayable state, it will register a job entry for restarting the job.
  Returns `true` if the job is retryable, `false` otherwise.
  Will throw an exception if the job has not terminated.
  The processing should be done by a `worker` thread."
  [db jobstore wid]
  ;; very similar to restart-workflow
  (jdbc/with-transaction [tx db]
    (let [{:workflow_run/keys [job uuid status]} (workflow-by-id-for-update tx wid)
          [ast context _] (job/restart job [])
          updated-context (assoc context :exec/retries (inc (get context :exec/retries 0)))
          replayable-ast  (job/prepare-replay ast)]

      ;; preflight check: don't try to retry ongoing job
      (when-not (job/done? ast)
        (throw (ex-info "Workflow run cannot be retried, has not terminated" {:workflow_run/uuid uuid :workflow_run/status status})))

      ;; can't replay, bail
      (if (job/done? replayable-ast)
        false
        ;; else
        (try
          (update-workflow tx wid replayable-ast updated-context)
          (let [job (job/make-with-context (job/unzip replayable-ast) updated-context)
                [ast context actions] (job/restart job [])]
            (doseq [action actions]
              (insert-action tx wid uuid action))
            (store/sync-persist jobstore uuid context ast))
          true
          (catch Exception e
            (log/error e "Cannot restart workflow" uuid)
            (throw e)))))))

(defn fail-pending-tasks!
  "Sequentially fails all tasks marked as pending.
  Returns a set with workflow ids.
  Workflows can then be retried via `(retry tx workflow-id jobstore)`
  "
  [db jobstore]
  (jdbc/with-transaction [tx db]
    (let [pending-tasks  (jdbc/execute! tx ["select * from task where status='pending' for update skip locked"])
          workflow-uuids (set (mapv :task/wid pending-tasks))
          ;; these tasks were marked as pending **on the AST** but never got to
          ;; actually have a chance to run (eg: due to few workers, etc) - the AST says "pending" but DB says "new"
          ;; why? ablauf will always insert a "pending" ast node on workflow restart
          ;; => we need to remove them from the workflow ASTs to ensure the workflow will be in a terminal state
          ;; only terminal workflows can be retried
          orphaned-tasks (jdbc/execute! tx ["select * from task where status = 'new' and wid in (?) for update skip locked"
                                            (str/join "," workflow-uuids)])]

      (log/infof "Pruning %s tasks with status='new'" (count orphaned-tasks))
      (doseq [[wid tasks] (->> orphaned-tasks (group-by :task/wid))]
        ;; for each workflow, compile a list of AST ids
        ;; remove them fromt the workflow's AST
        ;; even if eg: a (dopar ...) is run one by one, the AST nodes will already be marked as pending
        ;; this will be a problem for restart, as restart only works for terminal jobs
        (let [matching-id? (set (mapv (comp :ast/id deserialize :task/payload) tasks))
              {:workflow_run/keys [job uuid]} (workflow-by-id-for-update tx wid)
              [ast context _] (job/restart job [])
              to-remove    (fn [{:ast/keys [id]}] (matching-id? id))
              pruned-ast   (job/remove-nodes-by ast to-remove)]

          (update-workflow tx wid pruned-ast context)
          (store/sync-persist jobstore uuid context ast)
          ;; remove all tasks from the DB too
          ;; after this, the workflow AST should be replayable
          (run! #(clean-task tx (:task/id %)) tasks)))

      ;; go to each task, mark it as failure
      ;; restart the workflow to keep AST consistent
      (log/infof "Preparing to fail %s pending tasks" (count pending-tasks))
      (reduce (fn [workflow-ids db-task]
                (let [task            (-> db-task
                                          (update :task/payload deserialize)
                                          (update :task/wuuid parse-uuid))
                      {:task/keys [id wid]} task
                      context         (workflow-context-by-id tx wid)
                      updated-context (assoc context :exec/retries (inc (get context :exec/retries 0)))

                      action-fn       (fn [_] (d/error-deferred ""))
                      result+ts       (process-task updated-context action-fn task)]

                  (log/debugf "Marking task with id %s as failed" id)
                  (restart-workflow tx jobstore (assoc task :task/payload [result+ts]))
                  (clean-task tx id)
                  ;; append
                  (conj workflow-ids wid)))
              #{}
              pending-tasks))))
