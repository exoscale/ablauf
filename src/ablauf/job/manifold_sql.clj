(ns ablauf.job.manifold-sql
  (:require [clojure.tools.logging :as log]
            [manifold.deferred :as d]
            [ablauf.job.sql :as sql]))

(defn make-sql-runner
  "SQL runner compatible with the manifold one, useful to test against both."
  [db-spec]
  (fn sql-runner [store ast {:keys [action-fn id context]}]
    (let [the-id    (or id (random-uuid))
          get-job   (fn [] (sql/workflow-by-uuid db-spec the-id))
          job-done? (fn [] (some? (#{"success" "failure"}
                                   (:workflow_run/status (get-job)))))]
      ;; submit the job
      (d/future
        (try
          (sql/submit db-spec store ast {:context context :uuid the-id})
          (sql/worker db-spec store {:action-fn action-fn} 100 900 job-done?)
          (log/info "done processing job")
          (let [r (get-job)]
            (cond
              (nil? r)
              (throw (ex-info "workflow run is gone!" {}))

              (= "success" (:workflow_run/status r))
              (:workflow_run/job r)

              (= "failure" (:workflow_run/status r))
              (throw (ex-info "action error" {:data (:workflow_run/job r)}))

              :else
              (throw (ex-info "unknown workflow run state!" {}))))
          (catch Throwable e
            (log/error e "worker error")
            (throw e)))))))
