(ns ablauf.job.sql.internals.retry-lock-test
  "NS for testing private functions.
  Knows a bit too much about the internals."
  (:require [manifold.deferred :as d]
            [ablauf.job.sql :as sql]
            [ablauf.job.sql.utils :as sqlu :refer [mock-store sql-worker-fn query-workflow query-task]]
            [clojure.test :refer :all])
  (:import (java.util.concurrent Executors)))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(defn- action-fn [{:ast/keys [action payload]}]
  (case action
    ::inc (d/success-deferred (inc payload))))

(deftest task-status-pending
  (testing "ablauf internals: restart-workflow"

    ;; we want to specifically test that the next-task! query
    ;; requires the "... join workflow_run on workflow_run.id=task.wid " to ensure we lock both
    ;; the task and workflow
    ;; without that bit, we will end up restarting the same task id multiple times, which is NOT
    ;; what ablauf relies on when using the ".. for update skip locked"

    (let [restart-workflow @#'ablauf.job.sql/restart-workflow
          restart-tracker (atom {})]
      (with-redefs [ablauf.job.sql/restart-workflow (fn [tx jobstore {:task/keys [id] :as task}]
                                                      (swap! restart-tracker (fn [m]
                                                                               (if (contains? m id)
                                                                                 (update m id inc)
                                                                                 (assoc m id 1))))
                                                      (restart-workflow tx jobstore task))]
        (let [parnodes     (repeat 100 #:ast{:type :ast/leaf, :action ::inc, :payload 1})
              ast          #:ast{:type :ast/par, :nodes parnodes}
              tpool        (Executors/newFixedThreadPool 100)

              uuid         (random-uuid)
              store        (mock-store)
              job-success? (fn [] (= "success" (:workflow_run/status (query-workflow uuid))))
              job-failed?  (fn [] (= "failure" (:workflow_run/status (query-workflow uuid))))
              job-done?    (fn [] (or (job-success?)
                                      (job-failed?)))
              worker-fn    (sql-worker-fn sqlu/test-spec store action-fn job-done?)]

          (dotimes [_ 100]
            (.submit tpool ^Callable worker-fn))

          ;;wait
          (sql/submit sqlu/test-spec store ast {:uuid uuid})

          ;; wait for the job, we also don't leave the
          ;; with-redef block this way, so we ensure the redef still
          ;; applies to the worker fns
          (while (not (job-done?))
            (Thread/sleep 1000))

          (is (job-success?))
          (.shutdownNow tpool)

          ;; this is the bit we track down how many restarts we did per task
          (is (every? #(= 1 %) (vals @restart-tracker)) "Should only restart once per task-id!"))))))

