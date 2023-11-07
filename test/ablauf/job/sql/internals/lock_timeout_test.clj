(ns ablauf.job.sql.internals.lock-timeout-test
  "NS for testing private functions.
  Knows a bit too much about the internals."
  (:require [manifold.deferred :as d]
            [ablauf.job.sql :as sql]
            [ablauf.job.sql.utils :as sqlu :refer [make-datasource mock-store sql-worker-fn query-workflow query-task]]
            [clojure.test :refer :all])
  (:import (com.mysql.cj.jdbc.exceptions MySQLTransactionRollbackException)
           (java.util.concurrent Executors)))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(defn- action-fn [{:ast/keys [action payload]}]
  (case action
    ::inc (d/success-deferred (inc payload))))

(deftest task-status-pending
  (testing "ablauf internals: emulate lock timeout"
    (let [insert-workflow-restart @#'ablauf.job.sql/insert-workflow-restart
          restart-counter         (atom 0)]
      (with-redefs [ablauf.job.sql/insert-workflow-restart (fn [& args]
                                                             ;; throw exception only once
                                                             ;; we want to emulate a lock timeout
                                                             ;; when trying to insert the AST leaf update
                                                             (swap! restart-counter inc)
                                                             (when (= 2 @restart-counter)
                                                               (throw (MySQLTransactionRollbackException.)))
                                                             (apply insert-workflow-restart args))]
        (let [ast          #:ast{:type :ast/par, :nodes [#:ast{:type :ast/leaf, :action ::inc, :payload 1}
                                                         #:ast{:type :ast/leaf, :action ::inc, :payload 2}
                                                         #:ast{:type :ast/leaf, :action ::inc, :payload 3}]}
              tpool        (Executors/newFixedThreadPool 3)

              uuid         (random-uuid)
              store        (mock-store)
              job-success? (fn [] (= "success" (:workflow_run/status (query-workflow uuid))))
              job-failed?  (fn [] (= "failure" (:workflow_run/status (query-workflow uuid))))
              job-done?    (fn [] (or (job-success?)
                                      (job-failed?)))
              datasource   sqlu/test-spec
              worker-fn    (sql-worker-fn datasource store action-fn job-done?)]

          (dotimes [_ 4]
            (.submit tpool ^Callable worker-fn))

          ;;wait
          (sql/submit datasource store ast {:uuid uuid})

          ;; wait for the job, we also don't leave the
          ;; with-redef block this way, so we ensure the redef still
          ;; applies to the worker fns
          (while (not (job-done?))
            (Thread/sleep 1000))

          (is (job-success?))
          (.shutdownNow tpool)

          (let [{status :workflow_run/status} (query-workflow uuid)]
            (is (= "success" status))))))))
