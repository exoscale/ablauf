(ns ablauf.job.sql.retry-idempotent-2-test
  (:require [ablauf.job.ast :as ast]
            [ablauf.job.node :as node]
            [manifold.deferred :as d]
            [ablauf.job.sql :as sql]
            [ablauf.job.sql.utils :as sqlu :refer [mock-store make-datasource query-task query-workflow sql-worker-fn]]
            [clojure.test :refer :all]
            [hikari-cp.core :as hikari]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(deftest task-status-pending
  (let [ast (ast/dopar!!
             (ast/action!! :fail {} {:idempotent? true})
             (ast/action!! :fail {} {:idempotent? true})
             (ast/action!! :ok {}))]

    (testing "Simulate job submission + crash"
      (let [uuid      (random-uuid)
            counter   (atom 0)

            get-job   (fn [ds] (sql/workflow-by-uuid ds uuid))
            job-done? (fn [ds] (some? (#{"success" "failure"}
                                       (:workflow_run/status (get-job ds)))))
            ds        (make-datasource 1)
            store     (mock-store counter)

            done?     (atom 2)
            done-fn?  (fn [] (= 0 @done?))
            action-fn (fn [{:ast/keys [action]}]
                        ;; simulate connection error/process crash
                        (when (= :fail action)
                          (Thread/sleep 1000) ;; allow the other thread to catch up
                          (hikari/close-datasource ds)
                          (swap! done? dec)))
            worker-fn (sql-worker-fn ds store action-fn done-fn?)]

        (sql/submit sqlu/test-spec store ast {:uuid uuid})
        ;; start two workers
        (future (worker-fn))
        (future (worker-fn))

        (testing "Task is marked as pending"
          ;; just wait for the task to have run
          (while (not (done-fn?))
            (Thread/sleep 100))
          (let [{:task/keys [status]} (query-task uuid)]
            (is (= "pending" status))))

        (testing "Ablauf mark as failed facilties"
          (let [ds    (make-datasource 1)
                tasks (sql/fail-pending-tasks! ds store)
                wflow (query-workflow uuid)
                ast   (sql/deserialize (get wflow :workflow_run/ast))
                nodes (:ast/nodes ast)
                [a1 a2] nodes]

            (testing "Only 1 task on the database remains"
              (is (= 1 (count tasks))))

            (testing "AST is in sync with the task table"
              (is (= 2 (count nodes))))

            (testing "Node statuses as expected"
              (is (node/failed? a1))
              (is (node/failed? a2)))

            (testing "Retrying the failed job runs to completion because the failed action is idempotent thus retriable"
              (let [done?     (atom false)
                    done-fn?  (fn [] @done?)
                    worker-fn (sql-worker-fn ds store (constantly :ok) done-fn?)]
                (is (sql/submit-retry ds store (:workflow_run/id wflow)) "Retry should be possible")

                (testing "Job was done successfully"
                  (future (worker-fn))

                  ;; we should put a timeout here...
                  (while (not (job-done? ds))
                    (Thread/sleep 100))
                  (reset! done? true)
                  (is (= "success" (:workflow_run/status (get-job ds)))))))))))))

