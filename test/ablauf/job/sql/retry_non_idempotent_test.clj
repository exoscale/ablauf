(ns ablauf.job.sql.retry-non-idempotent-test
  (:require [ablauf.job.ast :as ast]
            [ablauf.job.node :as node]
            [manifold.deferred :as d]
            [ablauf.job.sql :as sql]
            [ablauf.job.sql.utils :as sqlu :refer [mock-store make-datasource query-task query-workflow sql-worker-fn]]
            [clojure.test :refer :all]
            [hikari-cp.core :as hikari]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(defn- action-fn [{:ast/keys [action payload]}]
  (case action
    ::close-db (d/error-deferred :error/error)
    ::sleep (d/chain nil (fn [_] (Thread/sleep 1000) (.getName (Thread/currentThread))))
    ::inc (d/success-deferred (inc payload))))

(deftest task-status-pending
  (let [ast (ast/do!!
             (ast/action!! :ok {})
             (ast/action!! :fail {}) ;; equivalent to  {:idempotent? false}
             (ast/action!! :ok {}))]

    (testing "Simulate job submission + crash"
      (let [uuid      (random-uuid)
            counter   (atom 0)

            get-job   (fn [ds] (sql/workflow-by-uuid ds uuid))
            job-done? (fn [ds] (some? (#{"success" "failure"}
                                       (:workflow_run/status (get-job ds)))))
            ds        (make-datasource 1)
            store     (mock-store counter)

            done?     (atom false)
            done-fn?  (fn [] @done?)
            action-fn (fn [{:ast/keys [action]}]
                        ;; simulate connection error/process crash
                        (when (= :fail action)
                          (hikari/close-datasource ds)
                          (reset! done? true)))
            worker-fn (sql-worker-fn ds store action-fn done-fn?)]

        (sql/submit sqlu/test-spec store ast {:uuid uuid})
        (future (worker-fn))

        (testing "Task is marked as pending"
          ;; just wait for the task to have run
          (while (not @done?)
            (Thread/sleep 100))
          (let [{:task/keys [status]} (query-task uuid)]
            (is (= "pending" status))))

        (testing "Ablauf mark as failed facilties"
          (let [ds    (make-datasource 1)
                tasks (sql/fail-pending-tasks! ds store)
                wflow (query-workflow uuid)
                ast   (sql/deserialize (get wflow :workflow_run/ast))
                nodes (:ast/nodes ast)
                [a1 a2 a3] nodes]

            (testing "Only 1 task on the database remains"
              (is (= 1 (count tasks))))

            (testing "Child node statuses are as expected"
              (is (node/done? a1))
              (is (node/failed? a2))
              (is (node/eligible? a3)))

            (testing "Retrying the failed job fails because the leaf is not idempotent"
              (let [done?     (atom false)
                    done-fn?  (fn [] @done?)
                    worker-fn (sql-worker-fn ds store (constantly :ok) done-fn?)]
                (is (not (sql/submit-retry ds store (:workflow_run/id wflow))) "Retry should NOT be possible")

                (testing "Job reached terminal state"
                  (future (worker-fn))

                  ;; we should put a timeout here...
                  (while (not (job-done? ds))
                    (Thread/sleep 100))
                  (reset! done? true)
                  (is (= "failure" (:workflow_run/status (get-job ds)))))))))))))

