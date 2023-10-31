(ns ablauf.job.sql.worker-fail-test
  (:require [manifold.deferred :as d]
            [ablauf.job.ast :as ast]
            [ablauf.job.sql.utils :as sqlu :refer [mock-store query-workflow]]
            [ablauf.job.manifold-sql :as msql]
            [clojure.test :refer :all]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(def sql-runner (msql/make-sql-runner sqlu/test-spec))

(defn- action-fn [{:ast/keys [action payload]}]
  (case action
    :action/fail (d/error-deferred :error/error)
    :action/error (throw (Error. "This is an error that is not caught"))
    ::inc (d/success-deferred (inc payload))))

(deftest worker-uncaught-error-job-completes
  (testing "Persist should work normally"
    (let [uuid (random-uuid)
          res  (sql-runner (mock-store) (ast/action!! :action/fail 1) {:action-fn action-fn :id uuid})]

      (is (thrown? Throwable (deref res)))

      (testing "job was still marked as failure"
        (is (= "failure" (:workflow_run/status (query-workflow uuid))))))))
