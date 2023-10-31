(ns ablauf.job.sql.basic-test
  (:require [manifold.deferred :as d]
            [ablauf.job.ast :as ast]
            [ablauf.job.sql.utils :as sqlu :refer [mock-store query-workflow query-task]]
            [ablauf.job.manifold-sql :as msql]
            [clojure.test :refer :all]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(def sql-runner (msql/make-sql-runner sqlu/test-spec))

(defn- action-fn [{:ast/keys [action payload]}]
  (case action
    :action/fail (d/error-deferred :error/error)
    ::inc (d/success-deferred (inc payload))))

(deftest job-archival-test
  (let [ast (ast/action!! ::inc 1)]

    (testing "Persist should work normally"
      (let [uuid (random-uuid)
            res  (sql-runner (mock-store) ast {:action-fn action-fn :id uuid})]

        ;;wait
        (deref res)

        (testing "data was archived"
          (is (some? (query-workflow uuid))))

        (testing "Tasks are cleaned"
          (is (empty? (query-task uuid))))))))

(deftest job-failure-archival-test
  (let [ast (ast/action!! :action/fail 1)]

    (testing "Persist should work normally"
      (let [uuid (random-uuid)
            res  (sql-runner (mock-store) ast {:action-fn action-fn :id uuid})]

        ;;wait
        (is (thrown? Exception (deref res)))

        (testing "job was still marked as failure"
          (is (= "failure" (:workflow_run/status (query-workflow uuid)))))

        (testing "Workflow run exists"
          (is (some? (query-workflow uuid))))

        (testing "Tasks are cleaned"
          (is (empty? (query-task uuid))))))))
