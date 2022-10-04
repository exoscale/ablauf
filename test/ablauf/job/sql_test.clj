(ns ablauf.job.sql-test
  (:require [manifold.deferred :as d]
            [ablauf.job.ast :as ast]
            [ablauf.job.store :as store]
            [ablauf.job.manifold :refer [runner]]
            [ablauf.job.sql-utils :as sqlu]
            [ablauf.job.manifold-sql :as msql]
            [clojure.test :refer :all]
            [next.jdbc :as jdbc]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(def sql-runner (msql/make-sql-runner sqlu/test-spec))

(defn- mock-store []
  (reify store/JobStore
    (persist [this uuid context state]
      (d/success-deferred :ok))))

(defn- action-fn [{:ast/keys [action payload]}]
  (case action
    :action/fail (d/error-deferred :error/error)
    ::inc (d/success-deferred (inc payload))))

(defn- query-workflow [uuid]
  (jdbc/execute-one!
   sqlu/test-spec
   ["select * from workflow_run where uuid=?" (str uuid)]))

(defn- query-task [uuid]
  (jdbc/execute!
   sqlu/test-spec
   ["select * from task where wuuid=?" (str uuid)]))

(deftest job-archival-test
  (let [ast (ast/action!! ::inc 1)]

    (testing "Persist should work normally"
      (let [uuid (random-uuid)
            res  (sql-runner (mock-store) ast {:action-fn action-fn :id uuid})]

        ;;wait
        (deref res)

        (testing "data was archived"
          (is (some? (query-workflow uuid))))))))

(deftest job-failure-archival-test
  (let [ast (ast/action!! :action/fail 1)]

    (testing "Persist should work normally"
      (let [uuid (random-uuid)
            res  (sql-runner (mock-store) ast {:action-fn action-fn :id uuid})]

        ;;wait
        (is (thrown? Exception (deref res)))

        (testing "data was archived"
          (is (some? (query-workflow uuid))))))))
