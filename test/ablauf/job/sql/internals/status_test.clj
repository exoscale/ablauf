(ns ablauf.job.sql.internals.status-test
  "NS for testing private functions.
  Knows a bit too much about the internals."
  (:require [manifold.deferred :as d]
            [ablauf.job.store :as store]
            [ablauf.job.sql :as sql]
            [ablauf.job.sql.utils :as sqlu]
            [clojure.test :refer :all]
            [next.jdbc :as jdbc]
            [hikari-cp.core :as hikari]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(defn- mock-store [counter]
  (reify store/JobStore
    (persist [this uuid context state]
      (swap! counter inc)
      (d/success-deferred :ok))))

(defn make-datasource [max-conns]
  (let [jdbc-url (:connection-uri sqlu/test-spec)]
    (hikari/make-datasource {:jdbc-url              jdbc-url
                             :url                   jdbc-url
                             :maximum-pool-size     max-conns
                             :datasource-class-name "com.mysql.cj.jdbc.MysqlDataSource"
                             :adapter               "mysql"})))

(defn- action-fn [{:ast/keys [action payload]}]
  (case action
    ::inc (d/success-deferred (inc payload))))

(defn- query-task [uuid]
  (jdbc/execute-one!
   sqlu/test-spec
   ["select * from task where wuuid=?" (str uuid)]))

(deftest task-status-pending
  (let [parnodes (repeat 1 #:ast{:type :ast/leaf, :action ::inc, :payload 1})
        ast #:ast{:type :ast/par, :nodes parnodes}
        ;; hack xD
        process-one #'ablauf.job.sql/process-one]

    (testing "Job submission"
      (let [uuid (random-uuid)
            counter (atom 0)
            store (mock-store counter)
            ds (make-datasource 1)]

        (sql/submit sqlu/test-spec store ast {:uuid uuid})

        (testing "Task status is new"
          (let [{:task/keys [status]} (query-task uuid)]
            (is (= "new" status))))

        (testing "Task status stays pending if eg db is unavailable"
          ;; insert workflow restart
          (process-one ds store action-fn)
          (is (thrown? Exception
                       (process-one ds store (fn [& _]
                                               ;; close datasource: ensure that when the action runs
                                               ;; we won't update the db anymore
                                               (hikari/close-datasource ds)))))
          (let [{:task/keys [status]} (query-task uuid)]
            (is (= "pending" status))))))))
