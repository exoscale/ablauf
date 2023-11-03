(ns ablauf.job.sql.single-connection-multiple-workers-test
  (:require [manifold.deferred :as d]
            [ablauf.job.sql :as sql]
            [ablauf.job.sql.utils :as sqlu :refer [mock-store make-datasource query-workflow sql-worker-fn]]
            [clojure.test :refer :all]
            [hikari-cp.core :as hikari])
  (:import [java.util.concurrent Executors TimeUnit]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(defn- action-fn [{:ast/keys [action payload]}]
  (case action
    ::fail (d/error-deferred :error/error)
    ::sleep (d/chain nil (fn [_] (Thread/sleep 1000) (.getName (Thread/currentThread))))
    ::inc (d/success-deferred (inc payload))))

(deftest job-parallel-single-conn-test
  (let [parnodes (repeat 10 #:ast{:type :ast/leaf, :action ::sleep, :payload 1})
        ast #:ast{:type :ast/par, :nodes parnodes}]

    (testing "single connection, multiple workers"
      (let [uuid (random-uuid)
            stop? (atom false)
            counter (atom 0)
            store (mock-store counter)
            datasource (make-datasource 1)
            workers  10
            executor (Executors/newFixedThreadPool workers)
            callable (sql-worker-fn datasource store action-fn #(deref stop?))]

        (dotimes [_ workers]
          (.submit executor ^Callable callable))
        (sql/submit sqlu/test-spec store ast {:uuid uuid})

        (testing "should run all tasks successfully under 2 secs"
          (.shutdown executor)
          (.awaitTermination executor 2 TimeUnit/SECONDS)
          ;; instruct all threads to stop execution
          (reset! stop? true)
          (.shutdownNow executor)
          (hikari/close-datasource datasource)
          (let [{status :workflow_run/status} (query-workflow uuid)]
            (is (= "success" status))))))))