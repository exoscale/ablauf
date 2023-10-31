(ns ablauf.job.sql.massive-parallel-test
  (:require [manifold.deferred :as d]
            [ablauf.job.sql :as sql]
            [ablauf.job.sql.utils :as sqlu :refer [mock-store query-workflow sql-worker-fn]]
            [clojure.test :refer :all])
  (:import [java.util.concurrent Executors TimeUnit]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(defn- action-fn [{:ast/keys [action payload]}]
  (case action
    :action/fail (d/error-deferred :error/error)
    ::inc (d/success-deferred (inc payload))))

(deftest job-massive-parallel-test
  (let [parallel 20
        parnodes (repeat parallel #:ast{:type :ast/leaf, :action ::inc, :payload 1})
        ast      #:ast{:type :ast/par, :nodes parnodes}]

    (testing "Each worker thread will have a chance to update the workflow"
      (let [uuid     (random-uuid)
            stop?    (atom false)
            counter  (atom 0)
            store    (mock-store counter)
            executor (Executors/newFixedThreadPool 100)
            callable (sql-worker-fn sqlu/test-spec store action-fn #(deref stop?))]

        (sql/submit sqlu/test-spec store ast {:uuid uuid})

        ;; 1 thread -> 1 leaf
        ;; not necessarily every thread will have a chance to run a leaf
        (dotimes [_ parallel]
          (.submit executor ^Callable callable))

        ;; initiate orderly shutdown
        ;; ensure workers stop when all is persisted
        ;; kinda relies on knowing that persist will be called N times
        (add-watch counter nil
                   (fn [_ _ _ val] (when (= val (inc parallel))
                                     (reset! stop? true))))
        (.shutdown executor)
        ;; give ~1sec per thread, more than enough
        (.awaitTermination executor parallel TimeUnit/SECONDS)

        (let [{status :workflow_run/status} (query-workflow uuid)]
          (is (= "success" status)))))))

