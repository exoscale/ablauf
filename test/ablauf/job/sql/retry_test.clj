(ns ablauf.job.sql.retry-test
  (:require [clojure.walk :as w]
            [manifold.deferred :as d]
            [ablauf.job.ast :as ast]
            [ablauf.job.store :as store]
            [ablauf.job.sql.utils :as sqlu :refer [query-workflow]]
            [clojure.test :refer :all]
            [ablauf.job.sql :as sql]
            [clojure.tools.logging :as log])
  (:import [java.util.concurrent Executors TimeoutException]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

;; comparison of similar objects (lenient with extra keys)
(defn dissoc-keys [ks v]
  (w/prewalk (fn [x]
               (if (map? x)
                 (apply dissoc x ks)
                 x))
             v))

(defn- sql-worker-fn
  [db-spec store action-fn done?]
  (fn []
    (sql/worker db-spec store {:action-fn action-fn} 100 900 done?)
    (log/info "Worker done!")))

(defn- mock-store [atm]
  (reify store/JobStore
    (persist [this uuid context state]
      (reset! atm {:context context :state state})
      (d/success-deferred :ok))))

(defn make-action-fn []
  (let [store (atom #{})]
    (fn action-fn [{:ast/keys [action payload]}]
      (case action
        :action/identity (d/success-deferred payload)
        :action/failonce (if (contains? @store payload)
                           (d/success-deferred :ok)
                           (do
                             (swap! store conj payload)
                             (d/error-deferred :error/failonce)))
        :default (d/error-deferred :error/error)))))

(deftest job-retry-success-test
  (let [ast (ast/do!!
             (ast/action!! :action/identity {:id 1})
             (ast/action!! :action/failonce {:id 1} {:idempotent? true}))]

    (testing "Job fails the first time"
      (let [tpool            (Executors/newFixedThreadPool 2)
            db-spec          sqlu/test-spec
            store-atm        (atom nil)
            uuid             (random-uuid)
            store            (mock-store store-atm)
            action-fn        (make-action-fn)
            job-success?     (fn [] (= "success" (:workflow_run/status (query-workflow uuid))))
            job-failed?      (fn [] (= "failure" (:workflow_run/status (query-workflow uuid))))
            job-done?        (fn [] (or (job-success?)
                                        (job-failed?)))
            wait-job-timeout (fn [seconds]
                               (loop [times 0]
                                 (Thread/sleep 1000)
                                 (when (> times seconds)
                                   (throw (TimeoutException. (format "Waited for job for more than %s seconds" seconds))))
                                 (if-not (job-done?)
                                   (recur (inc times)))))]

        ;; start worker
        (.submit tpool ^Callable (sql-worker-fn db-spec store action-fn job-success?))

        ;;wait
        (sql/submit db-spec store ast {:context {:some "context"} :uuid uuid})

        ;; busy loop+polling for 10s
        (testing "Job failed on first try"
          (wait-job-timeout 10)

          (is (job-failed?) "Job should have failed on the first try")
          (is (sql/submit-retry sqlu/test-spec store 1) "job should be retryable"))

        ;; busy loop+polling for 10s
        (testing "Failed action is idempotent, job retry should succeed"
          (wait-job-timeout 10)
          (is (job-success?) "Job should have succeeded after retry"))

        (testing "Persisted job"
          (let [{ctx :context state :state} @store-atm]
            (testing "Persisted AST matches"
              (is (= (dissoc-keys [:exec/timestamp :exec/duration] state)
                     [#:ast{:type :ast/seq,
                            :nodes [{:ast/type :ast/leaf,
                                     :ast/action :action/identity,
                                     :ast/payload {:id 1},
                                     :ast/id 1,
                                     :exec/result :result/success,
                                     :exec/output {:id 1}}
                                    {:ast/payload {:id 1},
                                     :exec/result :result/success,
                                     :ast/action :action/failonce,
                                     :ast/type :ast/leaf,
                                     :ast/idempotent? true,
                                     :ast/id 2,
                                     :exec/output :ok}],
                            :id 0}
                      nil])))

            (testing "Persisted context"
              (is (= {:some "context", :exec/last-error :error/failonce, :exec/retries 1}
                     ctx)))))

        (testing "Workflow run data"
          (let [workflow-run (query-workflow uuid)]
            (is (= {:some "context", :exec/last-error :error/failonce, :exec/retries 1}
                   (sql/deserialize (:workflow_run/context workflow-run))))))

        ;; finally cleanup
        (.shutdownNow tpool)))))
