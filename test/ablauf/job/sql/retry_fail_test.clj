(ns ablauf.job.sql.retry-fail-test
  (:require [manifold.deferred :as d]
            [ablauf.job.ast :as ast]
            [ablauf.job.store :as store]
            [ablauf.job.sql.utils :as sqlu :refer [query-workflow sql-worker-fn]]
            [clojure.test :refer :all]
            [ablauf.job.sql :as sql])
  (:import [java.util.concurrent Executors TimeoutException]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

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
        :action/fail (d/error-deferred :error/fail)
        :action/sleep (do (Thread/sleep 1000)
                          (d/error-deferred :error/fail))
        :default (d/error-deferred :error/error)))))

(deftest job-retry-fail-test
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

    (testing "Failed, non-idempotent job cannot be retried"

      ;;wait
      (sql/submit db-spec store (ast/action!! :action/fail {:id 1}) {:context {} :uuid uuid})

      ;; busy loop+polling for 10s
      (testing "Retrying a failed, non idempotent job"
        (wait-job-timeout 10)
        (is (false? (sql/submit-retry sqlu/test-spec store 1)))))

    ;; dont leave thread lingering
    (.shutdownNow tpool)))

(deftest job-retry-fail-ongoing-test
  (let [tpool            (Executors/newFixedThreadPool 2)
        db-spec          sqlu/test-spec
        store-atm        (atom nil)
        uuid             (random-uuid)
        store            (mock-store store-atm)
        action-fn        (make-action-fn)
        job-success?     (fn [] (= "success" (:workflow_run/status (query-workflow uuid))))
        job-failed?      (fn [] (= "failure" (:workflow_run/status (query-workflow uuid))))
        job-done?        (fn [] (or (job-success?)
                                    (job-failed?)))]

    ;; start worker
    (.submit tpool ^Callable (sql-worker-fn db-spec store action-fn job-done?))

    (testing "Failed, non-idempotent job cannot be retried"

      ;;wait
      (sql/submit db-spec store (ast/action!! :action/fail {:id 1}) {:context {} :uuid uuid})

      (testing "Ongoing job cannot be retried"

        ;;wait
        (sql/submit db-spec store (ast/action!! :action/sleep {:id 1}) {:context {:some "context"} :uuid uuid})

        ;; busy loop+polling for 10s
        (testing "Retrying an ongoing job fails"
          (is (thrown? Exception
                       (sql/submit-retry sqlu/test-spec store 1))))))

    ;; dont leave thread lingering
    (.shutdownNow tpool)))
