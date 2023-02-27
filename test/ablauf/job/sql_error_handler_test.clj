(ns ablauf.job.sql-error-handler-test
  (:require [clojure.test :refer :all]
            [spy.core :as spy]
            [manifold.deferred :as d]
            [ablauf.job.ast :as ast]
            [ablauf.job.store :as store]
            [ablauf.job.sql-utils :as sqlu]
            [ablauf.job.manifold-sql :as msql]
            [ablauf.job.sql :as sql]))

(use-fixtures :each (partial sqlu/reset-db-fixture sqlu/test-spec))

(def sql-runner (msql/make-sql-runner {:connection-uri "jdbc:mysql://xxx:yyy@127.0.0.1:3306/foobar2000"}))

(defn- mock-store []
  (reify store/JobStore
    (persist [this uuid context state]
      (d/success-deferred :ok))))

(deftest job-archival-test
  (let [ast (ast/action!! ::inc 1)]

    (testing "Persist should work normally"
      (let [error-handler (spy/spy (fn [e] (throw (ex-info "rethrow" {} e))))
            bad-uri       {:connection-uri "jdbc:mysql://xxx:yyy@127.0.0.1:3306/foobar2000"}
            opts          {:action-fn     (constantly :ok)
                           :id            (random-uuid)
                           :error-handler error-handler}
            ;; stop after 2s
            stopfn        (constantly false)
            store         (mock-store)]

        (is (thrown-with-msg?
             Exception
             #"rethrow"
             (sql/worker bad-uri store opts 100 900 stopfn)))
        (is (spy/called-n-times? error-handler 1))))))
;;wait

