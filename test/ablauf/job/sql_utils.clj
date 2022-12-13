(ns ablauf.job.sql-utils
  (:require [clojure.test :refer [deftest testing]]
            [migratus.core :as migratus]))
;;;;
;; db migrations

(def test-spec
  {:connection-uri (or (System/getenv "MARIA_JDBC_URI")
                       "jdbc:mysql://root:root@127.0.0.1:3306/ablauf")})
(defn migrate-db
  "Install database schema"
  ([]
   (migrate-db test-spec true))
  ([db]
   (migrate-db db true))
  ([db rollback?]
   (let [cfg {:store                :database
              :migration-dir        "migrations/"
              :migration-table-name "migrations"
              :db                   db}]
     (doseq [id (migratus/completed-list cfg)]
       (migratus/rollback cfg))
     (migratus/migrate cfg))))

(defn reset-db-fixture
  [db-spec f]
  (migrate-db db-spec)
  (f))

(defmacro deftestp
  "Allows parameterizing `deftest`, expands the parameters
  onto multiple `(testing ..` clauses. Example:

    (deftestp some-test [op [+ *]]
      (is (= 0 (op 0 0))))
 "
  [name pbindings & body]
  (let [[pname pvals] pbindings]
    `(deftest ~name
       ~@(for [param pvals]
           `(testing ~(str pname ": " (str param) ":")
              (let [~pname ~param]
                ~@body))))))
