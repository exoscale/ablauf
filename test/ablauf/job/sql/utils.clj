(ns ablauf.job.sql.utils
  (:require [ablauf.job.sql :as sql]
            [clojure.test :refer [deftest testing]]
            [clojure.tools.logging :as log]
            [hikari-cp.core :as hikari]
            [manifold.deferred :as d]
            [migratus.core :as migratus]
            [next.jdbc :as jdbc])
  (:import [ablauf.job.store JobStore]))

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
     (when rollback?
       (doseq [id (migratus/completed-list cfg)]
         (migratus/rollback cfg)))
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

;;;
;; generic utilities

(defn mock-store
  ([]
   (reify JobStore
     (persist [this uuid context state]
       (d/success-deferred :ok))))
  ([counter]
   (reify JobStore
     (persist [this uuid context state]
       (swap! counter inc)
       (d/success-deferred :ok)))))

(defn sql-worker-fn
  [db-spec store action-fn done?]
  (fn []
    (sql/worker db-spec store {:action-fn action-fn} 100 900 done?)
    (log/info "Worker done!")))

(defn make-datasource [max-conns]
  (let [jdbc-url (:connection-uri test-spec)]
    (hikari/make-datasource {:jdbc-url              jdbc-url
                             :url                   jdbc-url
                             :maximum-pool-size     max-conns
                             :datasource-class-name "com.mysql.cj.jdbc.MysqlDataSource"
                             :adapter               "mysql"})))

(defn query-task [uuid]
  (jdbc/execute-one! test-spec ["select * from task where wuuid=?" (str uuid)]))

(defn query-workflow [uuid]
  (jdbc/execute-one! test-spec ["select * from workflow_run where uuid=?" (str uuid)]))
