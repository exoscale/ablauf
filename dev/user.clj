(ns user
  (:require [migratus.core :as migratus]))

(def cfg {:store :database
          :migration-dir "resources/migrations"
          :db {:connection-uri (or (System/getenv "MARIA_JDBC_URI")
                                   "jdbc:mysql://root:root@127.0.0.1:3306/ablauf")}})

(migratus/pending-list cfg)

(comment
  (migratus/migrate cfg)
  (migratus/create cfg "task_pending_status"))