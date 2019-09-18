(ns ablauf.job.store)

(defprotocol JobStore
  (persist [this uuid context state]))

(defn mem-job-store
  [db]
  (reify JobStore
    (persist [this uuid context state]
      (swap! db assoc uuid {:state state :context context}))))
