(ns ablauf.job.store
  (:require [manifold.deferred :as d]))

(defprotocol JobStore
  (persist [this uuid context state]))

(defn mem-job-store
  [db]
  (reify JobStore
    (persist [_ uuid context state]
      (swap! db assoc uuid {:state state :context context}))))

(defn safe-persist
  "Ensure that persist does not throw and returns output that
   ablauf.job.manifold can safely process"
  [store uuid context state]
  (try
    (d/->deferred
     (persist store uuid context state)
     nil)
    (catch AssertionError ae
      (d/error-deferred (ex-info "assertion-error" {} ae)))
    (catch Exception e
      (d/error-deferred e))))

(defn sync-persist
  [jobstore uuid context state]
  (let [res (persist jobstore uuid context state)]
    (cond-> res (instance? clojure.lang.IDeref res) deref)))
