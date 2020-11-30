(ns ablauf.job.error-run
  (:require [ablauf.job.ast  :as ast]
            [ablauf.job.sync :as sync]
            [clojure.test    :refer [deftest is]]))

(def error ::error)

(defn error-in-context?
  [{:exec/keys [last-error]}]
  (= error last-error))

(def actions
  {::fail       #(assoc % :exec/result :result/failure :exec/output error)
   ::success    #(assoc % :exec/result :result/success :exec/output true)
   ::find-error (fn [{:exec/keys [context] :as node}]
                  (assoc node
                         :exec/result :result/success
                         :exec/output {::found (error-in-context? context)}))})

(defn action-fn
  [node]
  (if-let [f (get actions (:ast/action node))]
    (f node)
    (assoc node :exec/result :result/failure :exec/output ::not-found)))

(def failing-ast
  (ast/action!! ::fail true))

(deftest failing-ast-test
  (let [[_ context] (sync/run failing-ast action-fn)]
    (is (error-in-context? context))))

(def failing-with-rescue-ast
  (ast/try!!
   (ast/action!! ::fail true)
   (rescue!!
    (ast/with-augment [::found ::found-in-rescue]
      (ast/action!! ::find-error ::anything)))
   (finally!!
    (ast/with-augment [::found ::found-in-finally]
      (ast/action!! ::find-error ::anything)))))

(deftest failing-with-rescue-test
  (let [[_ context] (sync/run failing-with-rescue-ast action-fn)]
    (is (true? (::found-in-rescue context)))
    (is (true? (::found-in-finally context)))))
