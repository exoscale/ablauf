(ns ablauf.job.manifold-test
  (:require [manifold.stream :as stream]
            [manifold.deferred :as d]
            [ablauf.job :as job]
            [ablauf.job.ast :as ast]
            [ablauf.job.store :as store]
            [ablauf.job.manifold :refer [runner]]
            [clojure.walk :as walk]
            [clojure.test :refer :all]))

(defn- mock-store [{:keys [fail?] :as params}]
  (reify store/JobStore
    (persist [this uuid context state]
      (if fail?
        (d/error-deferred (ex-info "Forced fail" params))
        (d/success-deferred :ok)))))

(deftest persist-impacts-execution
  (let [action-fn (fn [{:ast/keys [action payload]}]
                    (case action
                      :action/fail (d/error-deferred :error/error)
                      ::inc (d/success-deferred (inc payload))))
        ast       (ast/action!! ::inc 1)]

    (testing "Persist should work normally"
      (let [res (runner (mock-store {:fail? false}) ast {:action-fn action-fn})
            [[{result :exec/output}] _] @res]
        (is (= 2 result))))

    (testing "When persist returns a failed deferred execution is halted"
      (is (thrown? Exception
                   @(runner (mock-store {:fail? true}) ast {:action-fn action-fn}))))))

(def parallel-try-ast
  (ast/try!!
   (ast/dopar!!
    (ast/action!! :record :a1)
    (ast/action!! :log :a2))
   (ast/action!! :record :b1)
   (finally!! (ast/action!! :record :f1))))

(defn recording-action-fn
  [state]
  (fn [{:ast/keys [action payload]}]
    (when (= :action/fail action)
      (throw (ex-info "fail" {})))
    (when (= :record action)
      (swap! state conj payload))
    payload))

(defn- memory-store [store]
  (reify store/JobStore
    (persist [this uuid context state]
      (swap! store conj
             (walk/postwalk #(cond-> %
                               (map? %)
                               (dissoc :ast/action
                                       :exec/timestamp
                                       :exec/duration
                                       :exec/output))
                            state)))))

(deftest parallelization-in-try-test
  (let [state     (atom [])
        store     (atom [])
        action-fn (recording-action-fn state)]
    @(runner (memory-store store) parallel-try-ast {:action-fn action-fn})
    (is (= [:a1 :b1 :f1] @state))))

(def typical-ast
  (ast/do!!
   (ast/action!! :a1 {})
   (ast/try!!
    (ast/do!!
     (ast/with-augment [:x :x]
       (ast/action!! :a2 {}))
     (ast/do!!
      (ast/action!! :a3 {})
      (ast/action!! :a4 {}))
     (ast/with-augment [:y :y]
       (ast/action!! :a5 {}))
     (ast/with-augment [:z :z]
       (ast/action!! :a6 {}))
     (ast/action!! :a7 {})
     (ast/action!! :a8 {})
     (ast/with-augment [:w :w]
       (ast/action!! :a9 {})))

    (rescue!!
     (ast/action!! :r1 {})
     (ast/fail!!))
    (finally!!
     (ast/action!! :f1 {})))))

(deftest typical-ast-test
  (let [store       (atom [])
        ast-actions (atom [])
        action-fn   (fn [{:ast/keys [action]}]
                      (swap! ast-actions conj action)
                      (if (or (= :a7 action)
                              (= :action/fail action))
                        (d/error-deferred action)
                        (d/success-deferred action)))]

    (try
      @(runner (memory-store store) typical-ast {:action-fn action-fn})
      (catch Exception _))
    (is (= [:a1 :a2 :a3 :a4 :a5 :a6 :a7 :r1 :action/fail :f1] @ast-actions))))
