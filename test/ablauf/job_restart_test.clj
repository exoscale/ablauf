(ns ablauf.job-restart-test
  (:require [ablauf.job.ast  :as ast]
            [ablauf.job.sync :as sync]
            [ablauf.job      :refer [restart make make-with-context
                                     ast-zip status]]
            [clojure.test    :refer [deftest is testing]]))

(deftest restart-test

  (testing "make: restart with no context works"
    (is (= (restart (make (ast/log!! :a)) [])
           [[{:ast/type    :ast/leaf
              :ast/action  :action/log
              :ast/payload :a
              :ast/id      0
              :exec/result :result/pending}
             nil]
            nil ;; no context
            [{:ast/type    :ast/leaf
              :ast/action  :action/log
              :ast/payload :a
              :ast/id      0
              :exec/result :result/pending}]])))

  (testing "make-with-context: restart with a context works"
    (is (= (restart (make-with-context (ast/log!! :a) {:a :b}) [])
           [[{:ast/type    :ast/leaf
              :ast/action  :action/log
              :ast/payload :a
              :ast/id      0
              :exec/result :result/pending}
             nil]
            {:a :b} ;; context is preserved
            [{:ast/type    :ast/leaf
              :ast/action  :action/log
              :ast/payload :a
              :ast/id      0
              :exec/result :result/pending}]])))

  (testing "ast-zip: restart with a manually created zipper works"
    (is (= (restart [(ast-zip {:ast/type         :ast/leaf,
                               :ast/action       :action/log,
                               :ast/payload      {:a :a},
                               :ast/augment      #:augment {:source :a, :dest :a},
                               :ast/id           0,
                               :exec/result      :result/pending})
                     {}] ;; initial context

                    [{:ast/type    :ast/leaf,
                      :ast/action  :action/log,
                      :ast/payload {:a :a},
                      :ast/augment #:augment {:source :a, :dest :b},
                      :ast/id      0,
                      :exec/result :result/success,
                      :exec/output {:a :a}}])
           [[{:ast/type    :ast/leaf
              :ast/action  :action/log
              :ast/payload {:a :a}
              :ast/id      0
              :ast/augment {:augment/source :a
                            :augment/dest   :b}
              :exec/result :result/success
              :exec/output {:a :a}}
             nil]
            {:b :a} ;; context updated
            nil])))) ;; no new dispatchs, we're done

(def finally-restart-step1
  [[#:ast{:type :ast/try,
          :nodes
          [#:ast{:type :ast/seq,
                 :nodes
                 [#:ast{:type :ast/par,
                        :nodes
                        [{:ast/type :ast/leaf,
                          :ast/action :record,
                          :ast/payload :a1,
                          :ast/id 3,
                          :exec/result :result/pending}
                         {:ast/type :ast/leaf,
                          :ast/action :log,
                          :ast/payload :a2,
                          :ast/id 4,
                          :exec/result :result/pending}],
                        :id 2}
                  #:ast{:type :ast/leaf,
                        :action :record,
                        :payload :b1,
                        :id 5}],
                 :id 1}
           #:ast{:type :ast/seq,
                 :nodes
                 [#:ast{:type :ast/leaf,
                        :action :record,
                        :payload :r1,
                        :id 7}],
                 :id 6}
           #:ast{:type :ast/seq,
                 :nodes
                 [#:ast{:type :ast/leaf,
                        :action :record,
                        :payload :f1,
                        :id 9}],
                 :id 8}],
          :id 0}
    nil]
   nil
   [{:ast/type :ast/leaf,
     :ast/action :record,
     :ast/payload :a1,
     :ast/id 3,
     :exec/result :result/pending}
    {:ast/type :ast/leaf,
     :ast/action :log,
     :ast/payload :a2,
     :ast/id 4,
     :exec/result :result/pending}]])

(def ast-with-finally
  (ast/try!!
   (ast/dopar!!
    (ast/action!! :record :a1)
    (ast/action!! :log :a2))
   (ast/action!! :record :b1)
   (rescue!! (ast/action!! :record :r1))
   (finally!! (ast/action!! :record :f1))))

(deftest ast-with-finally-sequencing-test
  (testing "initial restart yields two concurrent actions that are marked as pending"
    (is (= finally-restart-step1
           (restart (make ast-with-finally) [])))))

(defn recording-action-fn
  [state]
  (fn [{:ast/keys [action payload] :as x}]
    (when (= :record action)
      (swap! state conj payload))
    (assoc x :exec/result :result/success :exec/output {})))

(deftest finally-ast-run-output
  (let [state (atom [])
        action-fn (recording-action-fn state)]
    (sync/run ast-with-finally action-fn)
    (testing "running the 'ast-with-finally' job works"
      (is (= [:a1 :b1 :f1] @state)))))
