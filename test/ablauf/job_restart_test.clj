(ns ablauf.job-restart-test
  (:require [ablauf.job.ast  :as ast]
            [ablauf.job.sync :as sync]
            [ablauf.job      :refer [restart make make-with-context
                                     ast-zip status]]
            [clojure.test    :refer [deftest is testing are]]
            [ablauf.job :as job]
            [ablauf.job.node :as node]))

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

(defn simple-action-fn
  [{:ast/keys [action] :as x}]
  (assoc x :exec/result
         (if (= :action/fail action) :result/failure :result/success)
         :exec/output action
         :ast/augment #:augment{:source 'identity :dest :result}))

(defn run-sync
  [ast]
  (let [[job {:keys [result]}] (sync/run ast simple-action-fn)
        status                 (job/status job)]
    [status result job]))

(def simple-ast-do
  (ast/do!!
   (ast/action!! :1 {})
   (ast/action!! :2 {})))

(def simple-ast-do-fail
  (ast/do!!
   (ast/action!! :1 {})
   (ast/fail!!)
   (ast/action!! :3 {})))

(def simple-ast-try-rescue
  (ast/try!!
   (ast/action!! :1 {})
   (ast/fail!!)
   (ast/fail!!)
   (rescue!!
    (ast/action!! :4 {}))))

(def simple-ast-try-rescue-fail
  (ast/try!!
   (ast/action!! :1 {})
   (ast/fail!!)
   (rescue!!
    (ast/action!! :2 {})
    (ast/fail!!))))

(def simple-ast-try-rescue-finally
  (ast/try!!
   (ast/action!! :1 {})
   (ast/fail!!)
   (ast/fail!!)
   (rescue!!
    (ast/action!! :2 {}))
   (finally!!
    (ast/action!! :3 {})
    (ast/fail!!))))

(def simple-ast-try-finally
  (ast/try!!
   (ast/action!! :1 {})
   (ast/fail!!)
   (finally!!
    (ast/action!! :4 {}))))

(def simple-ast-try-finally-ok
  (ast/try!!
   (ast/action!! :1 {})
   (ast/action!! :2 {})
   (finally!!
    (ast/action!! :3 {}))))

(def simple-ast-try-finally-fail
  (ast/try!!
   (ast/action!! :1 {})
   (ast/action!! :2 {})
   (ast/action!! :3 {})
   (finally!!
    (ast/fail!!))))

(def simple-ast-try
  (ast/try!!
   (ast/action!! :1 {})
   (ast/action!! :2 {})
   (rescue!!
    (ast/fail!!))))

(def typical-ast1
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
     (ast/fail!!)
     (ast/action!! :a8 {})
     (ast/with-augment [:w :w]
       (ast/action!! :a9 {})))
    (rescue!!
     (ast/action!! :r1 {})
     (ast/fail!!))
    (finally!!
     (ast/action!! :f1 {})))))

(def typical-ast2
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

(def typical-ast3
  (ast/try!!
   (ast/fail!!)
   (rescue!!
    (ast/fail!!))
   (finally!!
    (ast/action!! :f1 {}))))

(def typical-ast4
  (ast/do!! typical-ast3))

(def typical-ast5
  (ast/do!!
   (ast/action!! :a1 {})
   (ast/action!! :a2 {})))

(deftest run-simple-asts-test
  (are [ast status result]
       (= [status result] (take 2 (run-sync ast)))
    simple-ast-do                 :job/success :2
    simple-ast-try                :job/success :2
    simple-ast-do-fail            :job/failure :1
    simple-ast-try-rescue         :job/success :4
    simple-ast-try-rescue-fail    :job/failure :2
    simple-ast-try-rescue-finally :job/failure :3
    simple-ast-try-finally        :job/failure :4
    simple-ast-try-finally-ok     :job/success :3
    simple-ast-try-finally-fail   :job/failure :3
    typical-ast1                  :job/failure :f1
    typical-ast2                  :job/success :f1
    typical-ast3                  :job/failure :f1
    typical-ast4                  :job/failure :f1
    typical-ast5                  :job/success :a2))
