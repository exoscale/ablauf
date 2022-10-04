(ns ablauf.job.ast-test
  (:require [ablauf.job.node :as node]
            [ablauf.job.ast  :refer :all]
            [clojure.test    :refer :all]))

(deftest ast-shape

  (testing "Basic AST shapes"

    (is (= (log!! :a) #:ast{:type :ast/leaf, :action :action/log, :payload :a}))

    (is (= (with-augment [:a :log-result] (log!! {:a :b}))
           #:ast{:type :ast/leaf, :action :action/log, :payload {:a :b}
                 :augment #:augment{:source :a, :dest :log-result}}))

    (is (= (fail!!) #:ast{:type :ast/leaf, :action :action/fail, :payload {}}))

    (is (= (do!! (log!! :a) (log!! :b))
           #:ast{:type :ast/seq,
                 :nodes
                 [#:ast{:type :ast/leaf, :action :action/log, :payload :a}
                  #:ast{:type :ast/leaf, :action :action/log, :payload :b}]}))

    (is (= (dopar!! (log!! :a) (log!! :b))
           #:ast{:type :ast/par,
                 :nodes
                 [#:ast{:type :ast/leaf, :action :action/log, :payload :a}
                  #:ast{:type :ast/leaf, :action :action/log, :payload :b}]}))

    (is (= (idempotent-action!! :idempotent-action {:a "payload"})
           #:ast{:action      :idempotent-action
                 :payload     {:a "payload"}
                 :idempotent? true
                 :type        :ast/leaf}))

    (is (= (action!! :idempotent-action {:a "payload"} {:idempotent? true})
           #:ast{:action      :idempotent-action
                 :payload     {:a "payload"}
                 :idempotent? true
                 :type        :ast/leaf}))

    (is (= (action!! :idempotent-action {:a "payload"} {:idempotent? false})
           #:ast{:action      :idempotent-action
                 :payload     {:a "payload"}
                 :idempotent? false
                 :type        :ast/leaf}))

    (is (= (action!! :idempotent-action {:a "payload"})
           #:ast{:action  :idempotent-action
                 :payload {:a "payload"}
                 :type    :ast/leaf})))

  (testing "AST dispatchs for log!!"

    (let [dispatchs (node/find-dispatchs (log!! :a))]
      (is (= [{:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :a
               :exec/result :result/pending}]
             dispatchs)))

    (let [dispatchs (node/find-dispatchs (do!! (log!! :a) (log!! :b)))]
      (is (= [{:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :a
               :exec/result :result/pending}]
             dispatchs)))

    (let [dispatchs (node/find-dispatchs (dopar!! (log!! :a) (log!! :b)))]
      (is (= [{:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :a
               :exec/result :result/pending}
              {:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :b
               :exec/result :result/pending}]
             dispatchs)))

    (let [dispatchs (node/find-dispatchs
                     (assoc (action!! :action/log {:a :a} {:idempotent? false}) :exec/result :result/pending))]
      (is (nil? dispatchs)))

    (let [dispatchs (node/find-dispatchs
                     (assoc (action!! :action/log {:a :a} {:idempotent? true}) :exec/result :result/failure))]
      (is (nil? dispatchs)))

    (let [dispatchs (node/find-dispatchs (try!! (log!! :a)))]
      (is (= [{:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :a
               :exec/result :result/pending}]
             dispatchs)))

    (let [dispatchs (node/find-dispatchs (try!! (finally!! (log!! :a))))]
      (is (= [{:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :a
               :exec/result :result/pending}]
             dispatchs)))

;; Let's play with failures and observe behavior

    (let [base-ast
          (try!! (log!! :a) (log!! :b)
                 (rescue!! (log!! :r))
                 (finally!!
                  (log!! :f)))

          first-is-done
          (assoc-in base-ast [:ast/nodes 0 :ast/nodes 0 :exec/result] :result/success)

          next-is-rescue
          (assoc-in base-ast [:ast/nodes 0 :ast/nodes 0 :exec/result] :result/failure)

          next-is-finally
          (-> base-ast
              (assoc-in [:ast/nodes 0 :ast/nodes 0 :exec/result] :result/success)
              (assoc-in [:ast/nodes 0 :ast/nodes 1 :exec/result] :result/success))

          finally-after-rescue
          (-> base-ast
              (assoc-in [:ast/nodes 0 :ast/nodes 0 :exec/result] :result/failure)
              (assoc-in [:ast/nodes 1 :ast/nodes 0 :exec/result] :result/success))

          all-done
          (-> base-ast
              (assoc-in [:ast/nodes 0 :ast/nodes 0 :exec/result] :result/success)
              (assoc-in [:ast/nodes 0 :ast/nodes 1 :exec/result] :result/success)
              (assoc-in [:ast/nodes 2 :ast/nodes 0 :exec/result] :result/success))]

      (is (= [{:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :a
               :exec/result :result/pending}]
             (node/find-dispatchs base-ast)))

      (is (= [{:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :b
               :exec/result :result/pending}]
             (node/find-dispatchs first-is-done)))

      (is (= [{:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :r
               :exec/result :result/pending}]
             (node/find-dispatchs next-is-rescue)))

      (is (= [{:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :f
               :exec/result :result/pending}]
             (node/find-dispatchs next-is-finally)))

      (is (= [{:ast/type    :ast/leaf
               :ast/action  :action/log
               :ast/payload :f
               :exec/result :result/pending}]
             (node/find-dispatchs finally-after-rescue)))

      (is (empty? (node/find-dispatchs all-done))))))

(deftest try-shape

  ;; Try is a bit of a special beast since there's no guarantee
  ;; that either rescue or finally nodes are present

  (testing "empty try ast"

    (let [try-ast (try!!)]

      (is (empty? (node/find-dispatchs try-ast)))
      (is (node/done? try-ast))

      (is (node/done? (try-nodes try-ast)))
      (is (node/done? (rescue-nodes try-ast)))
      (is (node/done? (finally-nodes try-ast)))

      (is (not (node/failed? (try-nodes try-ast))))
      (is (not (node/failed? (rescue-nodes try-ast))))
      (is (not (node/failed? (finally-nodes try-ast))))

      (is (= #:ast{:type :ast/seq, :nodes []} (try-nodes try-ast)))
      (is (= #:ast{:type :ast/seq, :nodes []} (rescue-nodes try-ast)))
      (is (= #:ast{:type :ast/seq, :nodes []} (finally-nodes try-ast))))))

(deftest expected-failures

  (let [base-ast (do!! (fail!!) (log!! :a) (log!! :b))]

    (is (=
         [{:ast/type    :ast/leaf,
           :ast/action  :action/fail,
           :ast/payload {},
           :exec/result :result/pending}]
         (node/find-dispatchs base-ast)))

    (is (empty?
         (node/find-dispatchs
          (assoc-in base-ast [:ast/nodes 0 :exec/result] :result/pending))))

    (is (empty?
         (node/find-dispatchs
          (update-in base-ast [:ast/nodes 0] assoc
                     :exec/result :result/failure
                     :exec/output {:fail? true}))))))
