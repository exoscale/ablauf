(ns ablauf.job-test
  (:require [ablauf.job.node :as node]
            [ablauf.job.ast  :as ast]
            [ablauf.job      :refer :all]
            [clojure.test    :refer :all]))

(deftest restart-test

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
            :exec/result :result/pending}]]))

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
            :exec/result :result/pending}]]))

  (is (= (restart [(ast-zip {:ast/type :ast/leaf,
                             :ast/action       :action/log,
                             :ast/payload      {:a :a},
                             :ast/augment      #:augment {:source :a, :dest :a},
                             :ast/id           0,
                             :exec/result      :result/pending})
                   {} ;; initial context
                   ]
                  [{:ast/type    :ast/leaf,
                    :ast/action  :action/log,
                    :ast/payload {:a :a},
                    :ast/augment #:augment {:source :a, :dest :a},
                    :ast/id      0,
                    :exec/result :result/success,
                    :exec/output {:a :a}}])
         [[{:ast/type    :ast/leaf
            :ast/action  :action/log
            :ast/payload {:a :a}
            :ast/id      0
            :ast/augment {:augment/source :a
                          :augment/dest   :a}
            :exec/result :result/success
            :exec/output {:a :a}}
           nil]
          {:a :a} ;; context updated
          nil ;; no new dispatchs, we're done
          ])))

(deftest status-test
  (is (= :job/pending
         (status [{:ast/type :ast/seq, :ast/nodes [{:ast/type :ast/leaf
                                                    :exec/result :result/pending}]}])))
  (is (= :job/failure
         (status [{:ast/type :ast/seq, :ast/nodes [{:ast/type :ast/leaf
                                                    :exec/result :result/failure}]}])))
  (is (= :job/success
         (status [{:ast/type :ast/seq, :ast/nodes [{:ast/type :ast/leaf
                                                    :exec/result :result/success}]}]))))
