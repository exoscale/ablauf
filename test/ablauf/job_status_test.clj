(ns ablauf.job-status-test
  (:require [ablauf.job.ast  :as ast]
            [ablauf.job      :refer [restart make make-with-context
                                     ast-zip status]]
            [clojure.test    :refer [deftest is testing]]))

(deftest status-test
  (is (= :job/pending (status (first (make (ast/action!! :a :a))))))

  (is (= :job/pending
         (status [{:ast/type :ast/seq, :ast/nodes [{:ast/type :ast/leaf
                                                    :exec/result :result/pending}]}])))
  (is (= :job/aborted
         (status [{:ast/type :ast/seq, :ast/nodes [{:ast/type :ast/leaf
                                                    :exec/result :result/aborted}]}])))
  (is (= :job/failure
         (status [{:ast/type :ast/seq, :ast/nodes [{:ast/type :ast/leaf
                                                    :exec/result :result/failure}]}])))

  (is (= :job/success
         (status [{:ast/type :ast/seq, :ast/nodes [{:ast/type :ast/leaf
                                                    :exec/result :result/success}]}]))))
