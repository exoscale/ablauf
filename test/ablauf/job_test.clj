(ns ablauf.job-test
  (:require [clojure.test :refer :all]
            [clojure.zip     :as zip]
            [ablauf.job :refer [prepare-replay ast-zip abort]]))

(def ast-prepare-replay (comp zip/node prepare-replay ast-zip))

(deftest replay-prepare-no-idempotent-test
  (testing "ast/leaf node should be marked as failure"
    (let [ast  #:ast{:payload     :a
                     :action      :action/log
                     :type        :ast/leaf
                     :exec/result :result/pending}
          expected #:ast{:payload     :a
                         :action      :action/log
                         :type        :ast/leaf
                         :exec/result :result/failure}]
      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/seq node should mark pending leafs as failure"
    (let [ast #:ast{:type :ast/seq
                    :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/pending}
                            #:ast{:payload :a :action :action/log :type :ast/leaf}]}
          expected #:ast{:type :ast/seq
                         :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/failure}
                                 #:ast{:payload :a :action :action/log :type :ast/leaf}]}]
      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/par node should mark pending leafs as failure"
    (let [ast #:ast{:type :ast/par
                    :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/pending}
                            #:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/pending}]}
          expected #:ast{:type :ast/par
                         :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/failure}
                                 #:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/failure}]}]
      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/par node should mark all pending leafs as failure"
    (let [ast #:ast{:type :ast/par
                    :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/success}
                            #:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/pending}
                            #:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/pending}]}
          expected #:ast{:type :ast/par
                         :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/success}
                                 #:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/failure}
                                 #:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/failure}]}]
      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/try node should mark pending try-leafs as failure"
    (let [ast #:ast{:type :ast/try
                    :nodes
                    [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/pending}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]}
          expected #:ast{:type :ast/try
                         :nodes
                         [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]}]

      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/try node should mark pending rescue-leafs as failure"
    (let [ast #:ast{:type :ast/try
                    :nodes
                    [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/pending}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]}
          expected #:ast{:type :ast/try
                         :nodes
                         [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/failure}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]}]

      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/try node should mark pending finaly-leafs as failure"
    (let [ast #:ast{:type :ast/try
                    :nodes
                    [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/success}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1 :exec/result :result/pending}]}]}
          expected #:ast{:type :ast/try
                         :nodes
                         [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/success}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1 :exec/result :result/failure}]}]}]

      (is (= expected (ast-prepare-replay ast))))))

(deftest replay-prepare-idempotent-test
  (testing "ast/leaf node should mark pending idempotent leafs as unstarted"
    (let [ast  #:ast{:payload     :a
                     :action      :action/log
                     :type        :ast/leaf
                     :idempotent? true
                     :exec/result :result/pending}
          expected #:ast{:payload     :a
                         :action      :action/log
                         :type        :ast/leaf
                         :idempotent? true}]
      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/leaf node should be marked as unstarted when failed but is idempotent"
    (let [ast  #:ast{:payload     :a
                     :action      :action/log
                     :type        :ast/leaf
                     :idempotent? true
                     :exec/result :result/failure}
          expected #:ast{:payload     :a
                         :action      :action/log
                         :type        :ast/leaf
                         :idempotent? true}]
      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/leaf node should NOT be marked as unstarted when not in pending state"
    (let [ast  #:ast{:payload     :a
                     :action      :action/log
                     :type        :ast/leaf
                     :idempotent? true
                     :exec/result :result/success}
          expected #:ast{:payload     :a
                         :action      :action/log
                         :type        :ast/leaf
                         :idempotent? true
                         :exec/result :result/success}]
      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/seq node should mark pending idempotent leafs as unstarted"
    (let [ast #:ast{:type :ast/seq
                    :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/pending :idempotent? true}
                            #:ast{:payload :a :action :action/log :type :ast/leaf}]}
          expected #:ast{:type :ast/seq
                         :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :idempotent? true}
                                 #:ast{:payload :a :action :action/log :type :ast/leaf}]}]
      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/par node should mark all pending idempotent leafs as unstarted"
    (let [ast #:ast{:type :ast/par
                    :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/success :idempotent? true}
                            #:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/pending :idempotent? true}]}
          expected #:ast{:type :ast/par
                         :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/success  :idempotent? true}
                                 #:ast{:payload :a :action :action/log :type :ast/leaf :idempotent? true}]}]
      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/par node should mark all pending idempotent leafs as unstarted"
    (let [ast #:ast{:type :ast/par
                    :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/success}
                            #:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/pending :idempotent? true}
                            #:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/pending :idempotent? true}]}
          expected #:ast{:type :ast/par
                         :nodes [#:ast{:payload :a :action :action/log :type :ast/leaf :exec/result :result/success}
                                 #:ast{:payload :a :action :action/log :type :ast/leaf :idempotent? true}
                                 #:ast{:payload :a :action :action/log :type :ast/leaf :idempotent? true}]}]
      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/try node should mark pending idempotent try-leafs as unstarted"
    (let [ast #:ast{:type :ast/try
                    :nodes
                    [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/pending :idempotent? true}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]}
          expected #:ast{:type :ast/try
                         :nodes
                         [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :idempotent? true}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]}]

      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/try node should mark pending idempotent rescue-leafs as unstarted"
    (let [ast #:ast{:type :ast/try
                    :nodes
                    [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/pending :idempotent? true}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]}
          expected #:ast{:type :ast/try
                         :nodes
                         [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :idempotent? true}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]}]

      (is (= expected (ast-prepare-replay ast)))))

  (testing "ast/try node should mark pending idempotent finaly-leafs as unstarted"
    (let [ast #:ast{:type :ast/try
                    :nodes
                    [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/success}]}
                     #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1 :exec/result :result/pending :idempotent? true}]}]}
          expected #:ast{:type :ast/try
                         :nodes
                         [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/success}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1 :idempotent? true}]}]}]

      (is (= expected (ast-prepare-replay ast))))))

(deftest abort-test
  (testing "abort call without reason must default reason to 'aborted'"
    (let [job  (ast-zip #:ast{:payload     :a
                              :action      :action/log
                              :type        :ast/leaf
                              :exec/result :result/pending})
          expected #:ast{:payload     :a
                         :action      :action/log
                         :type        :ast/leaf
                         :exec/result :result/failure
                         :exec/reason "aborted"}]
      (is (= expected (-> (abort job) zip/node)))))

  (testing "should mark pending leaf as failure and with reason"
    (let [job  (ast-zip #:ast{:payload     :a
                              :action      :action/log
                              :type        :ast/leaf
                              :exec/result :result/pending})
          expected #:ast{:payload     :a
                         :action      :action/log
                         :type        :ast/leaf
                         :exec/result :result/failure
                         :exec/reason "timeout"}]
      (is (= expected (-> (abort job "timeout") zip/node)))))

  (testing "should mark seq pending leaf as failure and reason"
    (let [job (ast-zip #:ast{:type :ast/seq
                             :nodes [#:ast{:payload :a :action :log :type :ast/leaf :exec/result :result/pending}
                                     #:ast{:payload :a :action :log :type :ast/leaf}]})
          expected #:ast{:type :ast/seq
                         :nodes [#:ast{:payload :a :action :log :type :ast/leaf :exec/result :result/failure :exec/reason "aborted"}
                                 #:ast{:payload :a :action :log :type :ast/leaf}]}]
      (is (= expected (-> (abort job) zip/node)))))

  (testing "should mark all par pending leafs as failure and reason"
    (let [job (ast-zip #:ast{:type :ast/par
                             :nodes [#:ast{:payload :a :action :log :type :ast/leaf :exec/result :result/success}
                                     #:ast{:payload :a :action :log :type :ast/leaf :exec/result :result/pending}
                                     #:ast{:payload :a :action :log :type :ast/leaf :exec/result :result/pending}]})
          expected #:ast{:type :ast/par
                         :nodes [#:ast{:payload :a :action :log :type :ast/leaf :exec/result :result/success}
                                 #:ast{:payload :a :action :log :type :ast/leaf :exec/result :result/failure :exec/reason "failed"}
                                 #:ast{:payload :a :action :log :type :ast/leaf :exec/result :result/failure :exec/reason "failed"}]}]
      (is (= expected (-> (abort job "failed") zip/node)))))

  (testing "should mark try-leafs as failure and reason"
    (let [job (ast-zip #:ast{:type :ast/try
                             :nodes
                             [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/pending}]}
                              #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1}]}
                              #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]})
          expected #:ast{:type :ast/try
                         :nodes
                         [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure :exec/reason "aborted"}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]}]

      (is (= expected (-> (abort job) zip/node)))))

  (testing "ast/try node should mark pending rescue-leafs as failure"
    (let [job (ast-zip #:ast{:type :ast/try
                             :nodes
                             [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                              #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/pending}]}
                              #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]})
          expected #:ast{:type :ast/try
                         :nodes
                         [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/failure :exec/reason "aborted"}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1}]}]}]

      (is (= expected (-> (abort job) zip/node)))))

  (testing "ast/try node should mark pending finaly-leafs as failure"
    (let [job (ast-zip #:ast{:type :ast/try
                             :nodes
                             [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                              #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/success}]}
                              #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1 :exec/result :result/pending}]}]})
          expected #:ast{:type :ast/try
                         :nodes
                         [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :try :payload :t1 :exec/result :result/failure}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :rescue :payload :r1 :exec/result :result/success}]}
                          #:ast{:type :ast/seq :nodes [#:ast{:type :ast/leaf :action :finally :payload :f1 :exec/result :result/failure :exec/reason "timeout"}]}]}]

      (is (= expected (-> (abort job "timeout") zip/node))))))