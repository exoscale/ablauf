(ns ablauf.job-abort-test
  (:require [ablauf.job.ast :as ast]
            [ablauf.job :refer [abort restart make ast-zip]]
            [ablauf.job :as job]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]))

(deftest restart-test

  (testing "ast-zip: restart with a manually created zipper works"
    (is (= (abort (ast-zip {:ast/type    :ast/leaf,
                            :ast/action  :action/log,
                            :ast/payload {:a :a},
                            :ast/augment #:augment {:source :a, :dest :a},
                            :ast/id      0,
                            :exec/result :result/pending}))

           [{:ast/type    :ast/leaf
             :ast/action  :action/log
             :ast/payload {:a :a}
             :ast/id      0
             :ast/augment {:augment/source :a
                           :augment/dest   :a}
             :exec/result :result/aborted}
            nil]))))

(def finally-abort-step1
  [#:ast{:type :ast/try,
         :nodes
         [#:ast{:type :ast/seq,
                :nodes
                [#:ast{:type :ast/par,
                       :nodes
                       [{:ast/type    :ast/leaf,
                         :ast/action  :record,
                         :ast/payload :a1,
                         :ast/id      3,
                         :exec/result :result/aborted}
                        {:ast/type    :ast/leaf,
                         :ast/action  :log,
                         :ast/payload :a2,
                         :ast/id      4,
                         :exec/result :result/aborted}],
                       :id   2}
                 #:ast{:type    :ast/leaf,
                       :action  :record,
                       :payload :b1,
                       :id      5}],
                :id   1}
          #:ast{:type :ast/seq,
                :nodes
                [#:ast{:type    :ast/leaf,
                       :action  :record,
                       :payload :r1,
                       :id      7}],
                :id   6}
          #:ast{:type :ast/seq,
                :nodes
                [#:ast{:type    :ast/leaf,
                       :action  :record,
                       :payload :f1,
                       :id      9}],
                :id   8}],
         :id   0}
   nil])

(def ast-with-finally
  (ast/try!!
   (ast/dopar!!
    (ast/action!! :record :a1)
    (ast/action!! :log :a2))
   (ast/action!! :record :b1)
   (rescue!! (ast/action!! :record :r1))
   (finally!! (ast/action!! :record :f1))))

(deftest ast-with-finally-abort-test
  (testing "aborting a new ast should work"
    (let [[initial-job] (make ast-with-finally)
          aborted-job   (abort initial-job)]

      (testing "Job that isn't started can be aborted"
        (is (job/aborted? aborted-job))))))

(deftest ast-with-finally-restart-abort-test
  (testing "aborting a ast that is to be dispatched should work"
    (let [[restarted-job] (restart (make ast-with-finally) [])
          aborted-job     (abort restarted-job)]

      (testing "steps are marked as aborted"
        (is (= finally-abort-step1 aborted-job)))

      (testing "status is aborted"
        (is (= :job/aborted (job/status aborted-job))))

      (testing "aborted? is true"
        (is (job/aborted? aborted-job)))

      (testing "aborted job is done"
        (is (job/done? aborted-job)))

      (testing "aborted job is failed "
        (is (job/failed? aborted-job)))

      (testing "aborted job is not eligible "
        (is (not (job/eligible? aborted-job))))

      (testing "aborted job is not pending "
        (is (not (job/pending? aborted-job)))))))
