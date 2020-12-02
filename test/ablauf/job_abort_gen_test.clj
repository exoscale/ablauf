(ns ablauf.job-abort-gen-test
  (:require [ablauf.job.ast-gen :as ast-gen]
            [ablauf.job :refer [abort make]]
            [ablauf.job :as job]

            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as prop]))

(defspec gen-tree-aborted 100
  (prop/for-all [random-ast ast-gen/random-ast]
                (let [[initial-job] (make random-ast)
                      aborted-job   (abort initial-job)]
                  (testing "job is aborted"
                    (is (job/aborted? aborted-job)))

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
