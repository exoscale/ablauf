(ns ablauf.job-prune-test
  (:require [ablauf.job :refer [prune]]
            [clojure.test :refer [deftest is testing]]))

(deftest prune-test
  (testing "removing empty :ast/seq empty leaves"
    (let [tree #:ast{:type :ast/seq
                     :nodes [#:ast{:type :ast/leaf :action :action/log :payload :b}
                             #:ast{:type :ast/seq :nodes []}]}]
      (is (= #:ast{:type :ast/seq
                   :nodes [#:ast{:type :ast/leaf :action :action/log :payload :b}]}
             (prune tree)))))

  (testing "removing dopar empty :ast/seq empty leaves"
    (let [tree #:ast{:type :ast/par
                     :nodes [#:ast{:type :ast/leaf :action :action/log :payload :b}
                             #:ast{:type :ast/seq :nodes []}
                             #:ast{:type :ast/leaf :action :action/log :payload :a}]}]
      (is (= #:ast{:type :ast/par
                   :nodes [#:ast{:type :ast/leaf :action :action/log :payload :b}
                           #:ast{:type :ast/leaf :action :action/log :payload :a}]}
             (prune tree))))

    (testing "removing one level nested :ast/seq empty leaves"
      (let [tree #:ast{:type :ast/seq
                       :nodes [#:ast{:type :ast/seq :nodes []}
                               #:ast{:type :ast/leaf :action :action/log :payload :a}]}]
        (is (= #:ast{:type :ast/seq
                     :nodes [#:ast{:type :ast/leaf :action :action/log :payload :a}]}
               (prune tree)))))

    (testing "removing one deep nested :ast/seq empty leaves"
      (let [tree #:ast{:type :ast/seq
                       :nodes [#:ast{:type :ast/seq :nodes [#:ast{:type :ast/seq :nodes []}]}
                               #:ast{:type :ast/leaf :action :action/log :payload :a}]}]
        (is (= #:ast{:type :ast/seq
                     :nodes [#:ast{:type :ast/leaf :action :action/log :payload :a}]}
               (prune tree))))))

  (testing "do not delete root"
    (let [tree #:ast{:type :ast/par :nodes []}]
      (is (= #:ast{:type :ast/par :nodes []}
             (prune tree)))))

  (testing "remove empty try leaves"
    (let [tree #:ast{:type  :ast/seq
                     :nodes [#:ast{:type :ast/try :nodes []}
                             #:ast{:type :ast/leaf :payload {} :action :a}]}]

      (is (= #:ast{:type :ast/seq
                   :nodes [#:ast{:type :ast/leaf :payload {} :action :a}]}
             (prune tree))))))
