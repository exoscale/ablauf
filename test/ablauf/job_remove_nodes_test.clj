(ns ablauf.job-remove-nodes-test
  (:require [ablauf.job :refer [remove-nodes-by ast-zip]]
            [clojure.test :refer [deftest is testing]]
            [clojure.zip :as zip]))

(deftest remove-nodes-via-predicate-test
  (testing "Remove with non-matching predicate"
    (let [f         (constantly false)
          xform-ast (comp zip/node #(remove-nodes-by %1 f) ast-zip)
          ast       #:ast{:type :ast/leaf}]
      (is (= ast (xform-ast ast)))))

  (testing "Remove with non-matching predicate"
    (let [;; remove ids 3 and 5
          f         (fn [{:ast/keys [id]}] (#{3 5} id))
          xform-ast (comp zip/node #(remove-nodes-by %1 f) ast-zip)
          ast       #:ast{:type :ast/seq
                          :id 1
                          :nodes [#:ast{:id 2 :type :ast/leaf, :action :action/log, :payload 1}
                                  #:ast{:id 3 :type :ast/leaf, :action :action/log, :payload 2}
                                  #:ast{:type :ast/seq
                                        :id 4
                                        :nodes [#:ast{:type :ast/seq
                                                      :id 5
                                                      :nodes [#:ast{:id 6 :type :ast/leaf, :action :action/log, :payload 1}
                                                              #:ast{:id 7 :type :ast/leaf, :action :action/log, :payload 2}]}
                                                #:ast{:id 8 :type :ast/leaf, :action :action/log, :payload 2}]}]}
          expected #:ast{:type :ast/seq
                         :id 1
                         :nodes [#:ast{:id 2 :type :ast/leaf, :action :action/log, :payload 1}
                                 #:ast{:type :ast/seq
                                       :id 4
                                       :nodes [#:ast{:id 8 :type :ast/leaf, :action :action/log, :payload 2}]}]}]

      (is (= expected (xform-ast ast))))))
