(ns ablauf.job.ast-gen
  "test.check generators for ablauf ASTs"
  (:require [clojure.test :refer :all]
            [clojure.test.check.generators :as gen]
            [ablauf.job.ast :as ast]))
;;
;; misc
;;

(def gen-payload (gen/map gen/keyword gen/string-alphanumeric {:min-elements 1 :max-elements 3}))

;;;
;; leaf nodes generators

(def gen-action (gen/let [t  gen/keyword
                          pl gen-payload]
                  (ast/action!! t pl)))

(def gen-log (gen/let [txt gen/string-alphanumeric]
               (ast/log!! txt)))

(def gen-random-aug (gen/let [leaf (gen/one-of [gen-action gen-log])
                              k    gen/keyword]
                      (ast/with-augment [identity k]
                        leaf)))

(def gen-random-leaf (gen/one-of [gen-action gen-log gen-random-aug]))

;;;;
;; container nodes generators - parameterized

(defn gen-random-do
  ([] (gen/let [len   (gen/choose 1 5)
                leafs (gen/vector gen-random-leaf len)]
        (gen-random-do leafs)))
  ([leafs]
   (gen/let [l (gen/return leafs)]
     (ast/do!! l))))

(defn gen-random-dopar
  ([] (gen/let [len   (gen/choose 1 5)
                leafs (gen/vector gen-random-leaf len)]
        (gen-random-dopar leafs)))
  ([leafs]
   (gen/let [l (gen/return leafs)]
     (ast/dopar!! l))))

(defn gen-random-try
  ([]
   (gen/let [tnodes (gen/one-of [gen-random-leaf (gen-random-do) (gen-random-dopar)])]
     (gen-random-try tnodes)))
  ([tnodes]
   (gen/let [rnodes (gen/one-of [gen-random-leaf (gen-random-do tnodes) (gen-random-dopar tnodes)])]
     (gen-random-try tnodes rnodes)))
  ([tnodes rnodes]
   (gen/let [fnodes (gen/one-of [gen-random-leaf (gen-random-do tnodes) (gen-random-dopar tnodes)])]
     (gen-random-try tnodes rnodes fnodes)))
  ([tnodes rnodes fnodes]
   (gen/let [t (gen/return tnodes)
             r (gen/return rnodes)
             f (gen/return fnodes)]
     (ast/try!! t
                (rescue!! r)
                (finally!! f)))))

;;;;
;; random tree

(defn random-container [inner]
  (gen/let [leaf inner]
    (gen/one-of [(gen-random-do leaf) (gen-random-dopar leaf) (gen-random-try leaf)])))

(def random-ast (gen/recursive-gen random-container gen-random-leaf))