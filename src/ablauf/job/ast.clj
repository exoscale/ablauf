(ns ablauf.job.ast
  "Specifies an AST and provides helper functions
   to build it"
  (:require [clojure.spec.alpha :as s]))

(def hierarchy
  "AST node types hierarchy"
  (-> (make-hierarchy)
      (derive :ast/seq :ast/branch)
      (derive :ast/par :ast/branch)
      (derive :ast/try :ast/branch)))

(s/def :ast/nodes     (s/coll-of ::ast))
(s/def :ast/action    keyword?)
(s/def :ast/payload   any?)
(s/def :ast/type      #{:ast/leaf :ast/seq :ast/par :ast/try})
(s/def :ast/idempotent? boolean?)

(defmulti  spec-by-ast-type :ast/type :hierarchy #'hierarchy)

(defmethod spec-by-ast-type :ast/branch
  [_]
  (s/keys :req [:ast/nodes]))

(defmethod spec-by-ast-type :ast/leaf
  [_]
  (s/keys :req [:ast/action :ast/payload]
          :opt [:ast/idempotent?]))

(s/def ::ast (s/multi-spec spec-by-ast-type :ast/type))

(defn branch?
  "Predicate to test whether a node is a branch"
  [node]
  (isa? hierarchy (:ast/type node) :ast/branch))

(defn leaf?
  "Predicate to test whether a node is a leaf"
  [node]
  (= :ast/leaf (:ast/type node)))

(defn log!!
  "Log action"
  [txt]
  {:ast/type    :ast/leaf
   :ast/action  :action/log
   :ast/payload txt})

(defn action!!
  ([type payload]
   {:ast/type    :ast/leaf
    :ast/action  type
    :ast/payload payload})
  ([type payload {:keys [idempotent?] :as _opts}]
   (cond-> {:ast/type    :ast/leaf
            :ast/action  type
            :ast/payload payload}
     (some? idempotent?) (assoc :ast/idempotent? idempotent?))))

(defn idempotent-action!! [type payload]
  (action!! type payload {:idempotent? true}))

(defn fail!!
  "Forcibly fail action"
  []
  {:ast/type    :ast/leaf
   :ast/action  :action/fail})

(defn do!!
  "Yields a branch of sequential actions"
  [& nodes]
  {:ast/type :ast/seq :ast/nodes (vec (remove nil? nodes))})

(defn dopar!!
  "Yields a branch of parallel actions"
  [& nodes]
  (assoc (apply do!! nodes) :ast/type :ast/par))

(defn try-extract
  "Tests whether the last form of a list is a list of
   starting with a special symbol. If found yields a
   tuple of preceding forms and the last special form
   list:

        (try-extract 'finally
          '(:foo :bar (finally :bim :bam)))

        ;; => ['(:foo :bar) '(:bim :bam)]

   This is used to break apart rescue and finally from
   try statements"
  [sym forms]
  (if (and (list? (last forms)) (= sym (first (last forms))))
    [(vec (drop-last forms)) (drop 1 (last forms))]
    [forms]))

(defmacro try!!
  "Yields a try statement, with potential rescue and finally
   special branches"
  [& forms]
  (let [[forms finally] (try-extract 'finally!! forms)
        [forms rescue]  (try-extract 'rescue!! forms)]
    {:ast/type  :ast/try
     :ast/nodes [{:ast/type  :ast/seq
                  :ast/nodes (vec forms)}
                 {:ast/type  :ast/seq
                  :ast/nodes (vec rescue)}
                 {:ast/type  :ast/seq
                  :ast/nodes (vec finally)}]}))

(defn try-nodes
  "Accessor for standard nodes of a try statement"
  [node]
  (get (:ast/nodes node) 0))

(defn rescue-nodes
  "Accessor for rescue nodes of a try statement"
  [node]
  (get (:ast/nodes node) 1))

(defn finally-nodes
  "Accessor for finally nodes of a try statement"
  [node]
  (get (:ast/nodes node) 2))

(defn with-augment
  "Provide a source and dest for `bundes.job/augment`"
  [[source dest] node]
  (assoc node :ast/augment #:augment{:source source :dest dest}))
