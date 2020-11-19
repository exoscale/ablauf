(ns ablauf.job.node
  "Operations on job nodes (statements) outside zippers"
  (:require [ablauf.job.ast :as ast]))

(defmulti failed?
  "Predicate to test for failure of a (sub)job"
  :ast/type :hierarchy #'ast/hierarchy)

(defmulti eligible?
  "Predicate to test for dispatch eligibility of a (sub)node"
  :ast/type :hierarchy #'ast/hierarchy)

(defmulti done?
  "Predicate to test for completion of a (sub)job"
  :ast/type :hierarchy #'ast/hierarchy)

(defmulti pending?
  "Predicate to test for pending state of a (sub)node"
  :ast/type :hierarchy #'ast/hierarchy)

(defmulti find-dispatchs
  "Returns next dispatchable actions for a (sub)node"
  :ast/type :hierarchy #'ast/hierarchy)

(def done-or-pending?
  "Predicate to test for completion or pending state of a (sub)node"
  (some-fn done? pending?))

(def done-or-failed?
  "Predicate to test for completion or error state of a (sub)node"
  (some-fn done? failed?))

(def pending-or-eligible?
  "Predicate to test for pending or future execution of a (sub)node"
  (some-fn eligible? pending?))

(defmethod failed? :ast/leaf
  [node]
  (contains? #{:result/failure :result/timeout} (:exec/result node)))

(defmethod failed? :ast/branch
  [node]
  (some failed? (:ast/nodes node)))

(defmethod failed? :ast/try
  [node]
  (or
   (and (failed? (ast/try-nodes node))
        (failed? (ast/rescue-nodes node))
        (done? (ast/finally-nodes node)))
   (failed? (ast/finally-nodes node))))

(defmethod done? :ast/leaf
  [node]
  (contains? #{:result/success :result/failure :result/timeout}
             (:exec/result node)))

(defmethod done? :ast/seq
  [node]
  (or (failed? node)
      (every? done? (:ast/nodes node))))

(defmethod done? :ast/par
  [node]
  (every? done? (:ast/nodes node)))

(defmethod done? :ast/try
  [node]
  (let [tnodes (ast/try-nodes node)
        rnodes (ast/rescue-nodes node)
        fnodes (ast/finally-nodes node)]
    (not
     (or (pending-or-eligible? tnodes)
         (pending-or-eligible? fnodes)
         (and (failed? tnodes)
              (pending-or-eligible? rnodes))))))

(defmethod pending? :ast/leaf
  [node]
  (= :result/pending (:exec/result node)))

(defmethod pending? :ast/branch
  [node]
  (some pending? (:ast/nodes node)))

(defmethod pending? :ast/try
  [node]
  (let [tnodes (ast/try-nodes node)
        rnodes (ast/rescue-nodes node)
        fnodes (ast/finally-nodes node)]
    (or (pending? tnodes)
        (and (failed? tnodes)
             (pending? rnodes))
        (pending? fnodes))))

(defmethod eligible? :ast/leaf
  [node]
  (nil? (:exec/result node)))

(defmethod eligible? :ast/par
  [node]
  (some eligible? (:ast/nodes node)))

(defmethod eligible? :ast/seq
  [node]
  (some eligible? (remove done-or-pending? (:ast/nodes node))))

(defmethod eligible? :ast/try
  [node]
  (let [tnodes (ast/try-nodes node)
        rnodes (ast/rescue-nodes node)
        fnodes (ast/finally-nodes node)]
    (cond
      (failed? tnodes)
      (or (eligible? rnodes) (eligible? fnodes))

      (done? tnodes)
      (eligible? fnodes)

      :else
      (eligible? tnodes))))

(defmethod find-dispatchs :ast/leaf
  [node]
  (when-not (done-or-pending? node)
    [(assoc node :exec/result :result/pending)]))

(defmethod find-dispatchs :ast/par
  [node]
  (vec
   (mapcat find-dispatchs (:ast/nodes node))))

(defmethod find-dispatchs :ast/seq
  [{:ast/keys [nodes] :as node}]
  (when-not (failed? node)
    (when-some [remaining (first (drop-while done? nodes))]
      (find-dispatchs remaining))))

(defmethod find-dispatchs :ast/try
  [node]
  (let [tnodes (ast/try-nodes node)
        rnodes (ast/rescue-nodes node)
        fnodes (ast/finally-nodes node)]
    (cond
      (and (failed? tnodes)
           (eligible? rnodes))
      (find-dispatchs rnodes)

      (and (failed? tnodes)
           (done-or-failed? rnodes))
      (find-dispatchs fnodes)

      (and (not (eligible? tnodes))
           (eligible? fnodes))
      (find-dispatchs fnodes)

      (eligible? tnodes)
      (find-dispatchs tnodes))))
