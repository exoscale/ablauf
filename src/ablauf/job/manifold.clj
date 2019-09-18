(ns ablauf.job.manifold
  "
  An execution engine for warp jobs on manifold.
  To perform parallel actions with manifold.

  Execution is represented as a stream on which results
  are produced.

  The stream is closed when execution is finished.
  Each execution step is store with the help of a
  `ablauf.job.store/JobStore` implementation that
  needs to be supplied.

  This namespace makes no assumption on how to actually
  perform side-effects, consumers of `runner` need to
  extend the `dispatch-action` multimethod to perform those.
  "
  (:require [ablauf.job           :as job]
            [ablauf.job.store     :as store]
            [ablauf.job.ast       :as ast]
            [manifold.deferred    :as d]
            [manifold.stream      :as s]
            [spootnik.transducers :refer [reductions-with]]))

(defn- timestamp
  "Standard wall clock implementation"
  []
  (System/currentTimeMillis))

(defn success!
  "Push a success value back on the restarter"
  [input clock dispatch result]
  (let [duration (- (clock) (:exec/timestamp dispatch))]
    (s/put! input [(-> dispatch
                       (assoc :exec/result :result/success
                              :exec/output result
                              :exec/duration duration)
                       (dissoc :exec/context))])))

(defn fail!
  "Push a failure value back on the restarter"
  [input clock dispatch result]
  (let [duration (- (clock) (:exec/timestamp dispatch))]
    (s/put! input [(-> dispatch
                       (assoc :exec/result :result/failure
                              :exec/output (or result :error/error)
                              :exec/duration duration)
                       (dissoc :exec/context))])))

(defmulti dispatch-action
  "
  Dumb action handler, should live in its own namespace
  and be provided to the runner instead.

  Methods should be installed by callers of `runner` since
  this namespace does not know what side effects may be performed.

  All methods are expected to yield manifold deferred.
  "
  :ast/action)

(defmethod dispatch-action :action/log
  [{:ast/keys [payload]}]
  (d/future payload))

(defmethod dispatch-action :action/fail
  [_]
  (d/error-deferred :error/error))

(defn redispatcher
  "Once dispatchs have been determined by `job/restart`, dispatch
   actions with callbacks into the restarter."
  [input store id result]
  (fn [[job context dispatchs]]
    (let [clock (or (get-in context [:exec/runtime :runtime/clock]) timestamp)]
      ;; Persist to given store
      (store/persist store id (dissoc context :exec/runtime) job)

      ;; Launch all dispatchs found
      (doseq [d    dispatchs
              :let [dispatch (assoc d
                                    :exec/context context
                                    :exec/timestamp (clock))]]
        (d/on-realized (dispatch-action dispatch)
                       (partial success! input clock dispatch)
                       (partial fail! input clock dispatch))))

    ;; Close input if processing is finished
    (when (job/done? job)
      (s/close! input)
      (if (job/failed? job)
        (d/error! result [job context])
        (d/success! result [job context])))))

(defn restart-transducer
  "A transducer which yields all intermediate
   reduced values of `job/restart`"
  [job]
  (reductions-with job/restart job))

(defn runner
  "Create a stream which listens for input results and figures
   out next dispatchs to send"
  ([store ast]
   (runner store ast {:id (java.util.UUID/randomUUID)}))
  ([store ast {:keys [id context buffer runtime] :or {buffer 10}}]
   (let [context (assoc context :exec/runtime runtime)
         job     (job/make-with-context ast context)
         input   (s/stream buffer (restart-transducer job))
         result  (d/deferred)]
     (s/consume (redispatcher input store id result) input)

     ;; Put an initial empty result payload in the stream
     ;; to guarantee initial dispatchs are sent
     (s/put! input [])
     result)))
