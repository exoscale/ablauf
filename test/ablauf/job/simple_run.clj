(ns ablauf.job.simple-run
  (:require [manifold.stream     :as stream]
            [manifold.deferred   :as d]
            [ablauf.job          :as job]
            [ablauf.job.ast      :as ast]
            [ablauf.job.store    :as store]
            [ablauf.job.manifold :refer [runner]]
            [clojure.test        :refer :all]))

(defn stream-store
  [s]
  (reify store/JobStore
    (persist [this uuid context state]
      (stream/put! s [state context])
      (when (job/done? state)
        (stream/close! s)))))

(defn run-ast
  [ast]
  (let [s         (stream/stream 10)
        action-fn (fn [{:ast/keys [action payload]}]
                    (case action
                      :action/fail (d/error-deferred :error/error)
                      :action/log  (d/success-deferred payload)))]
    (runner (stream-store s)
            ast
            {:runtime {:runtime/clock (constantly 0)}
             :action-fn action-fn})
    (vec (stream/stream->seq s))))

(def log-output
  [[[{:ast/type :ast/leaf,
       :ast/action :action/log,
       :ast/payload :a,
       :ast/id 0,
      :exec/result :result/pending}
     nil]
    {}]
   [[{:ast/type :ast/leaf,
       :ast/action :action/log,
       :ast/payload :a,
       :ast/id 0,
       :exec/result :result/success,
      :exec/output :a
      :exec/timestamp 0
      :exec/duration 0}
     nil]
    {}]])

(def fail-output
  [[[{:ast/type :ast/leaf,
       :ast/action :action/fail,
       :ast/id 0,
       :exec/result :result/pending}
     nil]
    {}]
   [[{:ast/type :ast/leaf,
       :ast/action :action/fail,
       :ast/id 0,
       :exec/result :result/failure,
      :exec/output :error/error
      :exec/timestamp 0
      :exec/duration 0      }
     nil]
    {}]])


(def simple-do-output

    [[[#:ast{:type :ast/seq,
         :nodes
         [{:ast/type :ast/leaf,
           :ast/action :action/log,
           :ast/payload :a,
           :ast/id 1,
           :exec/result :result/pending}
          #:ast{:type :ast/leaf,
                :action :action/log,
                :payload :b,
                :id 2}],
         :id 0}
   nil]
  {}]
 [[#:ast{:type :ast/seq,
         :nodes
         [{:ast/type :ast/leaf,
           :ast/action :action/log,
           :ast/payload :a,
           :ast/id 1,
           :exec/result :result/success,
           :exec/timestamp 0,
           :exec/duration 0,
           :exec/output :a}
          {:ast/type :ast/leaf,
           :ast/action :action/log,
           :ast/payload :b,
           :ast/id 2,
           :exec/result :result/pending}],
         :id 0}
   nil]
  {}]
 [[#:ast{:type :ast/seq,
         :nodes
         [{:ast/type :ast/leaf,
           :ast/action :action/log,
           :ast/payload :a,
           :ast/id 1,
           :exec/result :result/success,
           :exec/timestamp 0,
           :exec/duration 0,
           :exec/output :a}
          {:ast/type :ast/leaf,
           :ast/action :action/log,
           :ast/payload :b,
           :ast/id 2,
           :exec/result :result/success,
           :exec/timestamp 0,
           :exec/duration 0,
           :exec/output :b}],
         :id 0}
   nil]
  {}]])

(def try-rescue-output
  [[#:ast{:type :ast/try,
          :nodes
          [#:ast{:type :ast/seq,
                 :nodes
                 [{:ast/type :ast/leaf,
                   :ast/action :action/fail,
                   :ast/id 2,
                   :exec/result :result/failure,
                   :exec/output :error/error}],
                 :id 1}
           #:ast{:type :ast/seq,
                 :nodes
                 [{:ast/type :ast/leaf,
                   :ast/action :action/log,
                   :ast/payload :rescued,
                   :ast/id 4,
                   :exec/result :result/success
                   :exec/output :rescued}],
                 :id 3}
           #:ast{:type :ast/seq, :nodes [], :id 5}],
          :id 0}]
   {}])

(deftest small-asts

  (testing "logging"
    (let [output (run-ast (ast/log!! :a))
          [res]  (last output)]

      (is (job/done? res))
      (is (not (job/pending? res)))
      (is (not (job/failed? res)))
      (is (= log-output output))))


  (testing "failure"
    (let [output (run-ast (ast/fail!!))
          [res]  (last output)]

      (is (job/done? res))
      (is (not (job/pending? res)))
      (is (job/failed? res))
      (is (= fail-output output))))

  (testing "do with two logging statements"
    (let [output (run-ast (ast/do!! (ast/log!! :a) (ast/log!! :b)))
          [res]  (last output)]
      (is (job/done? res))
      (is (not (job/pending? res)))
      (is (not (job/failed? res)))
      (is (= output simple-do-output))))

  (testing "try rescue yields success"
    (let [output (run-ast (ast/try!!
                           (ast/fail!!)
                           (rescue!! (ast/log!! :rescued))))
          [res]  (last output)]

      (is (job/done? res))
      (is (not (job/failed? res)))
      (is (not (job/pending? res))))))

(deftest context-tests

  (let [output
        (run-ast
         (ast/do!!
          (ast/with-augment [identity :first] (ast/log!! 1)) ;; standard logging form
          (ast/with-augment [identity :second] (ast/log!! 1)) ;; will be overwritten later
          (ast/with-augment [identity [:nested :one]] (ast/log!! 3)) ;; nested key dest
          (ast/with-augment [[:nested :key] [:nested :two]] ;;  nested key source
            (ast/log!! {:nested {:key 4}}))
          (ast/with-augment [:output [:nested :three]] ;; keyword source
            (ast/log!! {:output 5}))
          (ast/with-augment [:output :second] (ast/log!! {:output 2})) ;; overwriting
          ))

        [_ context] (last output)]
    (is (= {:first 1
            :second 2
            :nested {:one 3
                     :two 4
                     :three 5}}
           context))))


(deftest runtime-test

  (let [runtime   {:foo 123}
        p         (promise)
        action-fn (fn [{{:exec/keys [runtime]} :exec/context :as payload}]
                    (deliver p runtime)
                    (d/future {:exec :success}))]

    (runner (stream-store (stream/stream 5))
            (ast/action!! :runtime/test {})
            {:runtime   runtime
             :action-fn action-fn})

    (is (= runtime @p))))

(deftest expected-failure-test

  (let [counter   (atom 0)
        action-fn (fn [{:ast/keys [action payload]}]
                    (case action
                      :action/fail (d/error-deferred :error/error)
                      :action/log  (d/success-deferred payload)
                      ::inc        (do (swap! counter inc)
                                       (d/success-deferred {:exec :success}))))]

    (let [res
          (runner (stream-store
                   (stream/stream 10))
                  (ast/do!!
                   (ast/fail!!)
                   (ast/action!! ::inc {})
                   (ast/action!! ::inc {})
                   (ast/action!! ::inc {}))
                  {:action-fn action-fn})]

      (is (zero? @counter))
      (reset! counter 0))

    (let [res
          (runner (stream-store
                   (stream/stream 10))
                  (ast/dopar!!
                   (ast/fail!!)
                   (ast/action!! ::inc {})
                   (ast/action!! ::inc {})
                   (ast/action!! ::inc {}))
                  {:action-fn action-fn})]

      (is (= @counter 3))
      (reset! counter 0))


    ))
