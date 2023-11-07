(ns ablauf.job.macros
  (:require [clojure.walk :as walk]))

;; copied from https://github.com/aphyr/dom-top/blob/master/src/dom_top/core.clj
(defrecord Retry [bindings])

(defmacro with-retry
  "It's really inconvenient not being able to recur from within (catch)
  expressions. This macro wraps its body in a (loop [bindings] (try ...)).
  Provides a (retry & new bindings) form which is usable within (catch) blocks:
  when this form is returned by the body, the body will be retried with the new
  bindings. For instance,

      (with-retry [attempts 5]
        (network-request...)
        (catch RequestFailed e
          (if (< 1 attempts)
            (retry (dec attempts))
            (throw e))))"
  [initial-bindings & body]
  (assert (vector? initial-bindings))
  (assert (even? (count initial-bindings)))
  (let [bindings-count (/ (count initial-bindings) 2)
        body (walk/prewalk (fn [form]
                             (if (and (seq? form)
                                      (= 'retry (first form)))
                               (do (assert (= bindings-count
                                              (count (rest form))))
                                   `(Retry. [~@(rest form)]))
                               form))
                           body)
        retval (gensym 'retval)]
    `(loop [~@initial-bindings]
       (let [~retval (try ~@body)]
         (if (instance? Retry ~retval)
           (recur ~@(map (fn [i] (let [retval (vary-meta retval
                                                         assoc :tag `Retry)]
                                   `(nth (.bindings ~retval) ~i)))
                         (range bindings-count)))
           ~retval)))))
