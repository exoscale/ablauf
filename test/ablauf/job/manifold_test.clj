(ns ablauf.job.manifold-test
  (:require [manifold.deferred :as d]
            [ablauf.job.ast :as ast]
            [ablauf.job.store :as store]
            [ablauf.job.manifold :refer [runner]]
            [clojure.test :refer :all]))

(defn- mock-store [{:keys [fail?] :as params}]
  (reify store/JobStore
    (persist [this uuid context state]
      (if fail?
        (d/error-deferred (ex-info "Forced fail" params))
        (d/success-deferred :ok)))))

(deftest persist-impacts-execution
  (let [action-fn (fn [{:ast/keys [action payload]}]
                    (case action
                      :action/fail (d/error-deferred :error/error)
                      ::inc (d/success-deferred (inc payload))))
        ast       (ast/action!! ::inc 1)]

    (testing "Persist should work normally"
      (let [res (runner (mock-store {:fail? false}) ast {:action-fn action-fn})
            [[{result :exec/output}] _] @res]
        (is (= 2 result))))

    (testing "When persist returns a failed deferred execution is halted"
      (is (thrown? Exception
            @(runner (mock-store {:fail? true}) ast {:action-fn action-fn}))))

    (testing "on-persist callback gets called with deferred"
      (let [persisted (d/deferred)
            _         @(runner (mock-store {:fail? false}) ast {:action-fn action-fn :persist-deferred persisted})]
        @(-> (d/chain persisted (fn [r] (is (= :ok r))))
             (d/catch (fn [_] (is (nil? "should never reach this point")))))))

    (testing "on-persist callback gets called even with error"
      (let [persisted (d/deferred)
            _         (runner (mock-store {:fail? true}) ast {:action-fn action-fn :persist-deferred persisted})]
        @(-> (d/chain persisted (fn [_] (assert nil? "should never reach this point")))
             (d/catch (fn [err] (is (= "Forced fail" (ex-message err))))))))))


(comment
  (run-tests *ns*))