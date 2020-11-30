(ns ablauf.job.sync
  "
  An execution engine for synchronous execution of ablauf jobs.

  This namespace makes no assumption on how to actually
  perform side-effects, an action runner fn must be provided.
  "
  (:require [ablauf.job :as job]))

(defn run
  "A basic AST runner which goes through all steps, useful to validate
   AST execution without having to go through the manifold runner"
  ([ast action-fn]
   (run ast nil action-fn))
  ([ast context action-fn]
   (loop [[job context dispatchs]
          (job/restart (job/make-with-context ast context) [])]
     (if (job/done? job)
       [job context]
       (recur (job/restart [job context]
                           (pmap #(action-fn (assoc % :exec/context context))
                                 dispatchs)))))))
