(defproject exoscale/ablauf "0.1.8-SNAPSHOT"
  :description "long-running workflow management"
  :url "https://github.com/exoscale/ablauf"
  :license {:name "ISC License"
            :url  "https://github.com/exoscale/ablauf/tree/master/LICENSE"}
  :codox {:source-uri "https://github.com/exoscale/ablauf/blob/{version}/{filepath}#L{line}"
          :doc-files  ["README.md"]
          :metadata   {:doc/format :markdown}}
  :plugins [[lein-codox "0.10.7"]]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [spootnik/commons    "0.3.0"]
                 [manifold            "0.1.8"]]
  :deploy-repositories [["snapshots" :clojars]
                        ["releases"  :clojars]]
  :aliases {"kaocha" ["with-profile" "+dev" "run" "-m" "kaocha.runner"]}
  :profiles {:dev {:dependencies [[lambdaisland/kaocha "0.0-529"]]
                   :pedantic? :ignore}}
  :pedantic? :abort)
