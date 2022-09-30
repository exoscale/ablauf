(defproject exoscale/ablauf "0.1.14-SNAPSHOT"
  :description "long-running workflow management"
  :url "https://github.com/exoscale/ablauf"
  :license {:name "ISC License"
            :url  "https://github.com/exoscale/ablauf/tree/master/LICENSE"}
  :dependencies [[org.clojure/clojure               "1.11.1"]
                 [spootnik/commons                  "0.3.0"]
                 [mysql/mysql-connector-java        "8.0.30"]
                 [com.github.seancorfield/next.jdbc "1.3.834"]
                 [org.clojure/tools.logging         "1.2.1"]
                 [manifold                          "0.2.4"]]
  :deploy-repositories [["snapshots" :clojars]
                        ["releases"  :clojars]]
  :profiles {:test {:plugins   [[lein-difftest "2.0.0"]
                                [lein-cljfmt   "0.6.7"]
                                [lein-cloverage "1.1.2"]]
                    :dependencies [[org.clojure/test.check "1.1.0"]]
                    :pedantic? :abort}
             :dev  {:pedantic? :ignore}}
  :aliases {"coverage" ["with-profile" "+test" "cloverage"]}
  :pedantic? :abort)
