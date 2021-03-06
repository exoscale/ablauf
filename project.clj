(defproject exoscale/ablauf "0.1.14-SNAPSHOT"
  :description "long-running workflow management"
  :url "https://github.com/exoscale/ablauf"
  :license {:name "ISC License"
            :url  "https://github.com/exoscale/ablauf/tree/master/LICENSE"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [spootnik/commons    "0.3.0"]
                 [manifold            "0.1.9-alpha4"]]
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
