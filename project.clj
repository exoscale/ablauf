(defproject exoscale/ablauf "0.3.6"
  :description "long-running workflow management"
  :url "https://github.com/exoscale/ablauf"
  :license {:name "ISC License"
            :url  "https://github.com/exoscale/ablauf/tree/master/LICENSE"}
  :dependencies [[org.clojure/clojure               "1.11.1"]
                 [spootnik/commons                  "0.3.2"]
                 [mysql/mysql-connector-java        "8.0.30"]
                 [com.github.seancorfield/next.jdbc "1.3.834"]
                 [org.clojure/tools.logging         "1.2.4"]
                 [manifold                          "0.2.4"]
                 [migratus                          "1.4.4"]]
  :deploy-repositories [["snapshots" :clojars]
                        ["releases"  :clojars]]
  :humane {:dependencies [[pjstadig/humane-test-output "0.11.0"]]
           :injections   [(require 'pjstadig.humane-test-output)
                          (pjstadig.humane-test-output/activate!)]}
  :profiles {:test {:plugins      [[lein-cljfmt                "0.9.0" :exclusions [org.clojure/clojure]]
                                   [lein-cloverage             "1.2.4"]
                                   [lein-test-report-junit-xml "0.2.0"]]
                    :dependencies [[org.clojure/test.check     "1.1.1"]
                                   [tortue/spy "2.13.0"]
                                   [hikari-cp/hikari-cp "3.0.1" :exclusions [org.slf4j/slf4j-api]]]
                    :pedantic?    :abort}
             :dev  {:pedantic?    :ignore
                    :dependencies [[ch.qos.logback/logback-core "1.4.4"]
                                   [ch.qos.logback/logback-classic "1.4.4"]]}}
  :global-vars {*warn-on-reflection* true}
  :aliases {"coverage" ["with-profile" "+test" "cloverage"]
            "difftest" ["with-profile" "+humane,+test" "test"]}
  :pedantic? :abort)
