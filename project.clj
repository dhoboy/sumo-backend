(defproject sumo-backend "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :min-lein-version "2.0.0"
  :main sumo-backend.mysql
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [compojure "1.6.1"]
                 [cheshire "5.10.0"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [clojure.jdbc/clojure.jdbc-c3p0 "0.3.3"]
                 [mysql/mysql-connector-java "5.1.6"]
                 [honeysql "1.0.444"]
                 [ring/ring-defaults "0.3.2"]
                 [ring/ring-json "0.5.0"]
                 [jumblerg/ring-cors "2.0.0"]]
  :plugins [[lein-ring "0.12.5"]]
  :ring {:handler sumo-backend.handler/app}
  :profiles
  {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                        [ring/ring-mock "0.3.2"]]}})
