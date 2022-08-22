(ns sumo-backend.data.database
  (:require [clojure.java.jdbc :as jdbc]
            [cheshire.core :refer [parse-string]] ; parses json
            [jdbc.pool.c3p0 :as c3p0]
            [sumo-backend.utils :refer [when-let-all]]))


;;
;; Namespace that connects to and manages MySql database
;;

(def key-file "./keys/mysql.json")


;; Rather than opening a new connection for each query, it's more efficient
;; and easier on the DB to maintan one or more connections, scaling
;; them as needed.
(def db-atom (atom nil))


;; @bslawski when this key file was in a def above, like
;; (def mysql-keys (:local (parse-string (slurp key-file) true)))
;; the db here would seem to evaluate before the file was loaded?
;; i was getting Access denied for user ''@'localhost' to database 'sumo'
;; errors the whole time
(defn db-conn
  []
  (or
    @db-atom
    #_{:clj-kondo/ignore [:unresolved-symbol]} ; clj-kondo doesn't like the when-let-all
    (when-let-all [db-keys (:local (parse-string (slurp key-file) true))
                   conn (c3p0/make-datasource-spec
                          {:classname "com.mysql.cj.jdbc.Driver" ; "com.mysql.jdbc.Driver"
                           :subprotocol "mysql"
                           :initial-pool-size 3
                           :subname "//127.0.0.1:3306/sumo?characterEncoding=utf8"
                           :user (:user db-keys)
                           :password (:password db-keys)})]
      (reset! db-atom conn)
      conn)))


;; On Connection Pooling-- Going with c3p0 for now. Further, Ben says:
;; You could also make an atom or ref that holds active DB connections
;; and threads running clojure.core.async/go-loops using those connections
;; to execute SQL queries they read from a clojure.core.async/chan, with
;; some other thread periodically checking the status of the query chan
;; and scaling connections/threads accordingly.

;; I think the query function can accept either a connection or config for a connection
;; If it gets config, it opens and closes its own connection
;; Yeah, looks like common parent class is javax.sql.DataSource
;; c3p0 makes a ComboPooledDataSource that implements DataSource
;; I think jdbc is creating some other DataSource
;; DataSource has a getConnection method that returns a Connection object with a live connection
;; For ComboPooledDataSource, getConnection grabs from a pool of already open connections
;; If you're implementing your own thread pool, you'd implement some subclass of DataSource
;; that has getConnection returning a Connection from that pool
;; Usually I'd use c3p0, but making your own would be good async / atom practice
;; Definitely you'll need the interop
;; Looks like extending DataSource is the way to go
;; Then make some atom/ref with a bunch of Connection objects, and have the DataSource subclass
;; implement getConnection to pull from that atom/ref.
;; https://stackoverflow.com/questions/40709151/subclasses-in-clojure

;;
;; rikishi Table Definition
;;

;; CREATE TABLE `rikishi` (
;;   `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
;;   `name` varchar(255) DEFAULT NULL,
;;   `image` varchar(255) DEFAULT NULL,
;;   `name_ja` varchar(255) DEFAULT NULL,
;;   PRIMARY KEY (`id`)
;; ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

(def rikishi-table
  (jdbc/create-table-ddl
    :rikishi
    [[:id "int(11)" :unsigned :not :null :auto_increment :primary :key]
     [:name "varchar(255)" :default :null]
     [:image "varchar(255)" :default :null]
     [:name_ja "varchar(255)" :default :null]]
    {:table-spec "ENGINE=InnoDB DEFAULT CHARSET=utf8"
     :conditional? true}))


;;
;; bout Table Definition
;;

;; CREATE TABLE `bout` (
;;   `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
;;   `east` varchar(255) DEFAULT NULL,
;;   `west` varchar(255) DEFAULT NULL,
;;   `east_rank` varchar(255) DEFAULT NULL,
;;   `east_rank_value` int(11) DEFAULT NULL,
;;   `west_rank` varchar(255) DEFAULT NULL,
;;   `west_rank_value` int(11) DEFAULT NULL,
;;   `winner` varchar(255) DEFAULT NULL,
;;   `loser` varchar(255) DEFAULT NULL,
;;   `is_playoff` tinyint(1) DEFAULT NULL,
;;   `technique` varchar(255) DEFAULT NULL,
;;   `technique_en` varchar(255) DEFAULT NULL,
;;   `technique_category` varchar(255) DEFAULT NULL,
;;   `year` int(11) DEFAULT NULL,
;;   `month` int(11) DEFAULT NULL,
;;   `day` int(11) DEFAULT NULL,
;;   PRIMARY KEY (`id`)
;; ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

(def bout-table
  (jdbc/create-table-ddl
    :bout
    [[:id "int(11)" :unsigned :not :null :auto_increment :primary :key]
     [:east "varchar(255)" :default :null]
     [:west "varchar(255)" :default :null]
     [:east_rank "varchar(255)" :default :null]
     [:east_rank_value "int(11)" :default :null]
     [:west_rank "varchar(255)" :default :null]
     [:west_rank_value "int(11)" :default :null]
     [:winner "varchar(255)" :default :null]
     [:loser "varchar(255)" :default :null]
     [:is_playoff "tinyint(1)" :default :null]
     [:technique "varchar(255)" :default :null]
     [:technique_en "varchar(255)" :default :null]
     [:technique_category "varchar(255)" :default :null]
     [:year "int(11)" :default :null]
     [:month "int(11)" :default :null]
     [:day "int(11)" :default :null]]
    {:table-spec "ENGINE=InnoDB DEFAULT CHARSET=utf8"
     :conditional? true}))


;;
;; Create Tables
;;

;; @bslawski why does the connetion need to be bound in a let
;; inside the fn, not just made in a def one time (def conn (db))?
(defn create-tables
  "Creates the rikishi and bout tables for
   this project if they don't already exist"
  []
  (if-let [conn (db-conn)]
    (do
      (jdbc/db-do-commands
        conn
        rikishi-table)
      (jdbc/db-do-commands
        conn
        bout-table)
      (println "Rikishi and Bout tables created!"))
    (println "No Mysql DB")))


;;
;; Drop Tables
;;

(defn drop-tables
  "Drops the rikishi and bout tables for
   this project if they exist"
  []
  (if-let [conn (db-conn)]
    (do
      (jdbc/db-do-commands
        conn
        (jdbc/drop-table-ddl
          :rikishi
          {:conditional? true}))
      (jdbc/db-do-commands
        conn
        (jdbc/drop-table-ddl
          :bout
          {:conditional? true}))
      (println "Rikishi and Bout tables dropped!"))
    (println "No Mysql DB")))
