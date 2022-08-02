(ns sumo-backend.data.rikishi
  (:require [honeysql.core :as sql]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [sumo-backend.utils :as utils]
            [sumo-backend.data.database :as db]))


;;
;; Namespace that provides Rikishi information
;;

(defn get-rikishi
  "gets rikihsi record specified by passed in name"
  [name]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select :*
          :from :rikishi
          :where [:= :name name])))
    (println "No Mysql DB")))


(comment
  (println (get-rikishi "endo")))


(defn list-rikishi
  "list all rikishi records"
  []
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select :*
          :from :rikishi)))
    (println "No Mysql DB")))


(comment
  (println (list-rikishi)))


(defn rikishi-exists?
  "true if data exists for
   passed in rikishi string, false otherwise"
  [rikishi]
  (let [rikishi-names (map #(get % :name) (list-rikishi))]
    (if (utils/in?
          rikishi-names
          (str/upper-case rikishi))
      true
      false)))


(comment
  (println (rikishi-exists? "Endo")))
