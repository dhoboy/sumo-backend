(ns sumo-backend.functions)
(require '[clojure.java.jdbc :as j])

(def mysql-db {:dbtype "mysql"
               :dbname "sumo"
               :user "fill-me-in"
               :password "fill-me-in"})

(defn get-rikishi
  "gets rikihsi record specified by passed in name"
  [name]
  (j/query mysql-db ["SELECT * FROM rikishi WHERE name = ?" name]))

(defn list-rikishi
  "list all rikishi records"
  []
  (j/query mysql-db ["SELECT * FROM rikishi"]))

(defn list-bouts
  "list all bouts data exists for"
  []
  (j/query mysql-db ["SELECT DISTINCT month,year from bout"]))

(defn get-bouts-by-date
  "gets all bouts in specified time frame: year, month, day"
  ([year]
    (j/query mysql-db ["SELECT * FROM bout WHERE year = ?" year]))
  ([year month]
    (j/query mysql-db ["SELECT * FROM bout WHERE year = ? AND month = ?" year month]))
  ([year month day]
    (j/query mysql-db ["SELECT * FROM bout WHERE year = ? AND month = ? AND day = ?" year month day])))

(defn get-bouts-by-rikishi
  "gets all bouts by rikishi and optional year, month, date"
  ([name] 
    (j/query mysql-db ["SELECT * FROM bout WHERE (east = ?) OR (west = ?)" name name]))
  ([name year] 
    (j/query mysql-db ["SELECT * FROM bout WHERE (east = ?) OR (west = ?) AND year = ?" name name year]))
  ([name year month] 
    (j/query mysql-db ["SELECT * FROM bout WHERE (east = ?) OR (west = ?) AND year = ? AND month = ?" name name year month]))
  ([name year month day] 
    (j/query mysql-db ["SELECT * FROM bout WHERE (east = ?) OR (west = ?) AND year = ? AND month = ? AND day = ?" name name year month day])))

  