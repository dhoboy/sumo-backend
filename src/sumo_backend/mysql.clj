;; TODO make this sumo-backend.mysql or sumo-backend.db
;; The namespace would handle all interactions with the DB,
;; and abstracts the underlying table structure.
(ns sumo-backend.mysql)
(require '[clojure.java.jdbc :as j])
(require '[cheshire.core :refer :all]) ; parses json


;; TODO read from config file / env vars
;; This could be read on build:
;; (def mysql-db
;;   (json/parse
;;     (slurp ...)
;;     true)) ;; this specifies that keys should be keywords, not strings
;;
;; Or cached:
;; (def mysql-db
;;   (memoize
;;     (fn [] ;; To support multiple DBs, pass in DB name as an arg
;;       (json/parse
;;         (slurp ...)
;;         true)))))
(def mysql-db 
  (:local 
    (parse-string
      (slurp "./keys/mysql.json")
      true)))

;; TODO use honeysql or similar to build SQL queries
;; Manually defining queries can get hard to debug
;; and protect from SQL injection, etc

;; TODO manage a pool of connections to the DB
;; Rather than opening a new connection for each query, it's more efficient
;; and easier on the DB to maintan one or more connections, scaling
;; them as needed. Depending on how much time you want to spend on this
;; part, you could either import a library or build it yourself.
;; There are libraries like c3p0 that do this, which are nice until they
;; fail, then can be a pain to debug.
;; You could also make an atom or ref that holds active DB connections
;; and threads running clojure.core.async/go-loops using those connections
;; to execute SQL queries they read from a clojure.core.async/chan, with
;; some other thread periodically checking the status of the query chan
;; and scaling connections/threads accordingly.

;; TODO shorter lines
;; Typically, line lengths are capped at 80 chars, and I've also used 100 chars.
;; Every team has their own conventions, but most are based on https://guide.clojure.style/

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

  
