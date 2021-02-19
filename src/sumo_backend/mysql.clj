(ns sumo-backend.mysql)
(require '[clojure.java.jdbc :as jdbc])
(require '[honeysql.core :as sql]
         '[honeysql.helpers :refer :all :as helpers])

(require '[cheshire.core :refer :all]) ; parses json
;(require '[jdbc.pool.c3p0 :as pool])


;; reading in keys from an env variable file for now
;; Or cached:
;; (def mysql-db
;;   (memoize
;;     (fn [] ;; To support multiple DBs, pass in DB name as an arg
;;       (json/parse
;;         (slurp ...)
;;         true)))))
(def mysql-db 
  (:local 
    #_:clj-kondo/ignore
    (parse-string
      (slurp "./keys/mysql.json")
      true)))

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

(defn get-rikishi
  "gets rikihsi record specified by passed in name"
  [name]
  (jdbc/query mysql-db (sql/format
    (sql/build
      :select :*
      :from :rikishi
      :where [:= :name name]))))
  
(defn list-rikishi
  "list all rikishi records"
  []
  (jdbc/query mysql-db (sql/format 
    (sql/build 
      :select :* 
      :from :rikishi))))

(defn list-bouts
  "list all bouts data exists for"
  []
  (jdbc/query mysql-db (sql/format
    (sql/build
      :select [:month :year]
      :modifiers [:distinct]
      :from :bout
      :order-by [[:year :desc] [:month :desc]]))))

(defn get-bouts-by-date
  "gets all bouts in specified time frame: year, month, day"
  ([year]
    (jdbc/query mysql-db (sql/format 
      (sql/build
        :select :*
        :from :bout
        :where [:= :year year]))))
  ([year month]
    (jdbc/query mysql-db (sql/format
      (sql/build 
        :select :* 
        :from :bout
        :where 
          [:and
            [:= :year year]
            [:= :month month]]))))
  ([year month day]
    (jdbc/query mysql-db (sql/format
      (sql/build
        :select :*
        :from :bout
        :where 
          [:and
            [:= :year year]
            [:= :month month] 
            [:= :day day]])))))

(defn get-bouts-by-rikishi
  "gets all bouts by rikishi and optional year, month, date"
  ([name] ; how can i do somehting like [clojure.string/upper-case name] here to force the param to come in upper-cased?
    (jdbc/query mysql-db (sql/format
      (sql/build
        :select :*
        :from :bout
        :where [:or [:= :east name] [:= :west name]]))))
  ([name year] 
    (jdbc/query mysql-db (sql/format 
      (sql/build
        :select :* 
        :from :bout
        :where 
          [:and 
            [:or [:= :east name] [:= :west name]] 
            [:= :year year]]))))
  ([name year month] 
    (jdbc/query mysql-db (sql/format 
      (sql/build
        :select :*
        :from :bout
        :where 
          [:and
            [:or [:= :east name] [:= :west name]]
            [:= :year year] 
            [:= :month month]]))))
  ([name year month day] 
    (jdbc/query mysql-db (sql/format
      (sql/build
        :select :*
        :from :bout
        :where
          [:and
            [:or [:= :east name] [:= :west name]]
            [:= :year year] 
            [:= :month month]
            [:= :day day]])))))

;;;;;;;;;; Calculating Rikishi's Rank ;;;;;;;;;;;;

(defn get-rank-in-bout
  "for a given bout hashmap and rikishi name string, 
   returns rikishi rank if it exists
   else returns nil"
  [rikishi bout]
  (or
    (and (= (:east bout) (clojure.string/upper-case rikishi)) (:east_rank bout)) 
    (and (= (:west bout) (clojure.string/upper-case rikishi)) (:west_rank bout)) 
    nil))

(defn get-rank-in-tournament
  "iterates over a list of tournaments,
   returns rank if found, else returns nil"
  [rikishi [bout & rest]] ; ["TAKAKEISHO" { ..bout } (list {...} {...} ...)]
    (if-let [rank (get-rank-in-bout rikishi bout)]
      rank
      (if (empty? rest)
        nil ; list is done, return nil
        (get-rank-in-tournament rikishi rest)))) ; keep going
         
(defn get-rikishi-rank
   "gets current rank of passed in rikishi"
   ([rikishi]
     (get-rikishi-rank (clojure.string/upper-case rikishi) (list-bouts)))
   ([rikishi [tournament & rest]]
     (if-let [rank ; rank from checking all bouts in a tournament
               (get-rank-in-tournament
                 rikishi
                 (get-bouts-by-rikishi
                   rikishi
                   (:year tournament)
                   (:month tournament)))]
        {:rank rank :tournament tournament} ; rank found, we're done
        (get-rikishi-rank rikishi rest)))) ; rank not found, move to next tournament

   
