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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;; Utilities ;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn in?
  "true if collection contains elm"
  [coll elm]
  (some #(= elm %) coll))

(def ranks
 { :yokozuna 1
   :ozeki 2
   :sekiwake 3
   :komusubi 4
   :maegashira 5 ; Maegashira #1, Maegashira #2, ...
   :juryo 6 })   ; Juryo #1, Juryo #2, ...

;; in progress...
;; (defn compare-rank
;;   "given two rank strings
;;    return the higher ranked of the 2
;;    and by how many steps it is higher.
;;    equal ranks return 'same' with :delta 0"
;;   [rank_a_str rank_b_str]
;;   (let [rank_a (keyword (clojure.string/lower-case rank_a_str))
;;         rank_b (keyword (clojure.string/lower-case rank_b_str))]
;;   (if (and (rank_a ranks) (rank_b ranks))

;;    nil))) ; invalid rank passed, nil returned

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

(defn valid-rikishi?
  "true if data exists for
   passed in rikishi string, false otherwise"
  [rikishi]
  (let [rikishi-names (map #(get % :name) (list-rikishi))]
   (if (in? rikishi-names (clojure.string/upper-case rikishi))
    true
    false)))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;; Rikishi's Head to Head Matchups ;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn rikishi-bout-history
  "given two rikishi name strings
   returns all bouts between the two"
  [rikishi_a rikishi_b]
  (if (and (valid-rikishi? rikishi_a) (valid-rikishi? rikishi_b))
   (jdbc/query mysql-db (sql/format
    (sql/build
      :select :*
      :from :bout
      :where
        [:or
          [:and [:= :east rikishi_a] [:= :west rikishi_b]]
          [:and [:= :east rikishi_b] [:= :west rikishi_a]]])))
   nil))

;;
;; -- waiting on compare rank function
;; -- add in a param to filter by rank delta.
;; -- so, i want to see how many times
;; -- this rikishi lost to 3 levels lower rank, etc
;; -- default to returning all losses to lower ranks
;;
;; (defn rikishi-loses-to-lower-rank
;;   "given a rikishi name string
;;    returns all bouts where he lost
;;    to a lower-ranked rikishi"
;;   [rikishi]
;;   (jdbc/query mysql-db (sql/format
;;    (sql/build
;;      :select :*
;;      :from :bout
;;      :where
;;      []
;;     ))))

;; (defn rikishi-win-against-higher-rank
;;   )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;; Rikishi's Rank functions ;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
   returns rank if found, else returns nil.
   given string 'TAKAKEISHO', returns string 'Ozeki'"
  [rikishi [bout & rest]] ; ["TAKAKEISHO" { ..bout } (list {...} {...} ...)]
    (if-let [rank (get-rank-in-bout rikishi bout)]
      rank
      (if (empty? rest)
        nil ; list is done, return nil
        (get-rank-in-tournament rikishi rest)))) ; keep going

(defn get-rikishi-current-rank
   "gets most recent rank for passed in rikishi name
    given string 'hakuho' this is returned--
    {:rank 'Yokozuna', :tournament {:month 7, :year 2020 }}
    returns nil if no data exists for passed in rikishi"
   ([rikishi]
    (if (valid-rikishi? rikishi)
     (get-rikishi-current-rank (clojure.string/upper-case rikishi) (list-bouts))
     nil))
   ([rikishi [tournament & rest]]
    (if-let [rank ; rank from checking all bouts in a tournament
             (get-rank-in-tournament
              rikishi
              (get-bouts-by-rikishi
               rikishi
               (:year tournament)
               (:month tournament)))]
     {:rank rank :tournament tournament} ; rank found, we're done
     (get-rikishi-current-rank rikishi rest)))) ; rank not found, move to next tournament

(defn get-rikishi-rank-over-time
  "returns rikishi rank over time, from as far back as data exists.
   everytime rikishi has new rank, that is included in the list"
  ([rikishi]
   (if (valid-rikishi? rikishi)
    (get-rikishi-rank-over-time (clojure.string/upper-case rikishi) (reverse (list-bouts)) [])
    nil))
  ([rikishi [tournament & rest] rank-over-time]
   (if (empty? tournament)
    rank-over-time ; checked every tournament, return what you've got
    (let [rank (get-rank-in-tournament
                rikishi
                (get-bouts-by-rikishi
                 rikishi
                 (:year tournament)
                 (:month tournament)))]
     (if (not= rank (last (map #(get % :rank) rank-over-time)))
      (get-rikishi-rank-over-time
       rikishi
       rest
       (conj rank-over-time {:rank rank :tournament tournament}))
      (get-rikishi-rank-over-time
       rikishi
       rest
       rank-over-time))))))

    