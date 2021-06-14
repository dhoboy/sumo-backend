(ns sumo-backend.mysql)
(require '[clojure.java.jdbc :as jdbc])
(require '[honeysql.core :as sql]
         '[honeysql.helpers :refer :all :as helpers])
(require '[cheshire.core :refer :all]) ; parses json
(require '[jdbc.pool.c3p0 :as pool]) ; TODO - will add this later
(require '[sumo-backend.mysql-table-definitions :as tables])
(require '[sumo-backend.utils :as utils])

;; Namespace that connects to MySql

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

;;;;;;;;;;;;;;;;;;;
;; Create Tables
;;;;;;;;;;;;;;;;;;;

(defn create-tables
  "Creates the rikishi and bout tables for
   this project if they don't already exist"
  []
  (jdbc/db-do-commands
    mysql-db
    tables/bout-table))

;;;;;;;;;;;;;;;;;;;;;;;
;;; Helper functions
;;;;;;;;;;;;;;;;;;;;;;;

(defn in?
  "true if collection contains elm"
  [coll elm]
  (some #(= elm %) coll))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Ranks Queries
;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-all-ranks-in-tournament
  "returns all ranks competing in a tournament"
  [{:keys [month year]}] ; hash set of every rank
  (into #{} ; how can this be done in one sql call?
    (concat
      (map
        #(:east_rank %)
        (jdbc/query mysql-db (sql/format
          (sql/build
            :select :east_rank
            :modifiers [:distinct]
            :from :bout
            :where
            [:and
              [:= :year year]
              [:= :month month]]))))
      (map
        #(:west_rank %)
        (jdbc/query mysql-db (sql/format
          (sql/build
            :select :west_rank
            :modifiers [:distinct]
            :from :bout
            :where
            [:and
            [:= :year year]
            [:= :month month]])))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Rikishi Queries
;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rikishi
  "gets rikihsi record specified by passed in name"
  [name]
  (jdbc/query
    mysql-db
    (sql/format
      (sql/build
        :select :*
        :from :rikishi
        :where [:= :name name]))))
  
(defn list-rikishi
  "list all rikishi records"
  []
  (jdbc/query 
    mysql-db 
    (sql/format 
      (sql/build 
        :select :* 
        :from :rikishi))))

(defn rikishi-exists?
  "true if data exists for
   passed in rikishi string, false otherwise"
  [rikishi]
  (let [rikishi-names (map #(get % :name) (list-rikishi))]
   (if (in? rikishi-names (clojure.string/upper-case rikishi))
    true
    false)))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Tournament Queries
;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn list-tournaments
  "list all tournaments data exists for"
  []
  (jdbc/query 
    mysql-db 
    (sql/format
      (sql/build
        :select [:month :year]
        :modifiers [:distinct]
        :from :bout
        :order-by [[:year :desc] [:month :desc]]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Optionally Paginated Bout List Queries
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; build bout list queries
(defn build-rikishi-bout-history-query
  "given a :rikishi and :opponent, returns all bouts between the two.
   optionally takes :winner, :looser, :year, :month, and :day params"
  [{:keys [rikishi opponent winner looser rank opponent-rank is-playoff year month day]}]
  [:select :*
   :from :bout
   :where
     (concat
       [:and
         [:or
           [:and [:= :east rikishi] [:= :west opponent]]
           [:and [:= :east opponent] [:= :west rikishi]]]]
       (when winner [[:= :winner winner]])
       (when looser [[:not= :winner looser]])
       (when rank [[:or
                    [:and [:= :east rikishi] [:= :east_rank rank]]
                    [:and [:= :west rikishi] [:= :west_rank rank]]]])
       (when opponent-rank [[:or
                             [:and [:= :east opponent] [:= :east_rank opponent-rank]]
                             [:and [:= :west opponent] [:= :west_rank opponent-rank]]]])
       (when is-playoff [[:= :is_playoff true]]) ; rikishi face each other twice on same day to break tie
       (when year [[:= :year year]])
       (when month [[:= :month month]])
       (when day [[:= :day day]]))])

(defn build-bouts-by-rikishi-query
  "gets all bouts by :rikishi with optional 
   :year, :month, :day and :winner params"
  [{:keys [rikishi winner looser rank is-playoff year month day]}]
  [:select :*
   :from :bout
   :where
   (let [rikishi-clause [:or [:= :east rikishi] [:= :west rikishi]]]
     (if (or winner looser rank year month day)
       (concat
        [:and rikishi-clause]
        (when winner [[:= :winner winner]])
        (when looser [[:not= :winner looser]])
        (when rank [[:or
                     [:and [:= :east rikishi] [:= :east_rank rank]]
                     [:and [:= :west rikishi] [:= :west_rank rank]]]])
        (when is-playoff [[:= :is_playoff true]]) ; rikishi face each other twice on same day to break tie
        (when year [[:= :year year]])
        (when month [[:= :month month]])
        (when day [[:= :day day]]))
       rikishi-clause))])

(defn build-rikishi-bouts-against-rank-query
  "gets all bouts by :rikishi against :rank.
   optional :year, :month, :day, :at-rank, and :winner? params"
  [{:keys [rikishi against-rank at-rank winner looser is-playoff year month day]}]
  [:select :*
   :from :bout
   :where
   (let [rikishi-clause [:or [:= :east rikishi] [:= :west rikishi]]
         against-rank-clause [:or
                               [:and [:= :east rikishi] [:= :west_rank against-rank]]
                               [:and [:= :west rikishi] [:= :east_rank against-rank]]]]
     (if (or at-rank winner looser year month day)
       (concat
         [:and rikishi-clause against-rank-clause]
         (when winner [[:= :winner winner]])
         (when looser [[:not= :winner looser]])
         (when at-rank [[:or
                      [:and [:= :east rikishi] [:= :east_rank at-rank]]
                      [:and [:= :west rikishi] [:= :west_rank at-rank]]]])
         (when is-playoff [[:= :is_playoff true]]) ; rikishi face each other twice on same day to break tie
         (when year [[:= :year year]])
         (when month [[:= :month month]])
         (when day [[:= :day day]]))
       [:and rikishi-clause against-rank-clause]))])

(defn build-bouts-by-date-query
  "gets all bouts given optional time params :year, :month, :day params
   also takes optional :winner param. :looser param not supported because
   there is no 'looser' column in the database. can't get looser without
   passing in rikishi name and opponent name, see build-rikishi-bout-history-query"
  [{:keys [winner is-playoff year month day]}]
  [:select :*
   :from :bout
   :where
     (if (or winner year month day)
       (concat
         [:and]
         (when winner [[:= :winner winner]])
         (when is-playoff [[:= :is_playoff true]]) ; rikishi face each other twice on same day to break tie
         (when year [[:= :year year]])
         (when month [[:= :month month]])
         (when day [[:= :day day]]))
       true)])

;; runs bout-list query against the database, with optional limit and offset pagination
(defn run-bout-list-query
  "returns a bout list using the appropriate query, optionally paginated"
  [{:keys [rikishi opponent against-rank page per] :as params}]
  (jdbc/query 
    mysql-db
    (sql/format
      (apply sql/build
        (apply merge
          (cond ; bring in appropriate query given passed in params
            (and rikishi opponent) (build-rikishi-bout-history-query params)
            (and rikishi against-rank) (build-rikishi-bouts-against-rank-query params)
            rikishi (build-bouts-by-rikishi-query params)
            :else (build-bouts-by-date-query params))
          (when (and page per) ; optionally add pagination
            [:order-by [[:year :desc] [:month :desc] [:day :asc]]
             :limit (Integer/parseInt per)
             :offset (* (- (Integer/parseInt page) 1) (Integer/parseInt per))]))))))

;; top level bout-list-by-criteria function
(defn get-bout-list
  "given a set of criteria, returns a bout list.
   pass { :paginate true } to get the response paginated.
   also takes :page and :per string params to step through response pages"
  [{:keys [paginate page per] :or {page "1" per "15"} :as params}]
  (if paginate
    {:pagination {:page (Integer/parseInt page) :per (Integer/parseInt per)}
     :items (run-bout-list-query (merge {:page page :per per} params))}
    (run-bout-list-query params)))

;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Write to Database
;;;;;;;;;;;;;;;;;;;;;;;;;

(defn bout-exists?
  "true if data exists for passed in bout record
   false otherwise. boolean :is_playoff represents
   when rikishi face each other twice on the same day, 
   no logic at this time for > 2 matches on same day"
  [{:keys [east west is_playoff date]}]
  ;; should only be 1 or 0 bout here
  ;; if there's more than that, its a dupe
  ;; could add in dupe handling code later...
  (let [bout-list (get-bout-list
                    {:rikishi (:name east)
                     :opponent (:name west)
                     :year (:year date)
                     :month (:month date)
                     :day (:day date)
                     :is-playoff is_playoff})]
    (if (> (count bout-list) 0)
      true
      false)))

(defn full-tournament-data-exists?
  "true if 15 days (or more) of bout data exist
   for given tournament year and month, else false.
   shouldn't have more than 15 days for any tournament,
   but for completeness >= 15 days is full tournament"
  [{:keys [year month]}]
  (let [bout-days (jdbc/query
                    mysql-db
                    (sql/format
                      (sql/build
                        :select [:day]
                        :modifiers [:distinct]
                        :from :bout
                        :where [:and
                                [:= :year year] 
                                [:= :month month]])))]
    (if (>= (count bout-days) 15)
      true
      false)))

; TODO--
; add in functions to update rikishi records
; with info like hometown, etc
(defn write-rikishi
  "write rikishi info to the database"
  [rikishi]
  (jdbc/insert-multi!
    mysql-db
    :rikishi
    [{:name (:name rikishi)
      :image (:image rikishi)
      :name_ja (:name_ja rikishi)}]))

; TODO-- 
; add in technique_ja, and a conversion fn if its not there?
; add in technique category
(defn write-bout
  "write a bout's information to the database"
  [{:keys [east west technique technique_ja date]}]
  (jdbc/insert-multi!
    mysql-db
    :bout
    [{:east (:name east)
      :east_rank (:rank east)
      :west (:name west)
      :west_rank (:rank west)
      :winner (utils/get-bout-winner east west)
      :loser (utils/get-bout-loser east west)
      :technique technique
      :technique_ja technique_ja
      :year (:year date)
      :month (:month date)
      :day (:day date)}]))

(defn update-bout
  "writes list of fields to bout with passed in id
   fields ex: '([:west_rank_value 16] [:east_rank_value 17])"
  [id & update-fields]
  (dorun
    (map
      (fn [[field value]]
        (jdbc/update!
          mysql-db
          :bout
          {field value}
          ["id = ?" id]))
     update-fields)))

(defn read-basho-file
  "read in a file representing one day's 
   sumo basho results, and write bout and 
   rikishi records to the database if they
   haven't been previously written"
  [filepath]
  (println "reading filepath: " filepath)
  (let [data (parse-string (slurp filepath) true)
        date (utils/get-date filepath)]
    (dorun ; usually what you need is dorun, doall returns results of map, dorun forces the lazy map to execute
      (map
        (fn [{:keys [east west] :as record}]
          (let [full_record (assoc
                             record
                             :date date)]
            (when (not
                    (rikishi-exists? (:name east)))
              (write-rikishi east))
            (when (not
                    (rikishi-exists? (:name west)))
              (write-rikishi west))
            (when (not
                    (bout-exists? full_record))
              (write-bout full_record))))
        (:data data)))
    (dissoc date :day))) ; {:year :month} of tournament that data was read for

(defn read-basho-dir
  "optimized load of files from a dir. If full
   tournament data is found in the database for whatever
   file, that file is skipped"
  [all-files]
  (doall
    (map
      (fn [filepath]
        (let [date (utils/get-date filepath)]
          (when (not (full-tournament-data-exists? date))
            (read-basho-file filepath))))
     all-files)))
