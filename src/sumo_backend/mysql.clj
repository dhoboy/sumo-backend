(ns sumo-backend.mysql)
(require '[clojure.java.jdbc :as jdbc])
(require '[honeysql.core :as sql]
         '[honeysql.helpers :refer :all :as helpers])
(require '[cheshire.core :refer :all]) ; parses json
(require '[jdbc.pool.c3p0 :as pool]) ; TODO - will add this later
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

;;;;;;;;;;;;;;;;;;;;;;;
;;; Helper functions
;;;;;;;;;;;;;;;;;;;;;;;

(defn in?
  "true if collection contains elm"
  [coll elm]
  (some #(= elm %) coll))

;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Write to Database
;;;;;;;;;;;;;;;;;;;;;;;;;

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
; after all data is loaded, run another fn that writes 
; rank values to each bout
; add bout looser! that would make queries on looser so much easier
(defn write-bout
  "write a bout's information to the databae"
  [east west winning-technique date]
  (jdbc/insert-multi! 
    mysql-db 
    :bout
    [{:east (:name east) :east_rank (:rank east)
      :west (:name west) :west_rank (:rank west)
      :winner (utils/get-bout-winner east west)
      :winning_technique winning-technique
      :year (:year date) :month (:month date) :day (:day date)}]))

(defn read-basho-file
  "read in a file representing one day's sumo basho results, write it to the database"
  [filepath]
  (let [data (parse-string (slurp filepath) true)]
    (map
      (fn [record]
        ; write unique rikishi records
        (when (= (jdbc/query mysql-db ["SELECT * FROM rikishi WHERE name = ?", (:name (:east record))]) [])
          (write-rikishi 
            (:east record)))
        (when (= (jdbc/query mysql-db ["SELECT * FROM rikishi WHERE name = ?", (:name (:west record))]) [])
          (write-rikishi 
            (:west record)))
        ; write all bout data to database
        (write-bout 
          (:east record) 
          (:west record) 
          (:technique record)
          (utils/get-date filepath)))
      (:data data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Unpaginated Queries
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
  [{:keys [rikishi opponent winner looser rank opponent-rank year month day]}]
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
       (when year [[:= :year year]])
       (when month [[:= :month month]])
       (when day [[:= :day day]]))])

(defn build-bouts-by-rikishi-query
  "gets all bouts by :rikishi with optional 
   :year, :month, :day and :winner params"
  [{:keys [rikishi winner looser rank year month day]}]
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
        (when year [[:= :year year]])
        (when month [[:= :month month]])
        (when day [[:= :day day]]))
       rikishi-clause))])

(defn build-rikishi-bouts-against-rank-query
  "gets all bouts by :rikishi against :rank.
   optional :year, :month, :day, :at-rank, and :winner? params"
  [{:keys [rikishi against-rank at-rank winner looser year month day]}]
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
         (when year [[:= :year year]])
         (when month [[:= :month month]])
         (when day [[:= :day day]]))
       [:and rikishi-clause against-rank-clause]))])

(defn build-bouts-by-date-query
  "gets all bouts given optional time params :year, :month, :day params
   also takes optional :winner param. :looser param not supported because
   there is no 'looser' column in the database. can't get looser without
   passing in rikishi name and opponent name, see build-rikishi-bout-history-query"
  [{:keys [winner year month day]}]
  [:select :*
   :from :bout
   :where
     (if (or winner year month day)
       (concat
         [:and]
         (when winner [[:= :winner winner]])
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
