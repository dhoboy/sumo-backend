(ns sumo-backend.mysql)
(require '[clojure.java.jdbc :as jdbc])
(require '[honeysql.core :as sql]
         '[honeysql.helpers :refer :all :as helpers])
(require '[cheshire.core :refer :all]) ; parses json
(require '[jdbc.pool.c3p0 :as pool])

;; Only namespace that connects to MySql 

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
;;; HELPER FUNCTIONS
;;;;;;;;;;;;;;;;;;;;;;;

(defn in?
  "true if collection contains elm"
  [coll elm]
  (some #(= elm %) coll))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; UNPAGINATED QUERIES... wrap in pagination later
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-all-ranks-in-tournament
  "returns all ranks competing in a tournament"
  [month year] ; hash set of every rank
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

(defn list-bouts
  "list all bouts data exists for"
  []
  (jdbc/query mysql-db (sql/format
    (sql/build
      :select [:month :year]
      :modifiers [:distinct]
      :from :bout
      :order-by [[:year :desc] [:month :desc]]))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; QUERIES WITH OPTIONAL PAGINATION
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; build queries
(defn build-rikishi-bout-history-query
  "given a :rikishi and :opponent, returns all bouts between the two.
   optionally takes :winner, :looser, :year, :month, and :day params"
  [{:keys [rikishi opponent winner looser year month day]}]
  [:select :*
   :from :bout
   :where
     [:and
       [:or
         [:and [:= :east rikishi] [:= :west opponent]]
         [:and [:= :east rikishi] [:= :west opponent]]]
       (if winner [:= :winner winner] nil)
       (if looser
         [:and
           [:or [:= :east rikishi] [:= :west rikishi]]
           [:not= :winner looser]]
         nil)
       (if year [:= :year year] nil)
       (if month [:= :month month] nil)
       (if day [:= :day day] nil)]])

(defn build-bouts-by-rikishi-query
  "gets all bouts by :rikishi with optional 
   :year, :month, :day and :winner params"
  [{:keys [rikishi winner looser year month day]}]
  [:select :*
   :from :bout
   :where
   (let [rikishi-clause [:or [:= :east rikishi] [:= :west rikishi]]]
     (if (or winner looser year month day)
       (concat
         [:and rikishi-clause]
         (when winner [[:= :winner winner]])
         (when looser [[:and rikishi-clause [:not= :winner looser]]])
         (when year [[:= :year year]])
         (when month [[:= :month month]])
         (when day [[:= :day day]]))
       rikishi-clause))])

(defn build-bouts-by-date-query
  "gets all bouts given optional time params :year, :month, :day params
   also takes optional :winner param. :looser param not supported because
   there is no 'looser' column in the database. can't get looser without
   passing in rikishi name, which is build-bouts-by-rikishi-query"
  [{:keys [winner year month day]}]
  [:select :*
   :from :bout
   :where
     (if (or winner year month day)
       [:and
         (if winner [:= :winner winner] nil)
         (if year [:= :year year] nil)
         (if month [:= :month month] nil)
         (if day [:= :day day] nil)]
       true)])

; runs query against the database
(defn run-bout-list-query
  "returns a bout list using the appropriate query, optionally paginated"
  [{:keys [rikishi opponent page per] :as params}]
  (jdbc/query 
    mysql-db
    (sql/format
      (apply sql/build
        (apply merge ; every query in this file could be run through this
          (cond ; bring in appropriate query given passed in params
            (and rikishi opponent) (build-rikishi-bout-history-query params)
            (and rikishi) (build-bouts-by-rikishi-query params)
            :else (build-bouts-by-date-query params))
          (if (and page per) ; optionally add pagination
            [:order-by [[:year :desc] [:month :desc] [:day :asc]]
             :limit (Integer/parseInt per)
             :offset (* (- (Integer/parseInt page) 1) (Integer/parseInt per))]
           nil))))))

; top level bout list by criteria function
(defn get-bout-list
  "given a set of criteria, returns a bout list.
   pass { :paginate true } to get the response paginated.
   also takes :page and :per string params to step through response pages"
  [{:keys [paginate page per] :or {page "1" per "15"} :as params}]
  (if paginate
    {:pagination {:page (Integer/parseInt page) :per (Integer/parseInt per)}
     :items (run-bout-list-query (merge {:page page :per per} params))}
    (run-bout-list-query params)))
