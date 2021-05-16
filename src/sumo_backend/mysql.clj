(ns sumo-backend.mysql)
(require '[clojure.java.jdbc :as jdbc])
(require '[honeysql.core :as sql]
         '[honeysql.helpers :refer :all :as helpers])
(require '[cheshire.core :refer :all]) ; parses json
(require '[jdbc.pool.c3p0 :as pool])


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


(defn in?
  "true if collection contains elm"
  [coll elm]
  (some #(= elm %) coll))

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
  [{:keys [name year month day page per] :or {page "1" per "10"}}]
    {:pagination {:page (Integer/parseInt page) :per (Integer/parseInt per)}
     :items
       (jdbc/query mysql-db (sql/format
         (sql/build
           :select :*
           :from :bout
           :where
             [:and
               [:or [:= :east name] [:= :west name]]
               (if year [:= :year year] nil)
               (if month [:= :month month] nil)
               (if day [:= :day day] nil)]
           :order-by [[:year :desc] [:month :desc] [:day :asc]]
           :limit (Integer/parseInt per)
           :offset (* (- (Integer/parseInt page) 1) (Integer/parseInt per)))))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rikishi's Head to Head Matchups
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

