(ns sumo-backend.service.mysql)
(require '[clojure.java.jdbc :as jdbc])
(require '[honeysql.core :as sql]
         '[honeysql.helpers :refer :all :as helpers])
(require '[cheshire.core :refer :all]) ; parses json
(require '[jdbc.pool.c3p0 :as c3p0])
(require '[sumo-backend.service.mysql-schema :as schema])
(require '[sumo-backend.utils :as utils :refer [when-let-all]])

;; Namespace that connects to MySql

(def key-file "./keys/mysql.json")

;; Rather than opening a new connection for each query, it's more efficient
;; and easier on the DB to maintan one or more connections, scaling
;; them as needed.
(def db-conn (atom nil))

;; @bslawski when this key file was in a def above, like
;; (def mysql-keys (:local (parse-string (slurp key-file) true)))
;; the db here would seem to evaluate before the file was loaded?
;; i was getting Access denied for user ''@'localhost' to database 'sumo'
;; errors the whole time
(defn db
  []
  (or
    @db-conn
    (when-let-all [db-keys (:local (parse-string (slurp key-file) true))
                   conn (c3p0/make-datasource-spec
                          {:classname "com.mysql.jdbc.Driver"
                           :subprotocol "mysql"
                           :initial-pool-size 3
                           :subname "//127.0.0.1:3306/sumo?characterEncoding=utf8"
                           :user (:user db-keys)
                           :password (:password db-keys)})]
      (reset! db-conn conn)
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

;;;;;;;;;;;;;;;;;;;
;; Create Tables
;;;;;;;;;;;;;;;;;;;

;; @bslawski why does the connetion need to be bound in a let
;; inside the fn, not just made in a def one time (def conn (db))?
(defn create-tables
  "Creates the rikishi and bout tables for
   this project if they don't already exist"
  []
  (if-let [conn (db)]
    (do
      (jdbc/db-do-commands
        conn
        schema/rikishi-table)
      (jdbc/db-do-commands
        conn
        schema/bout-table)
      (println "Rikishi and Bout tables created!"))
    (println "No Mysql DB")))

;;;;;;;;;;;;;;;;;;;
;; Drop Tables
;;;;;;;;;;;;;;;;;;;

(defn drop-tables
  "Drops the rikishi and bout tables for
   this project if they exist"
  []
  (if-let [conn (db)]
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
  [{:keys [month year]}]
  (if-let [conn (db)]
    (set
      (mapcat
        #(vals (select-keys % [:east_rank :west_rank]))
        (jdbc/query
          conn
          (sql/format
            (sql/build
              :select [:east_rank :west_rank]
              :from :bout
              :where
              [:and
               [:= :year year]
               [:= :month month]])))))
    (println "No Mysql DB")))

(defn get-rikishi-rank-in-tournament
  "returns rikishi's rank string and value in given tournament,
     returns {:rank nil :rank-value nil}
     if rikishi did not compete in tournament"
  [{:keys [rikishi month year]}]
  (if-let [conn (db)]
    (if-let [bout (first
                    (jdbc/query
                      conn
                      (sql/format
                        (sql/build
                          :select :*
                          :from :bout
                          :where
                          [:and
                           [:or
                            [:= :west rikishi]
                            [:= :east rikishi]]
                           [:= :year year]
                           [:= :month month]]))))]
      (if (=
           (clojure.string/upper-case (:west bout))
           (clojure.string/upper-case rikishi))
        {:rank (:west_rank bout)
         :rank-value (:west_rank_value bout)}
        {:rank (:east_rank bout)
         :rank-value (:east_rank_value bout)})
      {:rank nil :rank-value nil})
    (println "No Mysql DB")))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Technique Queries
;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn list-techniques
  "list all techniques used in bouts optionally constrained
   to passed in year, month, day params"
  [{:keys [year month day]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:technique :technique_en :technique_category]
          :modifiers [:distinct]
          :from :bout
          :where
          (if (or year month day)
            (concat
              [:and]
              (when year [[:= :year year]])
              (when month [[:= :month month]])
              (when day [[:= :day day]]))
            true))))
    (println "No Mysql DB")))

;; wins / losses for rikishi with technique
(defn get-rikishi-wins-by-technique
  "returns techniques rikishi has won by and frequency"
  [{:keys [rikishi year month day]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:technique :technique_en :technique_category [:%count.technique :count]]
          :modifiers [:distinct]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :winner rikishi]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by [:technique :technique_en :technique_category]
          :order-by [[:%count.technique :desc]])))
    (println "No Mysql DB")))

(comment
  (println (get-rikishi-wins-by-technique {:rikishi "ENDO"})))

(defn get-rikishi-wins-by-technique-category
  "returns technique categories rikishi has won by and frequency"
  [{:keys [rikishi year month day]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:technique_category [:%count.technique_category :count]]
          :modifiers [:distinct]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :winner rikishi]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :technique_category
          :order-by [[:%count.technique_category :desc]])))
    (println "No Mysql DB")))

(comment
  (println (get-rikishi-wins-by-technique-category {:rikishi "ENDO"})))

(defn get-rikishi-losses-to-technique
  "returns techniques rikishi has lost to and frequency"
  [{:keys [rikishi year month day]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:technique :technique_en :technique_category [:%count.technique :count]]
          :modifiers [:distinct]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :loser rikishi]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by [:technique :technique_en :technique_category]
          :order-by [[:%count.technique :desc]])))
    (println "No Mysql DB")))

(comment
  (println (get-rikishi-losses-to-technique {:rikishi "ENDO"})))


(defn get-rikishi-losses-to-technique-category
  "returns technique categories rikishi has lost to and frequency"
  [{:keys [rikishi year month day]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:technique_category [:%count.technique_category :count]]
          :modifiers [:distinct]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :loser rikishi]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :technique_category
          :order-by [[:%count.technique_category :desc]])))
    (println "No Mysql DB")))

;; all wins / losses for technique
(defn get-all-wins-by-technique
  "returns rikishi and number of times they have won with technique"
  [{:keys [technique year month day]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:winner [:%count.winner :count]]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :technique technique]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :winner
          :order-by [[:%count.winner :desc]])))
    (println "No Mysql DB")))

(defn get-all-wins-by-technique-category
  "returns rikishi and number of times they have won with technique category"
  [{:keys [category year month day]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:winner [:%count.winner :count]]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :technique_category category]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :winner
          :order-by [[:%count.winner :desc]])))
    (println "No Mysql DB")))

(defn get-all-losses-to-technique
  "returns rikishi and number of times they have lost to technique"
  [{:keys [technique year month day]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:loser [:%count.loser :count]]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :technique technique]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :loser
          :order-by [[:%count.loser :desc]])))
    (println "No Mysql DB")))

(defn get-all-losses-to-technique-category
  "returns rikishi and number of times they have lost to technique category"
  [{:keys [category year month day]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:loser [:%count.loser :count]]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :technique_category category]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :loser
          :order-by [[:%count.loser :desc]])))
    (println "No Mysql DB")))

; not sure if this is needed, leaving for now
(defn techniques-used
  "returns map of techniques used in the passed in tournament year and month.
   map keys are the technique Japanese name for each technique
   e.g. {:oshidashi {:jp 'oshidashi' :en 'Frontal push out' :cat 'push'}}"
  [{:keys [year month]}]
  (if-let [conn (db)]
    (reduce
      (fn [acc {:keys [technique technique_en technique_category]}]
        (assoc
          acc
          (keyword (clojure.string/lower-case technique_en))
          {:en technique_en :jp technique :cat technique_category}))
      {}
      (filter
        #(some? (:technique %))
        (jdbc/query
          conn
          (sql/format
            (sql/build
              :select [:technique :technique_en :technique_category]
              :modifiers [:distinct]
              :from :bout
              :where
              [:and
               [:= :year year]
               [:= :month month]])))))
    (println "No Mysql DB")))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Rikishi Queries
;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rikishi
  "gets rikihsi record specified by passed in name"
  [name]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select :*
          :from :rikishi
          :where [:= :name name])))
    (println "No Mysql DB")))

(defn list-rikishi
  "list all rikishi records"
  []
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select :*
          :from :rikishi)))
    (println "No Mysql DB")))

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
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:month :year]
          :modifiers [:distinct]
          :from :bout
          :order-by [[:year :desc] [:month :desc]])))
    (println "No Mysql DB")))

;; TODO -
;; Currently doesn't differentiate between
;; a rikishi going 0 - 15 and no rikishi data
;; existing for given tournament
(defn get-wins-in-tournament
  "returns list of rikishi wins in tournament"
  [{:keys [year month rikishi]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build ; column is winner, as rikishi... count(winner) as wins
          :select [[:winner :rikishi] [:%count.winner :wins]]
          :from :bout
          :where
          (concat
            [:and
             [:= :year year]
             [:= :month month]]
            (when rikishi [[:= :winner rikishi]]))
          :group-by :winner
          :order-by [[:%count.winner :desc]])))
    (println "No Mysql DB")))

;; TODO -
;; Currently doesn't differentiate between
;; a rikishi going 15 - 0 and no rikishi data
;; existing for given tournament
(defn get-losses-in-tournament
  "returns list of rikishi losses in tournament"
  [{:keys [year month rikishi]}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [[:loser :rikishi] [:%count.loser :losses]]
          :from :bout
          :where
          (concat
            [:and
             [:= :year year]
             [:= :month month]]
            (when rikishi [[:= :loser rikishi]]))
          :group-by :loser
          :order-by [[:%count.loser :asc]])))
    (println "No Mysql DB")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Optionally Paginated Bout List Queries
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; build bout list queries
(defn build-rikishi-bout-history-query
  "given a :rikishi and :opponent, returns all bouts between the two.
   optionally takes--
     :winner, :loser, :rank, :opponent-rank,
     :is-playoff, :year, :month, :day, and :total-only params"
  [{:keys [rikishi opponent winner loser technique technique-category
           rank opponent-rank is-playoff year month day total-only]}]
  [:select (if (some? total-only) [[:%count.* :total]] :*)
   :from :bout
   :where
   (concat
     [:and
      [:or
       [:and [:= :east rikishi] [:= :west opponent]]
       [:and [:= :east opponent] [:= :west rikishi]]]]
     (when winner [[:= :winner winner]])
     (when loser [[:= :loser loser]])
     (when technique [[:= :technique technique]])
     (when technique-category [[:= :technique_category technique-category]])
     (when rank [[:or
                  [:and [:= :east rikishi] [:= :east_rank rank]]
                  [:and [:= :west rikishi] [:= :west_rank rank]]]])
     (when opponent-rank [[:or
                           [:and [:= :east opponent] [:= :east_rank opponent-rank]]
                           [:and [:= :west opponent] [:= :west_rank opponent-rank]]]])
     (when is-playoff [[:= :is_playoff 1]]) ; fight twice on same day to break tie
     (when year [[:= :year year]])
     (when month [[:= :month month]])
     (when day [[:= :day day]]))])

(defn build-bouts-by-rikishi-query
  "gets all bouts by :rikishi.
   optionally takes--
     :winner, :loser, :rank, :is-playoff,
     :year, :month, :day, and :total-only params"
  [{:keys [rikishi winner loser technique technique-category
           rank is-playoff year month day total-only]}]
  [:select (if (some? total-only) [[:%count.* :total]] :*)
   :from :bout
   :where
   (let [rikishi-clause [:or [:= :east rikishi] [:= :west rikishi]]]
     (if (or winner loser technique technique-category rank is-playoff year month day)
       (concat
         [:and rikishi-clause]
         (when winner [[:= :winner winner]])
         (when loser [[:= :loser loser]])
         (when technique [[:= :technique technique]])
         (when technique-category [[:= :technique_category technique-category]])
         (when rank [[:or
                      [:and [:= :east rikishi] [:= :east_rank rank]]
                      [:and [:= :west rikishi] [:= :west_rank rank]]]])
         (when is-playoff [[:= :is_playoff 1]]) ; fight twice on same day to break tie
         (when year [[:= :year year]])
         (when month [[:= :month month]])
         (when day [[:= :day day]]))
       rikishi-clause))])

(defn build-rikishi-bouts-against-rank-query
  "gets all bouts by :rikishi against :against-rank.
   optionally takes--
     :at-rank, :winner, :loser, :technique, :technique-category,
     :comparison, :is-playoff, :year, :month, :day, and :total-only params"
  [{:keys [rikishi against-rank against-rank-value at-rank comparison winner loser
           technique technique-category is-playoff year month day total-only]}]
  [:select (if (some? total-only) [[:%count.* :total]] :*)
   :from :bout
   :where
   (let [rikishi-clause [:or [:= :east rikishi] [:= :west rikishi]]
         against-rank-clause (if (and
                                   (some? comparison)
                                   (some? against-rank-value)
                                   (not= comparison "="))
                               [:or
                                [:and
                                 [:= :east rikishi]
                                 [(keyword comparison)
                                  :west_rank_value against-rank-value]]
                                [:and
                                 [:= :west rikishi]
                                 [(keyword comparison)
                                  :east_rank_value against-rank-value]]]
                               [:or
                                [:and [:= :east rikishi] [:= :west_rank against-rank]]
                                [:and [:= :west rikishi] [:= :east_rank against-rank]]])]
     (if (or at-rank winner loser technique technique-category is-playoff year month day)
       (concat
         [:and rikishi-clause against-rank-clause]
         (when winner [[:= :winner winner]])
         (when loser [[:= :loser loser]])
         (when technique [[:= :technique technique]])
         (when technique-category [[:= :technique_category technique-category]])
         (when at-rank [[:or
                         [:and [:= :east rikishi] [:= :east_rank at-rank]]
                         [:and [:= :west rikishi] [:= :west_rank at-rank]]]])
         (when is-playoff [[:= :is_playoff 1]]) ; rikishi face each other twice on same day to break tie
         (when year [[:= :year year]])
         (when month [[:= :month month]])
         (when day [[:= :day day]]))
       [:and rikishi-clause against-rank-clause]))])

(defn build-upset-query
  "gets all bouts that are an upset of specified :rank-delta.
   optionally takes--
   (or :winner :loser), :comparison, :is-playoff, :technique
   :technique-category, :year, :month, :day, and :total-only params"
  [{:keys [winner loser rank-delta comparison technique technique-category
           is-playoff year month day total-only] :or {comparison "="}}]
  [:select (if (some? total-only) [[:%count.* :total]] :*)
   :from :bout
   :where
   (let [all-upsets-clause [:or
                            [(keyword comparison)
                             [:- :west_rank_value :east_rank_value] rank-delta]
                            [(keyword comparison)
                             [:- :east_rank_value :west_rank_value] rank-delta]]
         winner-clause [:and
                        [:= :winner winner]
                        [:or
                         [:and
                          [:= :east winner]
                          [(keyword comparison)
                           [:- :east_rank_value :west_rank_value] rank-delta]]
                         [:and
                          [:= :west winner]
                          [(keyword comparison)
                           [:- :west_rank_value :east_rank_value] rank-delta]]]]
         loser-clause [:and
                       [:= :loser loser]
                       [:or
                        [:and
                         [:= :east loser]
                         [(keyword comparison)
                          [:- :west_rank_value :east_rank_value] rank-delta]]
                        [:and
                         [:= :west loser]
                         [(keyword comparison)
                          [:- :east_rank_value :west_rank_value] rank-delta]]]]]
     (concat
       [:and]
       (when (and (nil? winner) (nil? loser)) [all-upsets-clause])
       (when winner [winner-clause])
       (when loser [loser-clause])
       (when technique [[:= :technique technique]])
       (when technique-category [[:= :technique_category technique-category]])
       (when is-playoff [[:= :is_playoff 1]]) ; fight twice on same day to break tie
       (when year [[:= :year year]])
       (when month [[:= :month month]])
       (when day [[:= :day day]])))])

(defn build-bouts-by-date-query
  "gets all bouts.
   optionally takes--
     :winner, :loser, :is-playoff,
     :year, :month, :day, and :total-only params"
  [{:keys [winner loser technique technique-category is-playoff
           year month day total-only]}]
  [:select (if (some? total-only) [[:%count.* :total]] :*)
   :from :bout
   :where
   (if (or winner loser technique technique-category is-playoff year month day)
     (concat
       [:and]
       (when winner [[:= :winner winner]])
       (when loser [[:= :loser loser]])
       (when technique [[:= :technique technique]])
       (when technique-category [[:= :technique_category technique-category]])
       (when is-playoff [[:= :is_playoff 1]]) ; fight twice on same day to break tie
       (when year [[:= :year year]])
       (when month [[:= :month month]])
       (when day [[:= :day day]]))
     true)])

;; runs bout-list query against the database, with optional limit and offset pagination
(defn run-bout-list-query
  "returns a bout list using the appropriate query, optionally paginated"
  [{:keys [rikishi opponent against-rank rank-delta page per] :as params}]
  (if-let [conn (db)]
    (jdbc/query
      conn
      (sql/format
        (apply sql/build
          (apply merge
            (cond ; bring in appropriate query given passed in params
              (and rikishi opponent) (build-rikishi-bout-history-query params)
              (and rikishi against-rank) (build-rikishi-bouts-against-rank-query params)
              rank-delta (build-upset-query params)
              rikishi (build-bouts-by-rikishi-query params)
              :else (build-bouts-by-date-query params))
            (when (and page per) ; optionally add pagination
              [:order-by [[:year :desc] [:month :desc] [:day :asc]]
               :limit (Integer/parseInt per)
               :offset (* (- (Integer/parseInt page) 1) (Integer/parseInt per))])))))
    (println "No Mysql DB")))

;; top level bout-list-by-criteria function
;; @bslawski, should I memoize fns like this that make DB calls?
(defn get-bout-list
  "given a set of criteria, returns a bout list.
   pass { :paginate true } to get the response paginated.
   also takes :page and :per string params to step through response pages"
  [{:keys [paginate page per] :or {page "1" per "15"} :as params}]
  (if paginate
    {:pagination
     {:page (Integer/parseInt page)
      :per (Integer/parseInt per)
      :total (:total (first (run-bout-list-query (merge {:total-only true} params))))}
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
  (if-let [conn (db)]
    (let [bout-days (jdbc/query
                      conn
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
        false))
    (println "No Mysql DB")))

; TODO--
; add in functions to update rikishi records
; with info like hometown, etc
(defn write-rikishi
  "write rikishi info to the database"
  [rikishi]
  (if-let [conn (db)]
    (jdbc/insert-multi!
      conn
      :rikishi
      [{:name (:name rikishi)
        :image (:image rikishi)
        :name_ja (:name_ja rikishi)}])
    (println "No Mysql DB")))

(defn write-bout
  "write a bout's information to the database"
  [{:keys [east west is_playoff technique_en technique technique_category date]}]
  (if-let [conn (db)]
    (jdbc/insert-multi!
      conn
      :bout
      [{:east (:name east)
        :east_rank (:rank east)
        :west (:name west)
        :west_rank (:rank west)
        :winner (utils/get-bout-winner east west)
        :loser (utils/get-bout-loser east west)
        :is_playoff is_playoff
        :technique technique
        :technique_en technique_en
        :technique_category technique_category
        :year (:year date)
        :month (:month date)
        :day (:day date)}])
    (println "No Mysql DB")))

(defn update-bout
  "writes list of fields to bout with passed in id
   fields ex: '([:west_rank_value 16] [:east_rank_value 17])"
  [id & update-fields]
  (if-let [conn (db)]
    (dorun
      (map
        (fn [[field value]]
          (jdbc/update!
            conn
            :bout
            {field value}
            ["id = ?" id]))
        update-fields))
    (println "No Mysql DB")))

(defn read-basho-file
  "read in a file representing one day's
   sumo basho results, and write bout and
   rikishi records to the database if they
   haven't been previously written"
  [filepath]
  (println "reading filepath:" filepath)
  (let [data (parse-string (slurp filepath) true)
        date (utils/get-date filepath)]
    (dorun ; usually what you need is dorun, doall returns results of map, dorun forces the lazy map to execute
      (map
        (fn [{:keys [east west] :as record}]
          (let [full_record (assoc
                              record
                              :date date)]
            (when (not (rikishi-exists? (:name east)))
              (write-rikishi east))
            (when (not (rikishi-exists? (:name west)))
              (write-rikishi west))
            (when (not (bout-exists? full_record))
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
