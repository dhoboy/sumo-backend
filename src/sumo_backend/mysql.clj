(ns sumo-backend.mysql)
(require '[clojure.java.jdbc :as jdbc])
(require '[honeysql.core :as sql]
         '[honeysql.helpers :refer :all :as helpers])
(require '[cheshire.core :refer :all]) ; parses json
(require '[jdbc.pool.c3p0 :as pool]) ; TODO - will add this later
(require '[sumo-backend.mysql-schema :as schema])
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
    schema/rikishi-table)
  (jdbc/db-do-commands
    mysql-db
    schema/bout-table))

;;;;;;;;;;;;;;;;;;;
;; Drop Tables
;;;;;;;;;;;;;;;;;;;

(defn drop-tables
  "Drops the rikishi and bout tables for
   this project if they exist"
  []
  (jdbc/db-do-commands
    mysql-db
    (jdbc/drop-table-ddl
      :rikishi
      {:conditional? true}))
  (jdbc/db-do-commands
    mysql-db
    (jdbc/drop-table-ddl
      :bout
      {:conditional? true})))

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

  (defn get-rikishi-rank-in-tournament
    "returns rikishi's rank string and value in given tournament,
     returns {:rank nil :rank-value nil} if rikishi did not compete in tournament"
    [{:keys [rikishi month year]}]
    (if-let [bout (first
                    (jdbc/query
                      mysql-db
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
      {:rank nil :rank-value nil}))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Technique Queries
;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn list-techniques
  "list all techniques used in bouts optionally constrained
   to passed in year, month, day params"
  [{:keys [year month day]}]
  (jdbc/query
   mysql-db
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
         true)))))

(defn get-rikishi-wins-by-technique
  "returns techniques rikishi has won by and frequency"
  [{:keys [rikishi year month day]}]
  (jdbc/query
    mysql-db
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
        :group-by :technique
        :order-by [[:%count.technique :desc]]))))

  (defn get-rikishi-wins-by-technique-category
    "returns technique categories rikishi has won by and frequency"
    [{:keys [rikishi year month day]}]
    (jdbc/query
      mysql-db
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
          :order-by [[:%count.technique_category :desc]]))))

(defn get-rikishi-losses-to-technique
  "returns techniques rikishi has lost to and frequency"
  [{:keys [rikishi year month day]}]
  (jdbc/query
    mysql-db
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
        :group-by :technique
        :order-by [[:%count.technique :desc]]))))

(defn get-rikishi-losses-to-technique-category
  "returns technique categories rikishi has lost to and frequency"
  [{:keys [rikishi year month day]}]
  (jdbc/query
   mysql-db
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
      :order-by [[:%count.technique_category :desc]]))))

(defn get-wins-by-technique
  "returns rikishi and number of times they have won with technique"
  [{:keys [technique year month day]}]
  (jdbc/query
    mysql-db
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
        :order-by [[:%count.winner :desc]]))))

(defn get-wins-by-technique-category
  "returns rikishi and number of times they have won with technique category"
  [{:keys [category year month day]}]
  (jdbc/query
    mysql-db
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
        :order-by [[:%count.winner :desc]]))))

(defn get-losses-to-technique
  "returns rikishi and number of times they have lost to technique"
  [{:keys [technique year month day]}]
  (jdbc/query
    mysql-db
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
        :order-by [[:%count.loser :desc]]))))

(defn get-losses-to-technique-category
  "returns rikishi and number of times they have lost to technique category"
  [{:keys [category year month day]}]
  (jdbc/query
    mysql-db
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
        :order-by [[:%count.loser :desc]]))))
  
; not sure if this is needed, leaving for now
(defn techniques-used
  "returns map of techniques used in the passed in tournament year and month.
   map keys are the technique Japanese name for each technique
   e.g. {:oshidashi {:jp 'oshidashi' :en 'Frontal push out' :cat 'push'}}"
  [{:keys [year month]}]
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
          mysql-db
          (sql/format
            (sql/build
              :select [:technique :technique_en :technique_category]
              :modifiers [:distinct]
              :from :bout
              :where 
                [:and
                  [:= :year year]
                  [:= :month month]]))))))

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

  (defn get-wins-in-tournament
    "returns list of rikishi wins in tournament"
    [{:keys [year month rikishi]}]
    (jdbc/query
     mysql-db
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
       :order-by [[:%count.winner :desc]]))))

  (defn get-losses-in-tournament
    "returns list of rikishi losses in tournament"
    [{:keys [year month rikishi]}]
    (jdbc/query
      mysql-db
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
        :order-by [[:%count.loser :asc]]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Optionally Paginated Bout List Queries
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; build bout list queries
(defn build-rikishi-bout-history-query
  "given a :rikishi and :opponent, returns all bouts between the two.
   optionally takes--
     :winner, :loser, :rank, :opponent-rank, 
     :is-playoff, :year, :month, and :day params"
  [{:keys [rikishi opponent winner loser technique technique-category 
           rank opponent-rank is-playoff year month day]}]
  [:select :*
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
      (when is-playoff [[:= :is_playoff 1]]) ; rikishi face each other twice on same day to break tie
      (when year [[:= :year year]])
      (when month [[:= :month month]])
      (when day [[:= :day day]]))])

(defn build-bouts-by-rikishi-query
  "gets all bouts by :rikishi. 
   optionally takes--
     :winner, :loser, :rank, :is-playoff, 
     :year, :month, and :day params"
  [{:keys [rikishi winner loser technique technique-category 
           rank is-playoff year month day]}]
  [:select :*
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
        (when is-playoff [[:= :is_playoff 1]]) ; rikishi face each other twice on same day to break tie
        (when year [[:= :year year]])
        (when month [[:= :month month]])
        (when day [[:= :day day]]))
       rikishi-clause))])

(defn build-rikishi-bouts-against-rank-query
  "gets all bouts by :rikishi against :against-rank.
   optionally takes--
     :at-rank, :winner, :loser, :technique, :technique-category,
     :comparison, :is-playoff, :year, :month, and :day params"
  [{:keys [rikishi against-rank against-rank-value at-rank comparison winner loser 
           technique technique-category is-playoff year month day]}]
  [:select :*
   :from :bout
   :where
   (let [rikishi-clause [:or [:= :east rikishi] [:= :west rikishi]]
         against-rank-clause (if (and (some? comparison) (some? against-rank-value) (not= comparison "="))
                               [:or
                                [:and [:= :east rikishi] [(keyword comparison) :west_rank_value against-rank-value]]
                                [:and [:= :west rikishi] [(keyword comparison) :east_rank_value against-rank-value]]]
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
   (or :winner :loser), :comparison, :is-playoff 
   :year, :month, and :day params"
  [{:keys [winner loser rank-delta comparison is-playoff year month day] :or {comparison "="}}]
  [:select :*
   :from :bout
   :where
   (let [all-upsets-clause [:or 
                             [(keyword comparison) [:- :west_rank_value :east_rank_value] rank-delta]
                             [(keyword comparison) [:- :east_rank_value :west_rank_value] rank-delta]]
         winner-clause [:and
                         [:= :winner winner]
                         [:or
                           [:and
                             [:= :east winner]
                             [(keyword comparison) [:- :east_rank_value :west_rank_value] rank-delta]]
                           [:and
                             [:= :west winner]
                             [(keyword comparison) [:- :west_rank_value :east_rank_value] rank-delta]]]]
         loser-clause [:and
                        [:= :loser loser]
                        [:or
                          [:and
                            [:= :east loser]
                            [(keyword comparison) [:- :west_rank_value :east_rank_value] rank-delta]]
                          [:and
                            [:= :west loser]
                            [(keyword comparison) [:- :east_rank_value :west_rank_value] rank-delta]]]]]
     (concat
       [:and]
       (when (and (nil? winner) (nil? loser)) [all-upsets-clause])
       (when winner [winner-clause])
       (when loser [loser-clause])
       (when is-playoff [[:= :is_playoff 1]]) ; rikishi face each other twice on same day to break tie
       (when year [[:= :year year]])
       (when month [[:= :month month]])
       (when day [[:= :day day]])))])

(defn build-bouts-by-date-query
  "gets all bouts. 
   optionally takes--
     :winner, :loser, :is-playoff,
     :year, :month, and :day params"
  [{:keys [winner loser technique technique-category is-playoff year month day]}]
  [:select :*
   :from :bout
   :where
     (if (or winner loser technique technique-category is-playoff year month day)
       (concat
        [:and]
        (when winner [[:= :winner winner]])
        (when loser [[:= :loser loser]])
        (when technique [[:= :technique technique]])
        (when technique-category [[:= :technique_category technique-category]])
        (when is-playoff [[:= :is_playoff 1]]) ; rikishi face each other twice on same day to break tie
        (when year [[:= :year year]])
        (when month [[:= :month month]])
        (when day [[:= :day day]]))
       true)])

;; runs bout-list query against the database, with optional limit and offset pagination
(defn run-bout-list-query
  "returns a bout list using the appropriate query, optionally paginated"
  [{:keys [rikishi opponent against-rank rank-delta page per] :as params}]
  (jdbc/query
    mysql-db
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

(defn write-bout
  "write a bout's information to the database"
  [{:keys [east west is_playoff technique_en technique technique_category date]}]
  (jdbc/insert-multi!
    mysql-db
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
  (println "reading filepath:" filepath)
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
