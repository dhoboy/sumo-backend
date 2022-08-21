(ns sumo-backend.data.tournament
  (:require
    [clojure.java.jdbc :as jdbc]
    [clojure.string :as str]
    [honeysql.core :as sql]
    [sumo-backend.data.bout :refer [get-bout-list]]
    [sumo-backend.data.database :as db]
    [sumo-backend.data.rikishi :refer [rikishi-exists?]]))


;;
;; Namespace dealing with tournaments, results, and rank
;;


;; FYI: As far as I can tell, there are a different number of maegashira
;; each basho. All juryo rank values depend on how many maegashira there
;; are per basho.

;; sanyaku rank values are always the same
(def sanyaku-rank-values
  {:yokozuna 1
   :ozeki 2
   :sekiwake 3
   :komusubi 4})


(def lowest-sanyaku (:komusubi sanyaku-rank-values))


;;
;; Database Queries
;;

(defn list-tournaments
  "list all tournaments data exists for"
  []
  (if-let [conn (db/db-conn)]
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
  "returns list of rikishi wins in tournament.
   year and month are required."
  [{:keys [year month rikishi]}]
  (if (or (nil? year) (nil? month))
    '()
    (if-let [conn (db/db-conn)]
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
      (println "No Mysql DB"))))


;; TODO -
;; Currently doesn't differentiate between
;; a rikishi going 15 - 0 and no rikishi data
;; existing for given tournament
(defn get-losses-in-tournament
  "returns list of rikishi losses in tournament.
   year and month are required."
  [{:keys [year month rikishi]}]
  (if (or (nil? year) (nil? month))
    '()
    (if-let [conn (db/db-conn)]
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
      (println "No Mysql DB"))))


(defn get-all-ranks-in-tournament
  "returns all ranks competing in a tournament.
   year and month are required."
  [{:keys [month year]}]
  (if (or (nil? month) (nil? year))
    #{}
    (if-let [conn (db/db-conn)]
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
      (println "No Mysql DB"))))


(defn get-rikishi-rank-in-tournament
  "returns rikishi's rank string and value in given tournament.
   returns {:rank nil :rank_value nil} if rikishi did not compete in tournament.
   rikishi, year, and month are required."
  [{:keys [rikishi month year]}]
  (if (or (nil? rikishi) (nil? month) (nil? year))
    {}
    (if-let [conn (db/db-conn)]
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
              (str/upper-case (:west bout))
              (str/upper-case rikishi))
          {:rank (:west_rank bout)
           :rank_value (:west_rank_value bout)}
          {:rank (:east_rank bout)
           :rank_value (:east_rank_value bout)})
        {:rank nil :rank_value nil})
      (println "No Mysql DB"))))


;;
;; List of all rikishi's results for a given tournament
;;

(defn build-rikishi-tournament-records
  "for a given tournament year and month,
   compile all rikishi wins and losses for that tournament"
  [{:keys [year month]}]
  (if (and (nil? year) (nil? month))
    '()
    (map
      (fn [[rikishi results]]
        (merge
          {:rikishi rikishi
           :results
           (cond
             (>= (:wins results) 8) (assoc results :result "kachikoshi")
             (< (:wins results) 8) (assoc results :result "machikoshi")
             :else (assoc results :result "rikishi match data incomplete"))}
          (get-rikishi-rank-in-tournament
            {:rikishi rikishi :year year :month month})))
      (reduce
        (fn [acc next]
          (let [rikishi-name   (:rikishi next)
                rikishi-in-acc (get acc rikishi-name)]
            (assoc
              acc
              rikishi-name
              (merge
                (or rikishi-in-acc {:wins 0 :losses 0})
                (dissoc next :rikishi)))))
        {}
        (concat
          (get-wins-in-tournament {:year year :month month})
          (get-losses-in-tournament {:year year :month month}))))))


;;
;; List of specific rikishi's results over time
;;

(defn get-rikishi-results-over-time
  "returns rikishi results over all tournaments we have data for"
  ([{:keys [rikishi]}] ; top-level
   (if (rikishi-exists? rikishi)
     (get-rikishi-results-over-time
       rikishi
       (list-tournaments)
       [])
     {:error (str "Data does not exist for rikishi " rikishi)}))
  ([rikishi [{:keys [year month] :as tournament} & rest] results-over-time] ; inner fn
   (if (empty? tournament)
     results-over-time ; checked every tournament, return what you've got
     (let [wins (:wins
                  (first
                    (get-wins-in-tournament
                      {:year year :month month :rikishi rikishi})))
           losses (:losses
                    (first
                      (get-losses-in-tournament
                        {:year year :month month :rikishi rikishi})))]
       (get-rikishi-results-over-time
         rikishi
         rest
         (merge
           results-over-time
           (merge
             {:year year
              :month month
              :results
              {:wins wins
               :losses losses
               :result
               (cond
                 (nil? wins) "rikishi match data incomplete"
                 (>= wins 8) "kachikoshi"
                 (< wins 8) "machikoshi")}}
             (get-rikishi-rank-in-tournament
               {:rikishi rikishi :year year :month month}))))))))


;;
;; Get a Rikishi's opponent from a bout
;;

(defn get-bout-opponent
  "for a given rikishi and bout
   return the bout opponent name string"
  [{:keys [rikishi bout]}]
  (or
    (and
      (= (str/upper-case (:east bout)) (str/upper-case rikishi)) (:west bout))
    (and
      (= (str/upper-case (:west bout)) (str/upper-case rikishi)) (:east bout))))


;;
;; Get the winner from a given pair of east and west maps
;;

(defn get-bout-winner
  "determines winner for passed in east and west records"
  [east west]
  (if (= (:result east) "win")
    (:name east)
    (:name west)))


;;
;; Get the looser from a given pair of east and west maps
;;

(defn get-bout-loser
  "determines loser for passed in east and west records"
  [east west]
  (if (= (:result east) "win")
    (:name west)
    (:name east)))


;;
;; Parse out a rank string's keyword and number
;;

(defn get-rank-keyword
  "for a given rank string, return its rank keyword"
  [rank_str]
  ;; "Maegashira #15" -> :maegashira, "Ozeki -> :ozeki"
  (if (= 0 (count rank_str))
    nil
    (keyword
      (str/lower-case
        (first
          (str/split rank_str #" "))))))


(defn get-rank-and-file-number
  "for a given 'rank and file' rikishi rank string,
   this means either Maegashira or Juryo,
   returns their associated number"
  [rank-str]
  ;; "Maegashira #15" -> 15
  (let [number (last (str/split (str/trim rank-str) #"\#"))]
    (if (= rank-str number) ; ranks w/o number return nil
      nil
      (Integer/parseInt number))))


;;
;; Convert Rank string to a keyword
;;

(defn rank-str-to-keyword
  "given a rank string returns it as a keyword
   e.g. 'Maegashira #1' -> :maegashira_1"
  [rank-str]
  (if (= 0 (count rank-str))
    nil
    (keyword
      (str/lower-case
        (str/join
          "_"
          (map ; maping a fn over a collection, not sure what the kondo error is talking about here
            str/trim
            (str/split rank-str #"\#")))))))


;;
;; Convert Rank keyword to a string
;;

(defn rank-keyword-to-str
  "given a rank keyword returns it as a string
   e.g. :maegashira_1 -> 'Maegashira #1'.
   reverse of rank-str-to-keyword"
  [rank-keyword]
  (if rank-keyword
    (str/capitalize
      (str/join
        " #"
        (str/split
          (name rank-keyword) #"_")))
    nil))


;;
;; Get the lowest Maegashira/Juryo Rank in a tournament
;;

;; memoize here helps this fn go from
;; "Elapsed time: 18.03663 msecs"
;; to
;; "Elapsed time: 0.149619 msecs"!
(def tournament-lowest-rank-and-file
  "for a given tournament, return the lowest maegashira or juryo
   rank value competing that we have data on.
   optional :level param takes either :maegashira or :juryo,
   defaults to :maegashira"
  (memoize
    (fn [{:keys [month year level] :or {level :maegashira}}]
      (if (and month year)
        (first
          (sort
            >
            (filter
              number? ; filter out nils, only want numbers here
              (map
                #(let [word (get-rank-keyword %)
                       number (get-rank-and-file-number %)]
                   (and (= word level) number))
                (get-all-ranks-in-tournament {:month month :year year})))))
        nil))))


;;
;; Map of all rank keywords in tournament to their value.
;; Not hard-coded like sanyaku-rank-values because
;; juryo rank values depend on how many maegashira there are
;; in a given tournament.
;; e.g. with 15 maegashira, { ... :juryo_1 16 ... }
;; e.g. with 17 maegashira, { ... :juryo_1 18 ... }
;;

(defn tournament-rank-values
  "for a given tournament, defined by its :year and :month,
   derive the values of all ranks participating in that tournament"
  [{:keys [year month]}]
  (let [lowest-maegashira (tournament-lowest-rank-and-file
                            {:month month
                             :year year})
        lowest-juryo (tournament-lowest-rank-and-file
                       {:month month
                        :year year
                        :level :juryo})]
    (apply
      merge
      sanyaku-rank-values
      (concat
        (when
          lowest-maegashira
          (map
            #(hash-map (keyword (str "maegashira_" %)) (+ lowest-sanyaku %))
            (range 1 (+ lowest-maegashira 1))))
        (when
          (and lowest-maegashira lowest-juryo) ; not expected to have juryo w/o maegashira, but just incase
          (map
            #(hash-map (keyword (str "juryo_" %)) (+ lowest-sanyaku lowest-maegashira %))
            (range 1 (+ lowest-juryo 1))))))))


;;
;; Get a Rank's value in a given tournament
;;

(defn get-rank-value
  ;; "Ozeki" => 2, "Maegashira #1" => 5, :maegashira_1 => 5
  "given a rank string or keyword, returns its value.
   if no tournament passed in, defaults to rank's
   value in latest tournament"
  [{:keys [rank year month]
    :or {year (:year (first (list-tournaments)))
         month (:month (first (list-tournaments)))}}]
  (if (nil? rank)
    nil
    (let [rank-values (tournament-rank-values {:year year :month month})]
      ((if (keyword? rank)
         rank
         (rank-str-to-keyword rank))
       rank-values))))


;;
;; Get a Rikishi's rank string from a bout
;;

(defn get-rank-string-in-bout
  "for a given bout hashmap and rikishi name string,
   returns rikishi rank string if it exists,
   else returns nil"
  [{:keys [rikishi bout]}]
  (if (nil? rikishi)
    nil
    (or
      (and (=
             (str/upper-case (:east bout))
             (str/upper-case rikishi))
        (:east_rank bout))
      (and (=
             (str/upper-case (:west bout))
             (str/upper-case rikishi))
        (:west_rank bout))
      nil)))


;;
;; Get a Rikishi's rank value from a bout
;;

(defn get-rank-value-in-bout
  "given a rikishi and bout, return
   rikishi's rank value"
  [{:keys [rikishi bout]}]
  ;; "takakeisho" { :east "takakeiso" :rank "ozeki" ... } -> 2
  (get-rank-value
    {:month (:month bout)
     :year (:year bout)
     :rank (get-rank-string-in-bout {:rikishi rikishi :bout bout})}))


;;
;; Get Opponent's rank string from a bout
;;
;; FYI: Not used, add testing when this function is needed
(defn get-opponent-rank-string-in-bout
  "given a rikishi and bout, return
   rikishi opponent's rank value"
  [{:keys [rikishi bout]}]
  (get-rank-string-in-bout
    {:rikishi (get-bout-opponent {:rikishi rikishi :bout bout})
     :bout bout}))


;;
;; Get Opponent's rank value from a bout
;;
;; FYI: Not used, add testing when this function is needed
(defn get-opponent-rank-value-in-bout
  "given a rikishi and bout, return
   rikishi opponent's rank value"
  [{:keys [rikishi bout]}]
  (get-rank-value-in-bout
    {:rikishi (get-bout-opponent {:rikishi rikishi :bout bout})
     :bout bout}))


;;
;; Get a Rikishi's rank for a tournament
;;

;; some is equivalent to (first (filter identity (map ...)))
(defn get-rank-string-in-tournament
  "iterates over a list of bouts comprising a tournament,
   returns rank if found, else returns nil.
   given string 'TAKAKEISHO', returns string 'Ozeki'"
  [rikishi bouts]
  ;; ["TAKAKEISHO" '({ ..bout } {...} {...} ...)]
  (some
    #(get-rank-string-in-bout {:rikishi rikishi :bout %})
    bouts))


;;
;; Current Rikishi Rank
;;

(defn get-rikishi-current-rank
  "gets most recent rank for passed in rikishi name
   given string 'hakuho' this is returned--
   {:rank 'Yokozuna', :tournament { :month 7, :year 2020 }}
   returns nil if no data exists for passed in rikishi"
  ([{:keys [rikishi]}] ; top-level
   (if (rikishi-exists? rikishi)
     (get-rikishi-current-rank
       (str/upper-case rikishi)
       (list-tournaments))
     {:error (str "Data does not exist for rikishi " rikishi)}))
  ([rikishi [tournament & rest]] ; inner function
   (if-let [rank ; rank from checking all bouts in a tournament
            (get-rank-string-in-tournament
              rikishi
              (get-bout-list
                {:rikishi rikishi
                 :year (:year tournament)
                 :month (:month tournament)}))]
     {:tournament tournament ; rank found, we're done
      :rank rank
      :rank_value (get-rank-value
                    {:rank rank
                     :year (:year tournament)
                     :month (:month tournament)})}
     (get-rikishi-current-rank rikishi rest)))) ; rank not found, move to next tournament

;;
;; Rikishi Rank History
;;

(defn get-rikishi-rank-over-time
  "returns rikishi rank over time, from as far back as data exists.
   everytime rikishi has new rank, that is included in the list.
   nil's represent tournaments rikishi did not compete in"
  ([{:keys [rikishi]}] ; top-level
   (if (rikishi-exists? rikishi)
     (get-rikishi-rank-over-time
       (str/upper-case rikishi)
       (reverse (list-tournaments))
       [])
     {:error (str "Data does not exist for rikishi " rikishi)}))
  ([rikishi [tournament & rest] rank-over-time] ; inner function
   (if (empty? tournament)
     rank-over-time ; checked every tournament, return what you've got
     (let [rank (get-rank-string-in-tournament
                  rikishi
                  (get-bout-list
                    {:rikishi rikishi
                     :year (:year tournament)
                     :month (:month tournament)}))]
       (if (not= rank (last (map #(get % :rank) rank-over-time)))
         (get-rikishi-rank-over-time
           rikishi
           rest
           (conj rank-over-time {:rank rank :tournament tournament}))
         (get-rikishi-rank-over-time
           rikishi
           rest
           rank-over-time))))))
