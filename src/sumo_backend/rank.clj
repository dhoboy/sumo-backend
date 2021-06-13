(ns sumo-backend.rank)
(require '[sumo-backend.mysql :as db])
(require '[sumo-backend.utils :as utils])

;; Everything to do with Rikishi Ranks

;; N.B. There are a different
;; number of maegashira each basho.
;; All juryo ranks depend
;; on how many maegashira there are per basho.

;; TODO--
;; use memoization where possible to make some of these calls cache

;; sanyaku rank values are always the same
(def sanyaku-rank-values
  {:yokozuna 1
   :ozeki 2
   :sekiwake 3
   :komusubi 4})

(def lowest-sanyaku (:komusubi sanyaku-rank-values))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Parse out a rank string's keyword and number
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rank-keyword
  "for a given rank string, return its rank keyword"
  [rank_str] ; "Maegashira #15" -> :maegashira, "Ozeki -> :ozeki"
  (keyword
   (clojure.string/lower-case
    (first
     (clojure.string/split rank_str #" ")))))

(defn get-rank-and-file-number
  "for a given 'rank and file' rikishi rank string,
   this means either Maegashira or Juryo,
   returns their associated number"
  [rank_str] ; "Maegashira #15" -> 15
  (let [number (last (clojure.string/split (clojure.string/trim rank_str) #"\#"))]
    (if (= rank_str number) ; ranks w/o number return nil
      nil
      (Integer/parseInt number))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Convert Rank string to a keyword
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn rank-str-to-keyword
  "given a rank string returns it as a keyword
   e.g. 'Maegashira #1' -> :maegashira_1"
  [rank-str]
  (keyword
    (clojure.string/lower-case
      (clojure.string/join 
        "_"
        (map ; maping a fn over a collection, not sure what the kondo error is talking about here
          clojure.string/trim
          (clojure.string/split rank-str #"\#"))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Convert Rank keyword to a string
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn rank-keyword-to-str
  "given a rank keyword returns it as a string
   e.g. :maegashira_1 -> 'Maegashira #1'.
   reverse of rank-str-to-keyword"
  [rank-keyword]
  (if rank-keyword
    (clojure.string/capitalize
      (clojure.string/join 
        " #"
        (clojure.string/split
          (name rank-keyword) #"_")))
    nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get the lowest Maegashira/Juryo Rank in a tournament
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tournament-lowest-rank-and-file
  "for a given tournament, return the lowest maegashira or juryo
   rank value competing that we have data on.
   optional :level param takes either :maegashira or :juryo,
   defaults to :maegashira"
  [{:keys [month year level] :or {level :maegashira}}]
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
              (db/get-all-ranks-in-tournament {:month month :year year})))))
      nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Map of all rank keywords in tournament to their value.
;; Not hard-coded like sanyaku-rank-values because
;; juryo rank values depend on how many maegashira there are
;; in a given tournament.
;; e.g. with 15 maegashira, { ... :juryo_1 16 ... }
;; e.g. with 17 maegashira, { ... :juryo_1 18 ... }
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(defn write-tournament-rank-values
  "depending on the number of maegashira each tournament
   the juryo rank values differ slightly. run this function
   after all the tournament data has been loaded."
  [{:keys [year month]}]
  (println "writing tournament rank values for year:" year "month:" month)
  (let [bouts (db/get-bout-list
               {:year year
                :month month})
        ranks (tournament-rank-values
                {:year year
                 :month month})]
    (dorun
      (map
        (fn [bout]
          (db/update-bout
            (:id bout)
            [:west_rank_value ((rank-str-to-keyword (:west_rank bout)) ranks)]
            [:east_rank_value ((rank-str-to-keyword (:east_rank bout)) ranks)]))
       bouts))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get a Rank's value in a given tournament
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rank-value ; "Ozeki" => 2, "Maegashira #1" => 5, :maegashira_1 => 5
  "given a rank string or keyword, returns its value.
   if no tournament passed in, defaults to rank's
   value in latest tournament"
  [{:keys [rank year month]
    :or {year (:year (first (db/list-tournaments)))
         month (:month (first (db/list-tournaments)))}}]
   (let [rank-values (tournament-rank-values {:year year :month month})]
     ((if (keyword? rank) rank (rank-str-to-keyword rank)) rank-values)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get a Rikishi's rank string from a bout
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rank-string-in-bout
  "for a given bout hashmap and rikishi name string, 
   returns rikishi rank string if it exists,
   else returns nil"
  [{:keys [rikishi bout]}]
  (or
   (and (= (:east bout) (clojure.string/upper-case rikishi)) (:east_rank bout))
   (and (= (:west bout) (clojure.string/upper-case rikishi)) (:west_rank bout))
   nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get a Rikishi's rank value from a bout 
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rank-value-in-bout
  "given a rikishi and bout, return
   rikishi's rank value"
  [{:keys [rikishi bout]}] ; "takakeisho" { :east "takakeiso" :rank "ozeki" ... } -> 2
  (get-rank-value
    {:month (:month bout)
     :year (:year bout)
     :rank (get-rank-string-in-bout {:rikishi rikishi :bout bout})}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get Opponent's rank string from a bout
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-opponent-rank-string-in-bout
  "given a rikishi and bout, return
   rikishi opponent's rank value"
  [{:keys [rikishi bout]}]
  (get-rank-string-in-bout
    {:rikishi (utils/get-bout-opponent {:rikishi rikishi :bout bout})
     :bout bout}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get Opponent's rank value from a bout
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-opponent-rank-value-in-bout
  "given a rikishi and bout, return
   rikishi opponent's rank value"
  [{:keys [rikishi bout]}]
  (get-rank-value-in-bout
    {:rikishi (utils/get-bout-opponent {:rikishi rikishi :bout bout})
     :bout bout}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get a Rikishi's rank for a tournament
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rank-string-in-tournament
  "iterates over a list of tournaments,
   returns rank if found, else returns nil.
   given string 'TAKAKEISHO', returns string 'Ozeki'"
  [rikishi [bout & rest]] ; ["TAKAKEISHO" { ..bout } (list {...} {...} ...)]
  (if-let [rank (get-rank-string-in-bout {:rikishi rikishi :bout bout})]
    rank
    (if (empty? rest)
      nil ; list is done, return nil
      (get-rank-string-in-tournament rikishi rest)))) ; keep going

;;;;;;;;;;;;;;;;;;;;;;;;;
;; Current Rikishi Rank
;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rikishi-current-rank
  "gets most recent rank for passed in rikishi name
    given string 'hakuho' this is returned--
    {:rank 'Yokozuna', :tournament { :month 7, :year 2020 }}
    returns nil if no data exists for passed in rikishi"
  ([{:keys [rikishi]}] ; top-level
    (if (db/rikishi-exists? rikishi)
      (get-rikishi-current-rank
        (clojure.string/upper-case rikishi)
        (db/list-tournaments))
      {:error (str "Data does not exist for rikishi " rikishi)}))
  ([rikishi [tournament & rest]] ; inner function
    (if-let [rank ; rank from checking all bouts in a tournament
              (get-rank-string-in-tournament
                rikishi
                (db/get-bout-list
                  {:rikishi rikishi
                   :year (:year tournament)
                   :month (:month tournament)}))]
     {:rank rank :tournament tournament} ; rank found, we're done
     (get-rikishi-current-rank rikishi rest)))) ; rank not found, move to next tournament

;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rikishi Rank History 
;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rikishi-rank-over-time
  "returns rikishi rank over time, from as far back as data exists.
   everytime rikishi has new rank, that is included in the list.
   nil's represent tournaments rikishi did not compete in"
  ([{:keys [rikishi]}] ; top-level
    (if (db/rikishi-exists? rikishi)
      (get-rikishi-rank-over-time
        (clojure.string/upper-case rikishi)
        (reverse (db/list-tournaments))
        [])
      {:error (str "Data does not exist for rikishi " rikishi)}))
  ([rikishi [tournament & rest] rank-over-time] ; inner function
    (if (empty? tournament)
      rank-over-time ; checked every tournament, return what you've got
      (let [rank (get-rank-string-in-tournament
                   rikishi
                   (db/get-bout-list
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

;; TODO--
;; everything that uses rikishi-comparision is super slow
;; replace all those calls with just sql calls when possible
;; only use rikishi-comparison if it cant be accomplished in 
;; sql first

;;;;;;;;;;;;;;;;;;;;;;;
;; Bouts Rikishi Lost
;;;;;;;;;;;;;;;;;;;;;;;

(defn losses-to-rank
  "given a rikishi and rank string,
   return all bouts where rikishi lost.
   optionally takes in comparision function
   to specify rank relative to passed in rank"
  [{:keys [rikishi comparison against-rank year month day] :or {comparison =}}] ; "endo" >= "ozeki"
    (utils/rikishi-comparison ; all losses against ozeki or higher
      rikishi
      "lose" ; criteria is rikishi opponent is certain rank
      #(let [against-rank-value (get-rank-value {:rank against-rank :year (:year %) :month (:month %)})
             opponent-rank-value (get-opponent-rank-value-in-bout {:rikishi rikishi :bout %})]
        (if (and against-rank-value opponent-rank-value)
          (comparison against-rank-value opponent-rank-value)
          false)) ; error getting rank for a certain rikisi + bout, move on, TODO: log this
      '()
      (db/get-bout-list {:rikishi rikishi :year year :month month :day day})))

(defn losses-to-lower-rank
  "Given a rikishi name string and optional 'delta' and 'comparison' fn pair,
   Returns either all losses to lower ranks
   or losses according to delta and comparison function
   e.g. endo >= 2 is all losses to rikishi >= 2 ranks lower than endo
   e.g. endo = 2 all losses to rikishi = 2 ranks lower than endo"
  [{:keys [rikishi comparison delta year month day] :or {comparison <= delta ##Inf}}]
    (utils/rikishi-comparison ; :comparison can be >, >=, =, <, <=
      rikishi
      "lose"
      #(let [rikishi-rank-value (get-rank-value-in-bout {:rikishi rikishi :bout %})
             opponent-rank-value (get-opponent-rank-value-in-bout {:rikishi rikishi :bout %})]
        (if (and rikishi-rank-value opponent-rank-value)
          (and ; opponent rank is lower and delta satisfies given comparision and delta
            (> opponent-rank-value rikishi-rank-value)
            (comparison (- opponent-rank-value rikishi-rank-value) delta))
          false)) ; error getting rank for a certain rikisi + bout, move on, TODO: log this
      '()
      (db/get-bout-list {:rikishi rikishi :year year :month month :day day})))

;;;;;;;;;;;;;;;;;;;;;;;
;; Bouts Rikishi Won
;;;;;;;;;;;;;;;;;;;;;;;

(defn wins-vs-rank
  "given a rikishi and rank string or keyword,
   return all bouts where rikishi won.
   optionally takes in comparison function
   to specify wins relative to passed in rank"
  [{:keys [rikishi comparison against-rank year month day] :or {comparison =}}] ; "endo" >= "ozeki"
    (utils/rikishi-comparison ; all wins against ozeki or higher
      rikishi
      "win" ; criteria: rikishi opponent is certain rank
      #(let [against-rank-value (get-rank-value {:rank against-rank :year (:year %) :month (:month %)})
             opponent-rank-value (get-opponent-rank-value-in-bout {:rikishi rikishi :bout %})]
        (if (and against-rank-value opponent-rank-value)
          (comparison against-rank-value opponent-rank-value)
          false)) ; error getting rank for a certain rikisi + bout, move on, TODO: log this
      '()
      (db/get-bout-list {:rikishi rikishi :year year :month month :day day})))

(defn wins-vs-higher-rank
  "Given a rikishi name string and optional 'delta' and 'comparison' fn pair,
   Returns either all wins against higher ranks,
   or wins according to delta and comparison fn.
   e.g. endo >= 2 is all losses to rikishi >= 2 ranks higher than endo
   e.g. endo = 2 is all losses to rikishi = 2 ranks higher than endo"
  [{:keys [rikishi comparison delta year month day] :or {comparison <= delta ##Inf}}]
    (utils/rikishi-comparison ; :comparison can be >, >=, =, <, <=
      rikishi
      "win"
      #(let [rikishi-rank-value (get-rank-value-in-bout {:rikishi rikishi :bout %})
             opponent-rank-value (get-opponent-rank-value-in-bout {:rikishi rikishi :bout %})]
        (if (and rikishi-rank-value opponent-rank-value)
          (and ; rikishi rank is higher and delta satisfies given comparision and delta
            (< opponent-rank-value rikishi-rank-value)
            (comparison (- rikishi-rank-value opponent-rank-value) delta))
          false)) ; error getting rank for a certain rikisi + bout, move on, TODO: log this
      '()
      (db/get-bout-list {:rikishi rikishi :year year :month month :day day})))
