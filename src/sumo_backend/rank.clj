(ns sumo-backend.rank)(ns sumo-backend.rank)
(require '[sumo-backend.mysql :as db])

;; essentially a re-do of compare.clj

;; Working on simplifying rank logic
;; biggest issue is that there are a different
;; number of maegashira each basho
;; and the first juryo's rank depends
;; on how many maegashira there are per basho.

;; Idea--
;; could have a base map like ranks { ... }
;; and make a version for each basho, with 
;; rank values down to the bottom of juryo
;; so there'd be (ranks-<year>-<month>-<day> {...})
;; one per tournament, to get that tournament 
;; by tournament rank number exactly right
;; bouts are identified by their year.month.day
;; anyway, so there is no reasoning about rank
;; or a bout without that info

;; Note on ranks--
;; Komusubi, Maegashira, and Juryo are not the same
;; Difference between them is the Maegashira's number
;; Or all Maegashira numbers + the Juryo number
;; e.g. Maegashira #1 is one rank away from Komusubi

(def ranks
  { :yokozuna 1
    :ozeki 2
    :sekiwake 3
    :komusubi 4
    :maegashira 4 ; Maegashira #1, Maegashira #2, ...
    :juryo 4 })   ; Juryo #1, Juryo #2, ...

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get the lowest Maegashira Rank in a tournamnet
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-lowest-maegashira-rank-in-tournament
  "for a given tournament, return
   the lowest maegashira rank number competing"
  [month year] ; 3 2019 -> 17
  (first
    (sort >
      (filter number?
        (map
          #(let [word (get-rank-keyword %)
                 number (get-rank-and-file-number %)]
              (and (= word :maegashira) number))
          (db/get-all-ranks-in-tournament month year))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get a Rank's numeric value
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rank-value ; "Ozeki" => 2, "Maegashira #1" => 5
  "given a rank string, returns its value"
  ([rank-str]
   (let [recent-tournament (first (db/list-bouts))]
     (get-rank-value rank-str (:month recent-tournament) (:year recent-tournament))))
  ([rank-str month year]
   (let [rank-keyword (get-rank-keyword rank-str)]
     (if (or
          (= rank-keyword :maegashira)
          (= rank-keyword :juryo))
       (let [rank-number (get-rank-and-file-number rank-str)
             maegashira-limit (get-lowest-maegashira-rank-in-tournament month year)]
         (+ (rank-keyword ranks)
            rank-number
            (or (and (= rank-keyword :juryo) maegashira-limit) 0)))
       (rank-keyword ranks)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get a Rikishi's rank from a bout 
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rank-in-bout
  "for a given bout hashmap and rikishi name string, 
   returns rikishi rank if it exists
   else returns nil"
  [rikishi bout]
  (or
   (and (= (:east bout) (clojure.string/upper-case rikishi)) (:east_rank bout))
   (and (= (:west bout) (clojure.string/upper-case rikishi)) (:west_rank bout))
   nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get a Rikishi's rank value from a bout 
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rank-value-in-bout
  "given a rikishi and bout, return
   rikishi's rank value"
  [rikishi bout] ; "takakeisho" { :east "takakeiso" :rank "ozeki" ... } -> 2
  (get-rank-value
    (get-rank-in-bout rikishi bout)
    (:month bout)
    (:year bout)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get Opponent's rank from a bout
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-opponent-rank-in-bout
  "given a rikishi and bout, return
   rikishi opponent's rank value"
  [rikishi bout]
  (get-rank-in-bout
   (get-bout-opponent
    rikishi
    bout)
   bout))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get Opponent's rank value from a bout
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-opponent-rank-value-in-bout
  "given a rikishi and bout, return
   rikishi opponent's rank value"
  [rikishi bout]
  (get-rank-value-in-bout
    (get-bout-opponent
      rikishi
      bout)
    bout))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get a Rikishi's rank for a tournament
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

;;;;;;;;;;;;;;;;;;;;;;;;;
;; Current Rikishi Rank
;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rikishi-current-rank
  "gets most recent rank for passed in rikishi name
    given string 'hakuho' this is returned--
    {:rank 'Yokozuna', :tournament { :month 7, :year 2020 }}
    returns nil if no data exists for passed in rikishi"
  ([rikishi]
   (if (db/valid-rikishi? rikishi)
     (get-rikishi-current-rank (clojure.string/upper-case rikishi) (db/list-bouts))
     nil))
  ([rikishi [tournament & rest]]
   (if-let [rank ; rank from checking all bouts in a tournament
            (get-rank-in-tournament
             rikishi
             (db/get-bout-list
              {:rikishi rikishi :year (:year tournament) :month (:month tournament)}))]
     {:rank rank :tournament tournament} ; rank found, we're done
     (get-rikishi-current-rank rikishi rest)))) ; rank not found, move to next tournament

;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rikishi Rank History 
;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rikishi-rank-over-time
  "returns rikishi rank over time, from as far back as data exists.
   everytime rikishi has new rank, that is included in the list"
  ([rikishi]
   (if (db/valid-rikishi? rikishi)
     (get-rikishi-rank-over-time (clojure.string/upper-case rikishi) (reverse (db/list-bouts)) [])
     nil))
  ([rikishi [tournament & rest] rank-over-time]
   (if (empty? tournament)
     rank-over-time ; checked every tournament, return what you've got
     (let [rank (get-rank-in-tournament
                 rikishi
                 (db/get-bout-list {:rikishi rikishi :year (:year tournament) :month (:month tournament)}))]
       (if (not= rank (last (map #(get % :rank) rank-over-time)))
         (get-rikishi-rank-over-time
          rikishi
          rest
          (conj rank-over-time {:rank rank :tournament tournament}))
         (get-rikishi-rank-over-time
          rikishi
          rest
          rank-over-time))))))
