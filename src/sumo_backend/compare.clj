(ns sumo-backend.compare)
(require '[sumo-backend.mysql :as db])
(require '[sumo-backend.utils :as utils])

;; Note on this namespace:
;; Similar to how get-rikishi-losses-to-opponent
;; and get-rikishi-wins-against-opponent
;; take a more optimized list of bouts to parse through,
;; see if other functions here can take a more
;; optimized list of bouts to parse through.
;; currently, most of these functions parse through
;; every bout, looking for their success-criteria.
;; that will get slow as more data is added.

;; Note on ranks
;; Komusubi, Maegashira, and Juryo are not the same
;; Difference between them is the Maegashira's number
;; Or all Maegashira numbers + the Juryo number
;; e.g. Maegashira #1 is one rank away from Komusubi

(def ranks
  {:yokozuna 1
   :ozeki 2
   :sekiwake 3
   :komusubi 4
   :maegashira 4 ; Maegashira #1, Maegashira #2, ...
   :juryo 4 })   ; Juryo #1, Juryo #2, ...

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Compare Rank Strings
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn compare-rank
  "given two rank strings and tournament date
    return the higher ranked of the 2
    and by how many steps it is higher.
    equal ranks return 'same' with :delta 0"
  ([rank-a-str rank-b-str] ; "yokozunza" "juryo #1" -> {:high-rank "yokozuna", :delta 21}
   (let [recent-tournament (first (db/list-bouts))]
     (compare-rank
      rank-a-str
      rank-b-str
      (:month recent-tournament)
      (:year recent-tournament))))
  ([rank-a-str rank-b-str month year] ; "Maegashira #15", "Ozeki", 3, 2019
   (let [rank-a-keyword (utils/get-rank-keyword rank-a-str)
         rank-b-keyword (utils/get-rank-keyword rank-b-str)]
     (if (or (nil? (rank-a-keyword ranks)) (nil? (rank-b-keyword ranks)))
       nil ; invalid rank passed, nil returned
       (let [rank-a-value (utils/get-rank-value rank-a-str month year)
             rank-b-value (utils/get-rank-value rank-b-str month year)]
         (if (< rank-a-value rank-b-value)
           {:high-rank rank-a-str :delta (- rank-b-value rank-a-value)}
           (if (< rank-b-value rank-a-value)
             {:high-rank rank-b-str :delta (- rank-a-value rank-b-value)}
             {:high-rank "same" :delta 0})))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Compare Rikishi bout history according to passed in function
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- rikishi-comparison
  "for a given rikishi
   process all bout data, comparing
   wins/losses according to passed in 
   success-criteria function"
  [rikishi outcome success-criteria results [bout & rest]] ; "endo" "lose" ...
  (if (nil? bout) ; no more bouts, return results
    results
    (if (and
          (utils/rikishi-win-or-lose-bout rikishi outcome bout)
          (success-criteria bout))
      (rikishi-comparison ; save this bout and continue
        rikishi
        outcome
        success-criteria
        (conj results bout)
        rest)
      (rikishi-comparison ; move on, dont save this bout
        rikishi
        outcome
        success-criteria
        results
        rest))))

;;;;;;;;;;;;;;;;;;;;;;;
;; Bouts Rikishi Lost
;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rikishi-losses-to-lower-rank
  "given a rikishi name string and
   optional 'delta' and 'comparison' function pair,
   Returns either all losses to lower ranks
   or losses according to delta and comparison function"
  ([rikishi] ; all losses to lower rank
   (rikishi-comparison
    rikishi
    "lose"
    #(let [rikishi-rank-value ; % is bout
            (utils/get-rank-value-in-bout rikishi %)
           opponent-rank-value 
            (utils/get-opponent-rank-value-in-bout rikishi %)]
      (and ; opponent rank is lower & rank delta less than infinity aka everything
       (>= opponent-rank-value rikishi-rank-value)
       (< (- opponent-rank-value rikishi-rank-value) ##Inf)))
    '()
    (db/get-bouts-by-rikishi rikishi)))
  ([rikishi comparison delta] ; >, >=, =, <, <=, any comparison
   (rikishi-comparison ; ["endo" >= 2] all losses to rikishi >= 2 ranks lower than endo
    rikishi            ; ["endo" = 2] all losses to rikishi = 2 ranks lower than endo
    "lose"
    #(let [rikishi-rank-value
            (utils/get-rank-value-in-bout rikishi %)
           opponent-rank-value
            (utils/get-opponent-rank-value-in-bout rikishi %)]
      (and ; opponent rank is lower and delta satisfies given comparision and delta
       (>= opponent-rank-value rikishi-rank-value)
       (comparison (- opponent-rank-value rikishi-rank-value) delta)))
    '() 
    (db/get-bouts-by-rikishi rikishi))))

(defn get-rikishi-losses-to-higher-rank
  "given a rikishi name string and
   optional 'delta' and 'comparison' fn pair,
   Returns either all losses to higher ranks
   or losses according to delta and comparison fn"
  ([rikishi]
    (rikishi-comparison ; all losses to higher rank
      rikishi
      "lose"
      #(let [rikishi-rank-value
              (utils/get-rank-value-in-bout rikishi %)
             opponent-rank-value
              (utils/get-opponent-rank-value-in-bout rikishi %)]
        (and
          (<= opponent-rank-value rikishi-rank-value)
          (< (- rikishi-rank-value opponent-rank-value) ##Inf)))
      '() 
      (db/get-bouts-by-rikishi rikishi)))
  ([rikishi comparison delta] ; >, >=, =, <, <=, any comparison
    (rikishi-comparison  ; ["endo" >= 2] all losses to rikishi >= 2 ranks higher than endo
      rikishi            ; ["endo" = 2]  all losses to rikishi = 2 ranks higher than endo
      "lose"
      #(let [rikishi-rank-value
              (utils/get-rank-value-in-bout rikishi %)
             opponent-rank-value
              (utils/get-opponent-rank-value-in-bout rikishi %)]
        (and
          (<= opponent-rank-value rikishi-rank-value)
          (comparison (- rikishi-rank-value opponent-rank-value) delta)))
      '() 
      (db/get-bouts-by-rikishi rikishi))))

(defn get-rikishi-losses-to-same-rank
  "given a rikishi name string,
   returns all losses to same rank"
  [rikishi]
    (rikishi-comparison
      rikishi
      "lose"
      #(let [rikishi-rank-value
              (utils/get-rank-value-in-bout rikishi %)
             opponent-rank-value
              (utils/get-opponent-rank-value-in-bout rikishi %)]
        (= rikishi-rank-value opponent-rank-value))
      '()
      (db/get-bouts-by-rikishi rikishi)))

(defn get-rikishi-losses-to-rank
  "given a rikishi and rank string,
   return all bouts where rikishi lost.
   optionally takes in comparision function
   to specify rank relative to passed in rank"
  ([rikishi rank-str] ; "endo" "ozeki"
   (rikishi-comparison
    rikishi
    "lose" ; criteria is rikishi opponent is certain rank
    #(let [opponent-rank
            (utils/get-opponent-rank-in-bout rikishi %)]
      (= (clojure.string/lower-case 
          (clojure.string/trim rank-str))
         (clojure.string/lower-case
          (clojure.string/trim opponent-rank))))
    '()
    (db/get-bouts-by-rikishi rikishi)))
  ([rikishi comparison rank-str] ; "endo" >= "ozeki"
   (rikishi-comparison ; all losses against ozeki or higher
    rikishi
    "lose" ; criteria is rikishi opponent is certain rank
    #(let [rank-str-value
            (utils/get-rank-value rank-str)
           opponent-rank-value
            (utils/get-opponent-rank-value-in-bout rikishi %)]
      (comparison rank-str-value opponent-rank-value))
    '()
    (db/get-bouts-by-rikishi rikishi))))

; un-necessary to pass in "lose" and
; success criteria function also looking for "lose"... re-visit later
(defn get-rikishi-losses-to-opponent
  "given a rikishi name and opponent name
   return all bouts where rikishi lost"
  [rikishi opponent]
   (rikishi-comparison
    rikishi
    "lose"
    #(utils/rikishi-win-or-lose-bout rikishi "lose" %)
    '()
    (db/rikishi-bout-history rikishi opponent)))
  
;;;;;;;;;;;;;;;;;;;;;;;
;; Bouts Rikishi Won
;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rikishi-wins-against-higher-rank
  "given a rikishi name string and
   optional 'delta' and 'comparison' fn pair,
   Returns either all wins against higher ranks
   or wins according to delta and comparison fn"
  ([rikishi]
    (rikishi-comparison ; all losses to higher rank
      rikishi 
      "win"
      #(let [rikishi-rank-value
              (utils/get-rank-value-in-bout rikishi %)
             opponent-rank-value
              (utils/get-opponent-rank-value-in-bout rikishi %)]
        (and
          (<= opponent-rank-value rikishi-rank-value)
          (< (- rikishi-rank-value opponent-rank-value) ##Inf)))
      '() 
      (db/get-bouts-by-rikishi rikishi)))
  ([rikishi comparison delta] ; >, >=, =, <, <=, any comparison
    (rikishi-comparison  ; ["endo" >= 2] all losses to rikishi >= 2 ranks higher than endo
      rikishi            ; ["endo" = 2]  all losses to rikishi = 2 ranks higher than endo
      "win"
      #(let [rikishi-rank-value
             (utils/get-rank-value-in-bout rikishi %)
             opponent-rank-value
             (utils/get-opponent-rank-value-in-bout rikishi %)]
        (and
          (<= opponent-rank-value rikishi-rank-value)
          (comparison (- rikishi-rank-value opponent-rank-value) delta)))
      '() 
      (db/get-bouts-by-rikishi rikishi))))

(defn get-rikishi-wins-against-lower-rank
  "given a rikishi name string and
   optional 'delta' and 'comparison' fn pair,
   Returns either all wins against lower ranks
   or wins according to delta and comparison fn"
  ([rikishi]
    (rikishi-comparison ; all losses to higher rank
      rikishi 
      "win"
      #(let [rikishi-rank-value
              (utils/get-rank-value-in-bout rikishi %)
             opponent-rank-value
              (utils/get-opponent-rank-value-in-bout rikishi %)]
        (and
          (>= opponent-rank-value rikishi-rank-value) ; yokozuna is rank value 1
          (< (- opponent-rank-value rikishi-rank-value) ##Inf)))
      '() 
      (db/get-bouts-by-rikishi rikishi)))
  ([rikishi comparison delta] ; >, >=, =, <, <=, any comparison
    (rikishi-comparison  ; ["endo" >= 2] all losses to rikishi >= 2 ranks higher than endo
      rikishi            ; ["endo" = 2]  all losses to rikishi = 2 ranks higher than endo
      "win"
      #(let [rikishi-rank-value
              (utils/get-rank-value-in-bout rikishi %)
             opponent-rank-value
              (utils/get-opponent-rank-value-in-bout rikishi %)]
        (and
          (>= opponent-rank-value rikishi-rank-value) ; yokozuna is rank value 1
          (comparison (- opponent-rank-value rikishi-rank-value) delta)))
      '() 
      (db/get-bouts-by-rikishi rikishi))))

(defn get-rikishi-wins-against-same-rank
  "given a rikishi name string,
   returns all wins against same rank"
  [rikishi]
    (rikishi-comparison
      rikishi
      "win"
      #(let [rikishi-rank-value
             (utils/get-rank-value-in-bout rikishi %)
             opponent-rank-value
             (utils/get-opponent-rank-value-in-bout rikishi %)]
        (= rikishi-rank-value opponent-rank-value))
      '()
      (db/get-bouts-by-rikishi rikishi)))

(defn get-rikishi-wins-against-rank
  "given a rikishi and rank string,
   return all bouts where rikishi won.
   optionally takes in comparison function
   to specify wins relative to passed in rank"
  ([rikishi rank-str] ; "endo" "ozeki"
   (rikishi-comparison
    rikishi
    "win" ; criteria: rikishi opponent is certain rank
    #(let [opponent-rank
           (utils/get-opponent-rank-in-bout rikishi %)]
       (= (clojure.string/lower-case
           (clojure.string/trim rank-str))
          (clojure.string/lower-case
           (clojure.string/trim opponent-rank))))
    '()
    (db/get-bouts-by-rikishi rikishi)))
  ([rikishi comparison rank-str] ; "endo" >= "ozeki"
   (rikishi-comparison ; all wins against ozeki or higher
    rikishi
    "win" ; criteria: rikishi opponent is certain rank
    #(let [rank-str-value
            (utils/get-rank-value rank-str)
           opponent-rank-value
            (utils/get-opponent-rank-value-in-bout rikishi %)]
       (comparison rank-str-value opponent-rank-value))
    '()
    (db/get-bouts-by-rikishi rikishi))))

; un-necessary to pass in "win" and
; success criteria function also looking for "win"... re-visit later
(defn get-rikishi-wins-against-opponent
  "given a rikishi name and opponent name
   return all bouts where rikishi won"
  [rikishi opponent]
  (rikishi-comparison
   rikishi
   "win"
   #(utils/rikishi-win-or-lose-bout rikishi "win" %)
   '()
   (db/rikishi-bout-history rikishi opponent)))

