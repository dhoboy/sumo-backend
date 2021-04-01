(ns sumo-backend.compare)
(require '[sumo-backend.mysql :as db])
(require '[sumo-backend.utils :as utils])

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
   success-criteria function" ; make this only take bout to be more generic
  [rikishi outcome success-criteria results [bout & rest]] ; "endo" "loose" ...
  (if (nil? bout) ; no more bouts, return results
    results
    (let [rikishi-rank-value  (utils/get-rank-value-in-bout rikishi bout)
          opponent-rank-value (utils/get-rank-value-in-bout (utils/get-bout-opponent rikishi bout) bout)]
      (if (and
           (utils/rikishi-win-or-loose-bout rikishi outcome bout)
           (success-criteria rikishi-rank-value opponent-rank-value))
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
          rest)))))

(defn- rikishi-comparison-2
  "for a given rikishi
   process all bout data, comparing
   wins/losses according to passed in 
   success-criteria function"
  [rikishi outcome success-criteria results [bout & rest]] ; "endo" "loose" ...
  (if (nil? bout) ; no more bouts, return results
    results
    (if (and
          (utils/rikishi-win-or-loose-bout rikishi outcome bout)
          (success-criteria bout))
      (rikishi-comparison-2 ; save this bout and continue
        rikishi
        outcome
        success-criteria
        (conj results bout)
        rest)
      (rikishi-comparison-2 ; move on, dont save this bout
        rikishi
        outcome
        success-criteria
        results
        rest))))

;;;;;;;;;;;;;;;;;;;;;;;
;; Bouts Rikishi Lost
;;;;;;;;;;;;;;;;;;;;;;;

; using more generic success-criteria function that only takes bout as an argument
; will switch all of these to use this generic success-criteria function
(defn get-rikishi-losses-to-lower-rank-2
  "given a rikishi name string and
   optional 'delta' and 'comparison' fn pair,
   Returns either all losses to lower ranks
   or losses according to delta and comparison fn"
  ([rikishi] ; all losses to lower rank
   (rikishi-comparison-2
    rikishi
    "loose"
    #(let [rikishi-rank-value ; % is bout
            (utils/get-rank-value-in-bout rikishi %)
           opponent-rank-value 
            (utils/get-rank-value-in-bout 
             (utils/get-bout-opponent rikishi %) %)]
      (and ; opponent rank is lower and rank delta less than infinity aka everything
       (>= opponent-rank-value rikishi-rank-value)
       (< (- opponent-rank-value rikishi-rank-value) ##Inf))) 
    '()
    (db/get-bouts-by-rikishi rikishi)))
  ([rikishi comparison delta] ; >, >=, =, <, <=, any comparison
   (rikishi-comparison-2 ; ["endo" >= 2] all losses to rikishi >= 2 ranks lower than endo
    rikishi              ; ["endo" = 2] all losses to rikishi = 2 ranks lower than endo
    "loose"
    #(let [rikishi-rank-value
            (utils/get-rank-value-in-bout rikishi %)
           opponent-rank-value
            (utils/get-rank-value-in-bout
             (utils/get-bout-opponent rikishi %) %)]
      (and ; opponent rank is lower and delta satisfies given comparision and delta
       (>= opponent-rank-value rikishi-rank-value)
       (comparison (- opponent-rank-value rikishi-rank-value) delta)))
    '() 
    (db/get-bouts-by-rikishi rikishi))))

(defn get-rikishi-losses-to-lower-rank
  "given a rikishi name string and
   optional 'delta' and 'comparison' fn pair,
   Returns either all losses to lower ranks
   or losses according to delta and comparison fn"
  ([rikishi] ; all losses to lower rank
    (rikishi-comparison 
      rikishi
      "loose"
      #(and                  ; %2 is opponent rank value, %1 is rikishi rank value
        (>= %2 %1)           ; opponent rank is lower (yokozuna is rank value 1)
        (< (- %2 %1) ##Inf)) ; rank delta less than infinity aka everything
      '()
      (db/get-bouts-by-rikishi rikishi)))
  ([rikishi delta comparison] ; >, >=, =, <, <=, any comparison
    (rikishi-comparison ; ["endo" 2 >=] all losses to rikishi >= 2 ranks lower than endo
      rikishi           ; ["endo" 2 =] all losses to rikishi = 2 ranks lower than endo
      "loose"
      #(and
        (>= %2 %1)                    ; opponent rank is lower (yokozuna is rank value 1)
        (comparison (- %2 %1) delta)) ; rank delta satisfies passed in comparision and delta
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
      "loose"
      #(and
        (<= %2 %1) ; yokozuna is rank value 1
        (< (- %1 %2) ##Inf))
      '() 
      (db/get-bouts-by-rikishi rikishi)))
  ([rikishi delta comparison] ; >, >=, =, <, <=, any comparison
    (rikishi-comparison  ; ["endo" 2 >=] all losses to rikishi >= 2 ranks higher than endo
      rikishi            ; ["endo" 2 =]  all losses to rikishi = 2 ranks higher than endo
      "loose"
      #(and
        (<= %2 %1) ; yokozuna is rank value 1
        (comparison (- %1 %2) delta))
      '() 
      (db/get-bouts-by-rikishi rikishi))))

(defn get-rikishi-losses-to-same-rank
  "given a rikishi name string,
   returns all losses to same rank"
  [rikishi]
    (rikishi-comparison
      rikishi
      "loose"
      #(= %1 %2)
      '()
      (db/get-bouts-by-rikishi rikishi)))

;; (defn get-rikishi-losses-to-rank
;;   "given a rikishi and rank string,
;;    return all bouts where rikishi lost"
;;   [rikishi rank-str] ; "endo" "ozeki"
;;   (rikishi-comparison
;;    rikishi
;;    "lose")) ; in progress...
  
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
      #(and
        (<= %2 %1) ; yokozuna is rank value 1
        (< (- %1 %2) ##Inf))
      '() 
      (db/get-bouts-by-rikishi rikishi)))
  ([rikishi delta comparison] ; >, >=, =, <, <=, any comparison
    (rikishi-comparison  ; ["endo" 2 >=] all losses to rikishi >= 2 ranks higher than endo
      rikishi            ; ["endo" 2 =]  all losses to rikishi = 2 ranks higher than endo
      "win"
      #(and
        (<= %2 %1) ; yokozuna is rank value 1
        (comparison (- %1 %2) delta))
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
      #(and
        (>= %2 %1) ; yokozuna is rank value 1
        (< (- %2 %1) ##Inf))
      '() 
      (db/get-bouts-by-rikishi rikishi)))
  ([rikishi delta comparison] ; >, >=, =, <, <=, any comparison
    (rikishi-comparison  ; ["endo" 2 >=] all losses to rikishi >= 2 ranks higher than endo
      rikishi            ; ["endo" 2 =]  all losses to rikishi = 2 ranks higher than endo
      "win"
      #(and
        (>= %2 %1) ; yokozuna is rank value 1
        (comparison (- %2 %1) delta))
      '() 
      (db/get-bouts-by-rikishi rikishi))))

(defn get-rikishi-wins-against-same-rank
  "given a rikishi name string,
   returns all wins against same rank"
  [rikishi]
    (rikishi-comparison
      rikishi
      "win"
      #(= %1 %2)
      '()
      (db/get-bouts-by-rikishi rikishi)))



; next up in comparison is wins/losses to rank names
; all endo losses to ozeki, all endo wins against ozeki
; - potentially add rank to wins against same rank to
;   specify when endo was a sekiwake all the times he beat sekiwake
; - potentially add rank string param to wins/losses agaisnt
;   same rank so when endo was komusubi all the times he beat ozeki
;  see about pulling all this comparison stuff 
;  into its own namespace 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rikishi wins and looses to differnet ranks
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; coming soon! 
;; ex: give me all endo's losses to ozeki
;; ex: give me all endo's wins against ozeki or higher

;; give me all endo's losses to takakeisho
;; head to head matchups too, between 2 rikishi
