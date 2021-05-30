(ns sumo-backend.compare)

;; Comparison functions returning lists of bouts
;; running on top of (mysql/get-bout-list...) queries

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; scratch area 
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Compare Rank Strings
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; (defn compare-rank ; not sure this gets used anywhere so far...
;;   "given two rank strings and tournament date
;;     return the higher ranked of the 2
;;     and by how many steps it is higher.
;;     equal ranks return 'same' with :delta 0"
;;   ([rank-a-str rank-b-str] ; "yokozunza" "juryo #1" -> {:high-rank "yokozuna", :delta 21}
;;    (let [recent-tournament (first (db/list-bouts))]
;;      (compare-rank
;;       rank-a-str
;;       rank-b-str
;;       (:month recent-tournament)
;;       (:year recent-tournament))))
;;   ([rank-a-str rank-b-str month year] ; "Maegashira #15", "Ozeki", 3, 2019
;;    (let [rank-a-keyword (rank/get-rank-keyword rank-a-str)
;;          rank-b-keyword (rank/get-rank-keyword rank-b-str)]
;;      (if (or (nil? (rank-a-keyword utils/ranks)) (nil? (rank-b-keyword utils/ranks)))
;;        nil ; invalid rank passed, nil returned
;;        (let [rank-a-value (rank/get-rank-value {:rank rank-a-str :month month :year year})
;;              rank-b-value (rank/get-rank-value {:rank rank-b-str :month month :year year})]
;;          (if (< rank-a-value rank-b-value)
;;            {:high-rank rank-a-str :delta (- rank-b-value rank-a-value)}
;;            (if (< rank-b-value rank-a-value)
;;              {:high-rank rank-b-str :delta (- rank-a-value rank-b-value)}
;;              {:high-rank "same" :delta 0})))))))

;; (defn get-rikishi-losses-to-higher-rank
;;   "given a rikishi name string and
;;    optional 'delta' and 'comparison' fn pair,
;;    Returns either all losses to higher ranks
;;    or losses according to delta and comparison fn"
;;   ([rikishi]
;;     (rikishi-comparison ; all losses to higher rank
;;       rikishi
;;       "lose"
;;       #(let [rikishi-rank-value
;;               (utils/get-rank-value-in-bout {:rikishi rikishi :bout %})
;;              opponent-rank-value
;;               (utils/get-opponent-rank-value-in-bout rikishi %)]
;;         (and
;;           (<= opponent-rank-value rikishi-rank-value)
;;           (< (- rikishi-rank-value opponent-rank-value) ##Inf)))
;;       '() 
;;       (db/get-bout-list {:rikishi rikishi})))
;;   ([rikishi comparison delta] ; >, >=, =, <, <=, any comparison
;;     (rikishi-comparison  ; ["endo" >= 2] all losses to rikishi >= 2 ranks higher than endo
;;       rikishi            ; ["endo" = 2]  all losses to rikishi = 2 ranks higher than endo
;;       "lose"
;;       #(let [rikishi-rank-value
;;               (utils/get-rank-value-in-bout {:rikishi rikishi :bout %})
;;              opponent-rank-value
;;               (utils/get-opponent-rank-value-in-bout rikishi %)]
;;         (and
;;           (<= opponent-rank-value rikishi-rank-value)
;;           (comparison (- rikishi-rank-value opponent-rank-value) delta)))
;;       '() 
;;       (db/get-bout-list {:rikishi rikishi}))))

;; ; un-necessary to pass in "win" and
;; ; success criteria function also looking for "win"... re-visit later
;; (defn get-rikishi-wins-against-opponent
;;   "given a rikishi name and opponent name
;;    return all bouts where rikishi won"
;;   [rikishi opponent]
;;   (rikishi-comparison
;;    rikishi
;;    "win"
;;    #(utils/rikishi-win-or-lose-bout {:rikishi rikishi :outcome "win" :bout %})
;;    '()
;;    (db/get-bout-list {:rikishi rikishi :opponent opponent})))

;; (defn get-rikishi-losses-to-same-rank
;;   "given a rikishi name string,
;;    returns all losses to same rank"
;;   [rikishi]
;;   (rikishi-comparison
;;    rikishi
;;    "lose"
;;    #(let [rikishi-rank-value
;;           (utils/get-rank-value-in-bout {:rikishi rikishi :bout %})
;;           opponent-rank-value
;;           (utils/get-opponent-rank-value-in-bout rikishi %)]
;;       (= rikishi-rank-value opponent-rank-value))
;;    '()
;;    (db/get-bout-list {:rikishi rikishi})))


;; ; un-necessary to pass in "lose" and
;; ; success criteria function also looking for "lose"... re-visit later
;; (defn get-rikishi-losses-to-opponent
;;   "given a rikishi name and opponent name
;;    return all bouts where rikishi lost"
;;   [rikishi opponent]
;;   (rikishi-comparison
;;    rikishi
;;    "lose"
;;    #(utils/rikishi-win-or-lose-bout {:rikishi rikishi :outcome "lose" :bout %})
;;    '()
;;    (db/get-bout-list {:rikishi rikishi :opponent opponent})))

;; (defn get-rikishi-wins-against-lower-rank
;;   "given a rikishi name string and
;;    optional 'delta' and 'comparison' fn pair,
;;    Returns either all wins against lower ranks
;;    or wins according to delta and comparison fn"
;;   ([rikishi]
;;     (rikishi-comparison ; all losses to higher rank
;;       rikishi 
;;       "win"
;;       #(let [rikishi-rank-value
;;               (utils/get-rank-value-in-bout {:rikishi rikishi :bout %})
;;              opponent-rank-value
;;               (utils/get-opponent-rank-value-in-bout rikishi %)]
;;         (and
;;           (>= opponent-rank-value rikishi-rank-value) ; yokozuna is rank value 1
;;           (< (- opponent-rank-value rikishi-rank-value) ##Inf)))
;;       '() 
;;       (db/get-bout-list {:rikishi rikishi})))
;;   ([rikishi comparison delta] ; >, >=, =, <, <=, any comparison
;;     (rikishi-comparison  ; ["endo" >= 2] all losses to rikishi >= 2 ranks higher than endo
;;       rikishi            ; ["endo" = 2]  all losses to rikishi = 2 ranks higher than endo
;;       "win"
;;       #(let [rikishi-rank-value
;;               (utils/get-rank-value-in-bout {:rikishi rikishi :bout %})
;;              opponent-rank-value
;;               (utils/get-opponent-rank-value-in-bout rikishi %)]
;;         (and
;;           (>= opponent-rank-value rikishi-rank-value) ; yokozuna is rank value 1
;;           (comparison (- opponent-rank-value rikishi-rank-value) delta)))
;;       '() 
;;       (db/get-bout-list {:rikishi rikishi}))))

;; (defn get-rikishi-wins-against-same-rank
;;   "given a rikishi name string,
;;    returns all wins against same rank"
;;   [rikishi]
;;     (rikishi-comparison
;;       rikishi
;;       "win"
;;       #(let [rikishi-rank-value
;;              (utils/get-rank-value-in-bout {:rikishi rikishi :bout %})
;;              opponent-rank-value
;;              (utils/get-opponent-rank-value-in-bout rikishi %)]
;;         (= rikishi-rank-value opponent-rank-value))
;;       '()
;;       (db/get-bout-list {:rikishi rikishi})))