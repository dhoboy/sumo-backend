(ns sumo-backend.data.bout-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing]]
    [sumo-backend.data.bout :refer [get-bout-list]]
    [sumo-backend.utils :refer [in?]]
    [sumo-backend.data.tournament :refer [get-bout-opponent
                                          get-opponent-rank-string-in-bout
                                          get-rank-string-in-bout]]))


(def rikishi "ENDO")


;; Limit these test calls so the tests don't take too long to run
(def page "1")
(def per "10")


;; Bout namespace returns lists of bouts given various criteria.
;; Every test will be testing get-bout-list, the namespace's only function that
;; is used by other namespaces.

;; FYI: This bout list function is pretty brittle, you have to pass
;; certain params in combination with each other or it won't work.
;; It doesn't add any value to test against edge cases here, as anything unexpected
;; breaks this function; however, the handler functions that use this don't
;; let the user pass in param combos this function can't handle.
;; Getting expected result sets for given params here is a useful testing goal, and
;; this function could be re-written to be more robust, at which point edge-case
;; testing would make more sense.

(deftest bout
  (testing ":rikishi parameter"
    (let [resp (get-bout-list {:rikishi rikishi :page page :per per})]
      (is (=
            (count resp)
            (count
              (filter
                #(or
                   (= rikishi (str/upper-case (:east %)))
                   (= rikishi (str/upper-case (:west %))))
                resp)))
        "When passed a :rikishi, every returned bout contains that rikishi")))


  (testing ":winner parameter"
    (let [resp (get-bout-list {:winner rikishi :page page :per per})]
      (is (=
            (count resp)
            (count (filter #(= rikishi (str/upper-case (:winner %))) resp)))
        "When passed a :winner, every bout returned contains that rikishi as the winner")))


  (testing ":loser parameter"
    (let [resp (get-bout-list {:loser rikishi :page page :per per})]
      (is (=
            (count resp)
            (count (filter #(= rikishi (str/upper-case (:loser %))) resp)))
        "When passed a :loser, every bout returned contains that rikishi as the loser")))


  (testing ":technique parameter"
    (let [resp (get-bout-list {:technique "oshidashi" :page page :per per})]
      (is (=
            (count resp)
            (count
              (filter
                #(= "oshidashi" (str/lower-case (:technique %)))
                resp)))
        "When passed a :technique, every bout returned was using that technique")))


  (testing ":technique-category parameter"
    (let [resp (get-bout-list {:technique-category "force" :page page :per per})]
      (is (=
            (count resp)
            (count (filter #(= "force" (str/lower-case (:technique_category %))) resp)))
        "When passed a :technique-category, every bout returned is categorized with that category")))


  (testing ":is-playoff parameter"
    (let [resp (get-bout-list {:is-playoff true :page page :per per})]
      (is (=
            (count resp)
            (count (filter #(= true (:is_playoff %)) resp)))
        "When passed :is_playoff, every bout returned is a playoff bout")))


  (testing ":year parameter"
    (let [resp (get-bout-list {:year 2021 :page page :per per})]
      (is (=
            (count resp)
            (count (filter #(= 2021 (:year %)) resp)))
        "When passed :year, every bout returned is in that year")))


  (testing ":month parameter"
    (let [resp (get-bout-list {:month 7 :page page :per per})]
      (is (=
            (count resp)
            (count (filter #(= 7 (:month %)) resp)))
        "When passed :month, every bout returned is in that month")))


  (testing ":day parameter"
    (let [resp (get-bout-list {:day 15 :page page :per per})]
      (is (=
            (count resp)
            (count (filter #(= 15 (:day %)) resp)))
        "When passed :day, every bout returned is in that day")))


  (testing ":rank parameter"
    (let [resp (get-bout-list
                 {:rikishi rikishi :rank "Maegashira #1" :page page :per per})]
      (is (=
            (count resp)
            (count
              (filter
                #(=
                   "maegashira #1"
                   (str/trim
                     (str/lower-case
                       (get-rank-string-in-bout {:rikishi rikishi :bout %}))))
                resp)))
        "When passed :rikishi and :rank parameters, every bout returned has that rikishi at that rank")))

  (testing ":at-rank parameter"
    (let [resp (get-bout-list {:rikishi rikishi :at-rank "Komusubi" :against-rank "Ozeki"})]
      (is (=
            (count resp)
            (count
              (filter
                #(=
                   "komusubi"
                   (str/trim
                     (str/lower-case
                       (get-rank-string-in-bout {:rikishi rikishi :bout %}))))
                resp)))
        "When passed :at-rank parameter, every bout returned has rikishi at that rank")))


  (testing ":opponent and :opponent-rank parameters"
    (let [resp (get-bout-list
                 {:rikishi rikishi
                  :opponent "takakeisho"
                  :opponent-rank "ozeki"
                  :page page
                  :per per})]

      (is (=
            (count resp)
            (count
              (filter
                #(and
                   (= "takakeisho"
                     (str/lower-case
                       (get-bout-opponent {:rikishi rikishi :bout %})))
                   (= "ozeki"
                     (str/lower-case
                       (get-opponent-rank-string-in-bout {:rikishi rikishi :bout %}))))
                resp)))
        "When passed :opponent and :opponent-rank parameters, every bout returned is against that opponent at that rank")))


  (testing ":against-rank and :against-rank-value parameters"
    (let [resp (get-bout-list
                 {:rikishi rikishi
                  :against-rank "ozeki"
                  :against-rank-value 2})
          opponent-ranks (map
                           #(str/lower-case
                              (get-opponent-rank-string-in-bout
                                {:rikishi rikishi :bout %}))
                           resp)]

      (is (=
            (count resp)
            (count (filter #(= "ozeki" %) opponent-ranks)))
        "Every bout with :against-rank and :against-rank-value parameters passed is against expected rank")))


  (testing ":comparison < parameter"
    (let [resp (get-bout-list
                 {:rikishi rikishi
                  :against-rank "ozeki"
                  :against-rank-value 2
                  :comparison "<"})]
      (is (=
            (count resp)
            (count
              (filter
                #(=
                   "yokozuna"
                   (str/lower-case
                     (get-opponent-rank-string-in-bout {:rikishi rikishi :bout %})))
                resp)))
        "All bouts against < ozeki are against yokozuna")))


  (testing ":comparison <= parameter"
    (let [resp (get-bout-list
                 {:rikishi rikishi
                  :against-rank "ozeki"
                  :against-rank-value 2
                  :comparison "<="})
          opponent-ranks (map
                           #(str/lower-case
                              (get-opponent-rank-string-in-bout {:rikishi rikishi :bout %}))
                           resp)]

      (is (=
            (count resp)
            (count
              (filter
                #(in? #{"yokozuna" "ozeki"} %) ; #(or (= "yokozuna" %) (= "ozeki" %))
                opponent-ranks)))
        "All bouts against <= ozeki are against yokozuna or ozeki")))


  (testing ":comparison > parameter"
    (let [resp (get-bout-list
                 {:rikishi rikishi
                  :against-rank "ozeki"
                  :against-rank-value 2
                  :comparison ">"})
          opponent-ranks (map
                           #(str/lower-case
                              (get-opponent-rank-string-in-bout {:rikishi rikishi :bout %}))
                           resp)]

      (is (=
            (count resp)
            (count (filter #(not= "yokozuna" %) opponent-ranks)))
        "No bout against > ozeki can be against a yokozuna")))


  (testing ":comparison >= parameter"
    (let [resp (get-bout-list
                 {:rikishi rikishi
                  :against-rank "ozeki"
                  :against-rank-value 2
                  :comparison ">="})
          opponent-ranks (map
                           #(str/lower-case
                              (get-opponent-rank-string-in-bout
                                {:rikishi rikishi :bout %}))
                           resp)]

      (is (=
            true
            (and
              (=
                (count resp)
                (count (filter #(not= "yokozuna" %) opponent-ranks)))
              (some #(= "ozeki" (str/lower-case %)) opponent-ranks)))
        "Bouts against >= ozeki are not against yokozuna, but can be against ozeki")))


  (testing ":rank-delta parameter"
    (let [resp (get-bout-list {:winner rikishi :rank-delta 1})]
      (is (=
            (count resp)
            (count
              (filter
                #(=
                   1
                   (Math/abs (- (:east_rank_value %) (:west_rank_value %))))
                resp)))
        "Bouts where rikishi won and the difference between his and opponents rank level was 1"))))
