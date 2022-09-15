(ns sumo-backend.data.tournament-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing]]
    [sumo-backend.data.tournament :as tt]))


(deftest tournament
  (testing "list-tournaments"
    (is (=
          '(:month :year)
          (keys (first (tt/list-tournaments))))
      "Returns a list of maps with :month and :year keys"))


  (testing "get-wins-in-tournament"
    (let [resp (tt/get-wins-in-tournament
                 {:rikishi "endo" :year 2021 :month 7})]

      (is (= "ENDO" (:rikishi (first resp)))
        "Requested rikishi is returned")

      (is (= '(:rikishi :wins) (keys (first resp)))
        "Returns a list with a map containing :rikishi and :wins keys"))

    (is (=
          '()
          (tt/get-wins-in-tournament {}))
      "Returns empty list if :rikishi, :year, and :month are not passed in"))


  (testing "get-losses-in-tournament"
    (let [resp (tt/get-losses-in-tournament
                 {:rikishi "endo" :year 2021 :month 7})]

      (is (= "ENDO" (:rikishi (first resp)))
        "Requested rikishi is returned")

      (is (= '(:rikishi :losses) (keys (first resp)))
        "Returns a list with a map containing :rikishi and :wins keys"))

    (is (=
          '()
          (tt/get-losses-in-tournament {}))
      "Returns empty list if :rikishi, :year, and :month are not passed in"))


  (testing "get-all-ranks-in-tournament"
    (let [resp (tt/get-all-ranks-in-tournament {:year 2021 :month 7})]

      (is (= true (and (set? resp) (> (count resp) 0)))
        "Returns a non-empty set for a given tournament :year and :month"))

    (is (=
          #{}
          (tt/get-all-ranks-in-tournament {}))
      "Returns empty set if :year and :month are not passed in"))


  (testing "get-rikishi-rank-in-tournament"
    (is (=
          '(:rank :rank_value)
          (keys
            (tt/get-rikishi-rank-in-tournament
              {:rikishi "endo" :year 2021 :month 7})))
      "Returns map with :rank and :rank_value keys")

    (is (=
          {:rank nil :rank_value nil}
          (tt/get-rikishi-rank-in-tournament
            {:rikishi "made-up-rikishi" :year 2021 :month 7}))
      "Returns {:rank nil :rank_value nil} if rikishi did not compete in tournament")

    (is (=
          {}
          (tt/get-rikishi-rank-in-tournament {}))
      "Returns empty map if :rikishi, :year, and :month are not passed in"))


  (testing "build-rikishi-tournament-records"
    (let [resp (tt/build-rikishi-tournament-records {:year 2021 :month 7})
          item (first resp)]

      (is
        (=
          '(:rikishi :results :rank :rank_value)
          (keys item))
        "Returns list of maps with :rikishi, :results, :rank, and :rank_value keys")

      (is
        (=
          '(:wins :losses :result)
          (keys (:results item)))
        "Returns :results map with :wins, :losses, :result keys"))

    (is
      (=
        '()
        (tt/build-rikishi-tournament-records {}))
      "Returns empty list if :year and :month are not passed in"))


  (testing "get-rikishi-results-over-time"
    (let [resp (tt/get-rikishi-results-over-time {:rikishi "endo"})
          item (first resp)]

      (is
        (=
          '(:year :month :results :rank :rank_value)
          (keys item))
        "Returns list of maps with :year :month :results :rank :rank_value keys")

      (is
        (=
          '(:wins :losses :result)
          (keys (:results item)))
        "Returns :results map with :wins, :losses, :result keys")))


  (testing "get-rank-keyword"
    (is
      (= :ozeki (tt/get-rank-keyword "Ozeki"))
      "Returns keyword of single word rank value")

    (is (= :maegashira (tt/get-rank-keyword "Maegashira #15"))
      "Returns keyword of multi word rank value")

    (is (= nil (tt/get-rank-keyword ""))
      "Returns nil for empty string input"))


  (testing "get-bout-opponent"
    (is (=
          "Sam"
          (tt/get-bout-opponent
            {:bout {:east "Bob" :west "Sam"} :rikishi "BoB"}))
      "Returns bout opponent; case irrevelent"))


  (testing "get-rank-and-file-number"
    (is (= 15 (tt/get-rank-and-file-number "Maegashira #15"))
      "Returns expected number for valid input")

    (is (= nil (tt/get-rank-and-file-number "Ozeki"))
      "Returns nil for ranks without a number portion")

    (is (= nil (tt/get-rank-and-file-number ""))
      "Returns nil for empty string input"))


  (testing "rank-str-to-keyword"
    (is (= :ozeki (tt/rank-str-to-keyword "ozeki"))
      "Returns expected rank keyword of passed in single word rank string")

    (is (= :maegashira_1 (tt/rank-str-to-keyword "Maegashira #1"))
      "Returns expected rank keyword of passed in multi word rank string")

    (is (= nil (tt/rank-str-to-keyword ""))
      "Returns nil for empty string input"))


  (testing "rank-keyword-to-str"
    (is (= "Ozeki" (tt/rank-keyword-to-str :ozeki))
      "Returns expected rank string of passed in single word rank keyword")

    (is (= "Maegashira #1" (tt/rank-keyword-to-str :maegashira_1))
      "Returns expected rank string of passed in multi word rank keyword")

    (is (= "" (tt/rank-keyword-to-str ""))
      "Returns nil for empty string input"))


  (testing "tournament-lowest-rank-and-file"
    (is (=
          true
          (number? (tt/tournament-lowest-rank-and-file {:year 2021 :month 7})))
      "Returns a number given required :year and :month params")

    (is (=
          true
          (number?
            (tt/tournament-lowest-rank-and-file
              {:year 2021 :month 7 :level :juryo})))
      "Returns a number given required :year and :month params, and optional :level param")

    (is (=
          nil
          (tt/tournament-lowest-rank-and-file {}))
      "Returns nil if required :year and :month are not passed in"))


  (testing "tournament-rank-values"
    (let [resp (tt/tournament-rank-values {:year 2021 :month 7})
          invalid-rank-keywords (filter
                                  #(nil? (tt/get-rank-value {:rank %}))
                                  (keys resp))]

      (is (=
            true
            (= 0 (count invalid-rank-keywords)))
        "Returns map of rank keywords")

      (is (=
            true
            (number? (reduce + (vals resp))))
        "Returns map of number values")

      (is (=
            {:yokozuna 1 :ozeki 2 :sekiwake 3 :komusubi 4}
            (tt/tournament-rank-values {}))
        "Returns map of sanyaku ranks to values when :year and :month are not passed in")))


  (testing "get-rank-value"
    (is (=
          2
          (tt/get-rank-value {:rank "ozeki"}))
      "String rank Ozeki returns expected value")

    (is (=
          2
          (tt/get-rank-value {:rank "Ozeki" :year 2021 :month 5}))
      "String rank Ozeki returns expected value given optional :year and :month params")

    (is (=
          2
          (tt/get-rank-value {:rank :ozeki}))
      ":ozeki keyword returns expected value")

    (is (=
          2
          (tt/get-rank-value {:rank :ozeki :year 2021 :month 5}))
      ":ozeki keyword returns expected value given optional :year and :month params")

    (is (=
          nil
          (tt/get-rank-value {}))
      "Returns nil if required :rank not passed in"))



  (testing "get-rank-string-in-bout"
    (let [bout {:technique_en "Slap down"
                :day 6
                :west_rank_value 1
                :west "KAKURYU"
                :winner "KAKURYU"
                :east_rank "Maegashira #3"
                :loser "SHODAI"
                :month 3
                :is_playoff nil
                :east_rank_value 7
                :east "SHODAI"
                :year 2019
                :technique "Hatakikomi"
                :id 1
                :west_rank "Yokozuna"
                :technique_category "dodge"}]

      (is (=
            "Yokozuna"
            (tt/get-rank-string-in-bout {:rikishi "KAKURYU" :bout bout}))
        "Returns expected rank string for west rikishi in given valid bout")

      (is (=
            "Maegashira #3"
            (tt/get-rank-string-in-bout {:rikishi "shodai" :bout bout}))
        "Returns expected rank string for east rikishi in given valid bout")

      (is (=
            nil
            (tt/get-rank-string-in-bout {:rikishi "bobby" :bout bout}))
        "Returns nil for :rikishi not in bout map")

      (is (=
            nil
            (tt/get-rank-string-in-bout {}))
        "Returns nil if required :rikishi and :bout params are not passed in")))


  (testing "get-rank-value-in-bout"
    (let  [bout {:technique_en "Slap down"
                 :day 6
                 :west_rank_value 1
                 :west "KAKURYU"
                 :winner "KAKURYU"
                 :east_rank "Maegashira #3"
                 :loser "SHODAI"
                 :month 3
                 :is_playoff nil
                 :east_rank_value 7
                 :east "SHODAI"
                 :year 2019
                 :technique "Hatakikomi"
                 :id 1
                 :west_rank "Yokozuna"
                 :technique_category "dodge"}]

      (is (=
            1
            (tt/get-rank-value-in-bout {:rikishi "kakuryu" :bout bout}))
        "Returns expected rank value for west rikishi in given valid bout")

      (is (=
            7
            (tt/get-rank-value-in-bout {:rikishi "shodai" :bout bout}))
        "Returns expected rank value for east rikishi in given valid bout")

      (is (=
            nil
            (tt/get-rank-value-in-bout {:rikishi "bobby" :bout bout}))
        "Returns nil for :rikishi not in bout map")

      (is (=
            nil
            (tt/get-rank-value-in-bout {}))
        "Returns nil if required :rikishi and :bout params are not passed in")))

  (testing "get-opponent-rank-string-in-bout"
    (let [bout {:technique_en "Slap down"
                :day 6
                :west_rank_value 1
                :west "KAKURYU"
                :winner "KAKURYU"
                :east_rank "Maegashira #3"
                :loser "SHODAI"
                :month 3
                :is_playoff nil
                :east_rank_value 7
                :east "SHODAI"
                :year 2019
                :technique "Hatakikomi"
                :id 1
                :west_rank "Yokozuna"
                :technique_category "dodge"}]

      (is (=
            "yokozuna"
            (str/lower-case
              (tt/get-opponent-rank-string-in-bout {:rikishi "Shodai" :bout bout})))
        "Returns expected opponent rank value in bout map")

      (is (=
            nil
            (tt/get-opponent-rank-string-in-bout {:rikishi "" :bout bout}))
        "Returns nil for invalid rikishi in bout")))


  (testing "get-rank-string-in-tournament"
    (let [tournament [{:technique_en "Frontal force out"
                       :day 6
                       :west_rank_value 7
                       :west "NISHIKIGI"
                       :winner "HAKUHO"
                       :east_rank "Yokozuna"
                       :loser "NISHIKIGI"
                       :month 3
                       :is_playoff nil
                       :east_rank_value 1
                       :east "HAKUHO"
                       :year 2019
                       :technique "Yorikiri"
                       :id 2
                       :west_rank "Maegashira #3"
                       :technique_category "force"}
                      {:technique_en "Over arm throw"
                       :day 6
                       :west_rank_value 4
                       :west "MITAKEUMI"
                       :winner "TOCHINOSHIN"
                       :east_rank "Ozeki"
                       :loser "MITAKEUMI"
                       :month 3
                       :is_playoff nil
                       :east_rank_value 2
                       :east "TOCHINOSHIN"
                       :year 2019
                       :technique "Uwatenage"
                       :id 3
                       :west_rank "Komusubi"
                       :technique_category nil}]]

      (is (=
            "Yokozuna"
            (tt/get-rank-string-in-tournament "Hakuho" tournament))
        "Returns expected rank string for rikishi in the tournament")

      (is (=
            nil
            (tt/get-rank-string-in-tournament "Endo" tournament))
        "Returns nil for rikishi not in tournament")

      (is (=
            nil
            (tt/get-rank-string-in-tournament #{} tournament))
        "Returns nil for invalid rikishi name")

      (is (=
            nil
            (tt/get-rank-string-in-tournament "Hakuho" nil))
        "Returns nil for invalid tournament")))


  (testing "get-rikishi-current-rank"
    (let [resp (tt/get-rikishi-current-rank {:rikishi "Hakuho"})]
      (is (=
            '(:tournament :rank :rank_value)
            (keys resp))
        "Returns as expected for valid rikishi")

      (is (=
            '(:month :year)
            (keys (:tournament resp)))
        "Returns tournament map with :month and :year keys"))

    (is (=
          {:error "Data does not exist for rikishi Frank"}
          (tt/get-rikishi-current-rank {:rikishi "Frank"}))
      "Returns error message for invalid rikishi")

    (is (=
          {:error "Data does not exist for rikishi "}
          (tt/get-rikishi-current-rank {:rikishi nil}))
      "Returns error message for invalid rikishi"))


  (testing "get-rikishi-rank-over-time"
    (let [resp (tt/get-rikishi-rank-over-time {:rikishi "endo"})
          entry (first resp)]

      (is (=
            '(:rank :tournament :rank_value)
            (keys entry))
        "Returns a list of maps with :rank and :tournament keys")

      (is (=
            '(:month :year)
            (keys (:tournament entry)))
        "Tournament map in returned list of maps contains :month and :year keys")

      (is (=
            {:error "Data does not exist for rikishi "}
            (tt/get-rikishi-rank-over-time {}))
        "Returns error message for invalid rikishi"))))
