(ns sumo-backend.api.handler)
(require '[reitit.core :as r])
(require '[ring.logger :refer [wrap-with-logger]])
(require '[ring.middleware.json :refer [wrap-json-response]])
(require '[ring.middleware.params :refer :all])
(require '[ring.util.response :refer [response]])
(require '[jumblerg.middleware.cors :refer [wrap-cors]])
(require '[sumo-backend.utils :as utils])
(require '[sumo-backend.mysql :as db])
(require '[sumo-backend.rank :as rank])
(require '[sumo-backend.api.technique :as technique])
(require '[sumo-backend.api.tournament :as tournament])

  ;;
  ;; ******* Rikishi information *************
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list
  ;;

  (context ["/rikishi"] []

    ;; TODO--
    ;; Route for highest rank achieved, and tournaments
    ;; rikishi held that rank? could also be derived
    ;; from rikishi-rank-over-time

    ;; list of all rikishi
    (GET "/list" [page per])

    ;; specific rikishi record
    (GET "/details/:name" [name page per])


    ;; rikishi current rank
    ;; current rank is rank in last basho rikishi competed in
    (GET "/current_rank/:name" [name page per])

    ;; list of rikishi's rank changes over time
    (GET "/rank_over_time/:name" [name page per])

    ;; list of rikishi tournament results over time
    (GET "/results_over_time/:name" [name page per])

    ;; TODO -- add in groups of wins / losses occuring at each rank?
    ;; basic stats on techniques and categories rikishi wins / looses by
    (GET "/technique_breakdown/:name" [name year month day page per]))

  ;;
  ;; ****** Tournament information ***********
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list
  ;;

  (context ["/tournament"] []

    ;; list of all tournaments data exists for
    (GET "/list" [page per]
      (response
        (utils/paginate-list
          (merge
            {:item-list (db/list-tournaments)}
            (when page {:page page})
            (when per {:per per})
            (when (and (nil? page) (nil? per)) {:all true})))))

  ;; TODO -- add tournament champion and location.
  ;; details about tournament, e.g. rikishi records
  (GET "/details/:year/:month" [year month page per]
    (response
      (utils/paginate-list
        (merge
          {:item-list (tournament/build-rikishi-tournament-records {:year year :month month})}
          (when page {:page page})
          (when per {:per per})
          (when (and (nil? page) (nil? per)) {:all true}))))))

  ;; TODO -- Technique can be used on a separate Technique page...

  ;;
  ;; ******** Technique information **********
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list
  ;;

  (context "/technique" []

    ;; technique list
    ;; takes optional :year :month :day params
    ;; defaults to returning all sets of--
    ;; technique, technique_en, and technique_category
    ;; found in database, else returns them for
    ;; specified :year, :month, :day
    (GET "/list" [year month day page per]
      (response
        (utils/paginate-list
          (merge
            {:item-list (db/list-techniques {:year year :month month :day day})}
            (when page {:page page})
            (when per {:per per})
            (when (and (nil? page) (nil? per)) {:all true})))))

    ;; list of all categories and technique keys classified within
    (GET "/categories" [page per]
      (response
        (utils/paginate-list
          (merge
            {:item-list (technique/get-categories)}
            (when page {:page page})
            (when per {:per per})
            (when (and (nil? page) (nil? per)) {:all true})))))

    (comment
      (println (technique/get-categories)))

    ;; rikishi wins and losses by a certain technique
    ;; takes optional :year :month :day params
    (GET "/details/:technique" [technique year month day page per]
      (response
        (utils/paginate-list
          (merge
            {:item-list
             (conj
               []
               {:wins-by-technique
                (utils/add-percent-to-list
                  (db/get-all-wins-by-technique
                    {:technique technique
                     :year year
                     :month month
                     :day day}))}
               {:losses-to-technique
                (utils/add-percent-to-list
                  (db/get-all-losses-to-technique
                    {:technique technique
                     :year year
                     :month month
                     :day day}))})}
            (when page {:page page})
            (when per {:per per})
            (when (and (nil? page) (nil? per)) {:all true})))))

    (comment
      (conj
        []
        {:wins-by-technique
          (utils/add-percent-to-list
            (db/get-all-wins-by-technique
              {:technique "oshidashi"}))}
        {:losses-to-technique
          (utils/add-percent-to-list
            (db/get-all-losses-to-technique
              {:technique "oshidashi"}))}))

    ;; rikishi wins and losses by a certain technique category
    ;; takes optional :year :month :day params
    (GET "/category_details/:category" [category year month day page per]
      (response
        (utils/paginate-list
          (merge
            {:item-list
             (conj
               []
               {:rikishi-wins-by-technique-category
                (utils/add-percent-to-list
                  (db/get-all-wins-by-technique-category
                    {:category category
                     :year year
                     :month month
                     :day day}))}
               {:rikishi-losses-to-technique-category
                (utils/add-percent-to-list
                  (db/get-all-losses-to-technique-category
                    {:category category
                     :year year
                     :month month
                     :day day}))})}
            (when page {:page page})
            (when per {:per per})
            (when (and (nil? page) (nil? per)) {:all true})))))

    (comment
      (conj
               []
               {:rikishi-wins-by-technique-category
                (utils/add-percent-to-list
                  (db/get-all-wins-by-technique-category
                    {:category "force"}))}
               {:rikishi-losses-to-technique-category
                (helper/add-percent-to-list
                  (db/get-all-losses-to-technique-category
                    {:category "force"}))})))

  ;; TODO: Bout, Fare, and Upset can be used on the Matchups UI Page

  ;;
  ;; ********************* BOUT ****************************
  ;;   - Lists of bouts, against rikishi or by technique
  ;;   - "How does Endo do in general, or against Takakeisho?"
  ;;   - "How many wins by oshidashi does Endo have?"
  ;;
  ;;   - All routes take these optional params
  ;;      :winner => rikishi name
  ;;      :loser  => rikikshi name
  ;;      :technique => technique
  ;;      :technique_category => technique category
  ;;      :is_playoff => pass true to get playoff matches only
  ;;      :year, :month, :day => specify as you wish
  ;;   - Routes take optional :page and :per
  ;;   - Default to returning :page "1" with :per "15"
  ;;

  (context ["/bout"] []

    ;; all bouts.
    ;; e.g. /bout/list?year=2021&winner=endo
    (GET "/list"
      [winner loser technique technique_category
       is_playoff year month day page per]
      (response
        (db/get-bout-list
          (merge
            {:winner winner
             :loser loser
             :technique technique
             :technique-category technique_category
             :is-playoff is_playoff
             :year year
             :month month
             :day day}
            ;; :paginate true}
            (if page {:page page} nil)
            (if per  {:per per} nil)))))

    ;; all bouts :rikishi is in.
    ;; takes optional :rank param for what rank rikishi was in the bout
    ;; e.g. /bout/list/endo?rank=maegashira_1&year=2020&month=1&day=1&per=1&page=1
    (GET "/list/:rikishi"
      [rikishi winner loser technique technique_category
       rank is_playoff year month day page per]
      (response
        (db/get-bout-list
          (merge
            {:rikishi rikishi
             :winner winner
             :loser loser
             :technique technique
             :technique-category technique_category
             :rank (rank/rank-keyword-to-str rank)
             :is-playoff is_playoff
             :year year
             :month month
             :day day}
            ;; :paginate true}
            (if page {:page page} nil)
            (if per  {:per per} nil)))))

    ;; all bouts :rikishi is in with :opponent.
    ;; takes optional :rank param for what rank rikishi was in the bout
    ;; takes optional :opponent_rank param for what rank opponent was in the bout
    ;; e.g. /bout/list/endo/takakeisho?winner=endo
    (GET "/list/:rikishi/:opponent"
      [rikishi opponent winner loser technique technique_category
       rank opponent_rank is_playoff year month day page per]
      (response
        (db/get-bout-list
          (merge
            {:rikishi rikishi
             :opponent opponent
             :winner winner
             :loser loser
             :technique technique
             :technique-category technique_category
             :rank (rank/rank-keyword-to-str rank)
             :opponent-rank (rank/rank-keyword-to-str opponent_rank)
             :is-playoff is_playoff
             :year year
             :month month
             :day day}
            ;; :paginate true}
            (if page {:page page} nil)
            (if per  {:per per} nil))))))

  ;;
  ;; ************************* FARE ****************************
  ;;   - Lists of bouts by rikishi against certain ranks
  ;;   - "How does endo fare against Ozeki or higher rank?"
  ;;   - Allows for more rank specificity than BOUT via the matchup param
  ;;
  ;;   - All routes take these optional params
  ;;      :at_rank     => rank rikishi was at in bout
  ;;      :matchup     => "includes_higher_ranks", "higher_ranks_only",
  ;;                      "includes_lower_ranks", "lower_ranks_only"
  ;;      :technique => technique
  ;;      :technique_category => technique category
  ;;      :is_playoff  => pass true to get playoff matches only
  ;;      :year, :month, :day => specify as you wish
  ;;   - Pass nothing for matchup, defaults to specified rank only
  ;;   - Routes take optional :page and :per
  ;;   - Defaults to returning :page "1" with :per "15"
  ;;

  (context "/fare" []

    ;; bout list of :rikishi vs :against_rank,
    ;; e.g. /fare/endo/ozeki?&matchup=higher_ranks_only
    ;;  - "give me all bouts where endo faced higher rank than ozeki"
    ;; e.g. /fare/endo/ozeki?at_rank=maegashira_1&matchup=includes_lower_ranks
    ;;  - "give me all bouts where endo was maegashira #1 and faced ozeki or lower rank"
    (GET "/:rikishi/:against_rank"
      [rikishi against_rank at_rank matchup technique technique_category
       is_playoff year month day page per]
      (response
        (db/get-bout-list
          (merge
            {:rikishi rikishi
             :against-rank (rank/rank-keyword-to-str against_rank)
             :against-rank-value (rank/get-rank-value {:rank against_rank :year year :month month})
             :at-rank (rank/rank-keyword-to-str at_rank)
             :technique technique
             :technique-category technique_category
             :is-playoff is_playoff
             :year year
             :month month
             :day day}
            ;; :paginate true} ; higher ranks have lower rank-value
            (when (= matchup "includes_higher_ranks")
              {:comparison "<="})
            (when (= matchup "higher_ranks_only")
              {:comparison "<"})
            (when (= matchup "includes_lower_ranks")
              {:comparison ">="})
            (when (= matchup "lower_ranks_only")
              {:comparison ">"})
            (when page {:page page})
            (when per {:per per})))))

    ;; wins :rikishi had vs :against_rank, e.g. maegashira_1
    (GET "/win/:rikishi/:against_rank"
      [rikishi against_rank at_rank matchup technique technique_category
       is_playoff year month day page per]
      (response
        (db/get-bout-list
          (merge
            {:rikishi rikishi
             :against-rank (rank/rank-keyword-to-str against_rank)
             :against-rank-value (rank/get-rank-value {:rank against_rank :year year :month month})
             :at-rank (rank/rank-keyword-to-str at_rank)
             :winner rikishi
             :technique technique
             :technique-category technique_category
             :is-playoff is_playoff
             :year year
             :month month
             :day day}
            ;; :paginate true}
            (when (= matchup "includes_higher_ranks")
              {:comparison "<="})
            (when (= matchup "higher_ranks_only")
              {:comparison "<"})
            (when (= matchup "includes_lower_ranks")
              {:comparison ">="})
            (when (= matchup "lower_ranks_only")
              {:comparison ">"})
            (when page {:page page})
            (when per {:per per})))))

    ;; losses :rikishi had to :against_rank
    (GET "/lose/:rikishi/:against_rank"
      [rikishi against_rank at_rank matchup technique technique_category
       is_playoff year month day page per]
      (response
        (db/get-bout-list
          (merge
            {:rikishi rikishi
             :against-rank (rank/rank-keyword-to-str against_rank)
             :against-rank-value (rank/get-rank-value {:rank against_rank :year year :month month})
             :at-rank (rank/rank-keyword-to-str at_rank)
             :loser rikishi
             :technique technique
             :technique-category technique_category
             :is-playoff is_playoff
             :year year
             :month month
             :day day}
            ;; :paginate true} ; higher ranks have lower rank-value
            (when (= matchup "includes_higher_ranks")
              {:comparison "<="})
            (when (= matchup "higher_ranks_only")
              {:comparison "<"})
            (when (= matchup "includes_lower_ranks")
              {:comparison ">="})
            (when (= matchup "lower_ranks_only")
              {:comparison ">"})
            (when page {:page page})
            (when per {:per per}))))))

  ;;
  ;; ********************** UPSET ***********************
  ;;   - Lists of upsets rikishi achieved or surrendered
  ;;   - "How many time did Endo beat someone 5 ranks
  ;;      higher than himself?"
  ;;
  ;;   - All routes take these optional params
  ;;      :rank_delta => difference between rank levels
  ;;      :matchup => "includes_larger", "larger_only",
  ;;                  "includes_smaller", "smaller_only"
  ;;      :technique => technique
  ;;      :technique_category => technique category
  ;;      :is_playoff  => pass true to get playoff matches only
  ;;      :year, :month, :day => specify as you wish
  ;;   - Pass nothing for matchup, defaults to specified rank-delta only
  ;;   - Routes take optional :page and :per
  ;;   - Defauls to returning :page "1" with :per "15"
  ;;

  (context "/upset" []

    ;; all upsets where the rikishi ranks meet the passed in rank-delta
    ;; e.g. /upset/list?rank_delta=10&matchup=includes_larger&technique_category=push
    ;; - "give me all upsets of 10 rank levels or higher where the technique category was push"
    (GET "/list"
      [rank_delta matchup technique technique_category is_playoff year month day page per]
      (response
        (db/get-bout-list
          (merge
            {:rank-delta rank_delta
             :technique technique
             :technique-category technique_category
             :is-playoff is_playoff
             :year year
             :month month
             :day day}
            ;; :paginate true}
            (when (= matchup "includes_larger")
              {:comparison ">="})
            (when (= matchup "larger_only")
              {:comparison ">"})
            (when (= matchup "includes_smaller")
              {:comparison "<="})
            (when (= matchup "smaller_only")
              {:comparison "<"})
            (when page {:page page})
            (when per {:per per})))))

    ;; get all upsets that :rikishi won (defeated higher ranked opponent)
    (GET "/win/:rikishi"
      [rikishi rank_delta matchup technique technique_category is_playoff year month day page per]
      (response
        (db/get-bout-list
          (merge
            {:winner rikishi
             :rank-delta rank_delta
             :technique technique
             :technique-category technique_category
             :is-playoff is_playoff
             :year year
             :month month
             :day day}
            ;; :paginate true}
            (when (= matchup "includes_larger")
              {:comparison ">="})
            (when (= matchup "larger_only")
              {:comparison ">"})
            (when (= matchup "includes_smaller")
              {:comparison "<="})
            (when (= matchup "smaller_only")
              {:comparison "<"})
            (when page {:page page})
            (when per {:per per})))))

    ;; get all bouts where :rikishi was upset (lost to lower ranked opponent)
    (GET "/lose/:rikishi"
      [rikishi rank_delta matchup technique technique_category is_playoff year month day page per]
      (response
        (db/get-bout-list
          (merge
            {:loser rikishi
             :rank-delta rank_delta
             :technique technique
             :technique-category technique_category
             :is-playoff is_playoff
             :year year
             :month month
             :day day}
            ;; :paginate true}
            (when (= matchup "includes_larger")
              {:comparison ">="})
            (when (= matchup "larger_only")
              {:comparison ">"})
            (when (= matchup "includes_smaller")
              {:comparison "<="})
            (when (= matchup "smaller_only")
              {:comparison "<"})
            (when page {:page page})
            (when per {:per per}))))))

  ;;
  ;; ******************** PERFORM ***************************
  ;;   - Lists of bouts by rank and technique
  ;;   - "How do ozeki perform? Against oshidashi?"
  ;;
  ;;   - All routes take these optional params
  ;;     ...
  ;;     :is_playoff => pass true to get playoff matches only
  ;;     :year, :month, :day => specify as you wish
  ;;  - Routes take optional :page and :per
  ;;  - Defaults to returning :page "1" with :per "15"
  ;;

  (context "/perform" []

    ;; TODO -- reconsider what this grouping of routes is
    ;;   and if it's needed at all
    ;; all bouts, wins, and losses by rank
    ;; (GET "/:rank")
    ;; (GET "/win/:rank")
    ;; (GET "/lose/:rank")

    ;; techniques performed by ranks?

    ;; all bouts, wins, and losses by rank and technique
    ;; (GET "/:rank/:technique" [technique year month day page per]
    ;;   (response
    ;;    (db/get-bout-list
    ;;     (merge
    ;;      {:technique technique
    ;;       :year year
    ;;       :month month
    ;;       :day day
    ;;       :paginate true}
    ;;      (when page {:page page})
    ;;      (when per {:per per})))))
    ;; (GET "/win/:rank/:technique" )
    ;; (GET "/lose/:rank/:technique")
    )

  (route/not-found "Route Not Found"))


(def app
  (-> app-routes
    (wrap-params)
    (wrap-cors #".*") ; lock down to just this app's front-end when deploying
    (wrap-with-logger {:response-keys [:headers]})
    (wrap-json-response)))
