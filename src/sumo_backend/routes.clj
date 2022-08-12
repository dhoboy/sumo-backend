(ns sumo-backend.routes
  (:require [compojure.core :refer [GET context defroutes]]
            [compojure.route :as route]
            [ring.logger :refer [wrap-with-logger]]
            [ring.middleware.json :refer [wrap-json-response]]
            [ring.middleware.params :refer [wrap-params]]
            [jumblerg.middleware.cors :refer [wrap-cors]]
            [sumo-backend.api.rikishi-list :as rikishi_list]
            [sumo-backend.api.rikishi-details :as rikishi_details]
            [sumo-backend.api.rikishi-current-rank :as rikishi_current_rank]
            [sumo-backend.api.rikishi-rank-over-time :as rikishi_rank_over_time]
            [sumo-backend.api.rikishi-results-over-time :as rikishi_results_over_time]
            [sumo-backend.api.rikishi-technique-breakdown :as rikishi_technique_breakdown]
            [sumo-backend.api.tournament-list :as tournament_list]
            [sumo-backend.api.tournament-details :as tournament_details]
            [sumo-backend.api.technique-list :as technique_list]
            [sumo-backend.api.technique-categories :as technique_categories]
            [sumo-backend.api.technique-details :as technique_details]
            [sumo-backend.api.technique-category-details :as technique_category_details]
            [sumo-backend.api.bout-list :as bout_list]
            [sumo-backend.api.bout-list-rikishi :as bout_list_rikishi]
            [sumo-backend.api.bout-list-rikishi-opponent :as bout_list_rikishi_opponent]
            [sumo-backend.api.fare-list :as fare_list]
            [sumo-backend.api.fare-win :as fare_win]
            [sumo-backend.api.fare-lose :as fare_lose]
            [sumo-backend.api.upset-list :as upset_list]
            [sumo-backend.api.upset-win :as upset_win]
            [sumo-backend.api.upset-lose :as upset_lose]))


(defroutes app-routes

  ;; ************************ RIKISHI ***********************
  ;;   - All routes take optional :page and :per, called "options" below
  ;;   - Default to returning all as one unpaginated list
  ;;
  ;;   - /technique_breakdown/:name also takes optional
  ;;       :year, :month, :day => specify as you wish

  (context ["/rikishi"] []
    (GET "/list" [& options]
      (rikishi_list/handler options))

    (GET "/details/:name" [name & options]
      (rikishi_details/handler name options))

    (GET "/current_rank/:name" [name & options]
      (rikishi_current_rank/handler name options))

    (GET "/rank_over_time/:name" [name & options]
      (rikishi_rank_over_time/handler name options))

    (GET "/results_over_time/:name" [name & options]
      (rikishi_results_over_time/handler name options))

    (GET "/technique_breakdown/:name" [name & options]
      (rikishi_technique_breakdown/handler name options)))


  ;; ************************ TOURNAMENT ********************************
  ;;   - All Routes take optional :page and :per, called "options" below
  ;;   - Default to returning all as one unpaginated list

  (context ["/tournament"] []
    (GET "/list" [& options]
      (tournament_list/handler options))

    (GET "/details/:year/:month" [year month & options]
      (tournament_details/handler year month options)))


  ;; ************************ TECHNIQUE *********************************
  ;;   - All Routes take optional :page and :per, called "options" below
  ;;   - Default to returning all as one unpaginated list
  ;;
  ;;   - /list, /details/:technique, /category_details/:category also take optional
  ;;       :year, :month, :day => specify as you wish

  (context ["/technique"] []
    (GET "/list" [& options]
      (technique_list/handler options))

    (GET "/details/:technique" [technique & options]
      (technique_details/handler technique options))

    (GET "/category_details/:category" [category & options]
      (technique_category_details/handler category options))

    (GET "/categories" [& options]
      (technique_categories/handler options)))

  ;; FYI: Bout, Fare, and Upset can be used on the Matchups UI Page


  ;; ********************* BOUT ****************************
  ;;   - Lists of bouts, against rikishi or by technique
  ;;   - "How does Endo do in general, or against Takakeisho?"
  ;;   - "How many wins by oshidashi does Endo have?"
  ;;
  ;;   - All Routes take these optional params, called "options" below
  ;;      :winner => rikishi name
  ;;      :loser  => rikikshi name
  ;;      :technique => technique
  ;;      :technique_category => technique category
  ;;      :is_playoff => pass true to get playoff matches only
  ;;      :year, :month, :day => specify as you wish
  ;;   - Routes take optional :page and :per
  ;;   - Default to returning :page "1" with :per "15"

  (context ["/bout"] []
    (GET "/list" [& options]
      (bout_list/handler options))

    (GET "/list/:rikishi" [rikishi & options]
      (bout_list_rikishi/handler rikishi options))

    (GET "/list/:rikishi/:opponent" [rikishi opponent & options]
      (bout_list_rikishi_opponent/handler rikishi opponent options)))


  ;; ************************* FARE ****************************
  ;;   - Lists of bouts by rikishi against certain ranks
  ;;   - "How does endo fare against Ozeki or higher rank?"
  ;;   - Allows for more rank specificity than BOUT via the matchup param
  ;;
  ;;   - All Routes take these optional params, called "options" below
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

  (context "/fare" []
    (GET "/:rikishi/:against_rank"  [rikishi against_rank & options]
      (fare_list/handler rikishi against_rank options))

    (GET "/win/:rikishi/:against_rank" [rikishi against_rank & options]
      (fare_win/handler rikishi against_rank options))

    (GET "/lose/:rikishi/:against_rank" [rikishi against_rank & options]
      (fare_lose/handler rikishi against_rank options)))


  ;; ********************** UPSET ***********************
  ;;   - Lists of upsets rikishi achieved or surrendered
  ;;   - "How many time did Endo beat someone 5 ranks
  ;;      higher than himself?"
  ;;
  ;;   - All Routes take these optional params, called "options" below
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

  (context "/upset" []
    (GET "/list" [& options]
      (upset_list/handler options))

    (GET "/win/:rikishi" [rikishi & options]
      (upset_win/handler rikishi options))

    (GET "/lose/:rikishi" [rikishi & options]
      (upset_lose/handler rikishi options)))


  ;; FYI: Not implemented yet
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
