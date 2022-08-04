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
            [sumo-backend.api.technique-category-details :as technique_category_details]))


(defroutes app-routes

  ;; ******* Rikishi information *************
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list

  (context ["/rikishi"] []
    ;; list of all rikishi
    (GET "/list" [page per]
      (rikishi_list/handler page per))

    ;; specific rikishi record
    (GET "/details/:name" [name page per]
      (rikishi_details/handler name page per))

    ;; rikishi current rank
    ;; current rank is rank in last basho rikishi competed in
    (GET "/current_rank/:name" [name page per]
      (rikishi_current_rank/handler name page per))

    ;; list of rikishi's rank changes over time
    (GET "/rank_over_time/:name" [name page per]
      (rikishi_rank_over_time/handler name page per))

    ;; list of rikishi tournament results over time
    (GET "/results_over_time/:name" [name page per]
      (rikishi_results_over_time/handler name page per))

    ;; TODO -- add in groups of wins / losses occuring at each rank?
    ;; basic stats on techniques and categories rikishi wins / looses by
    (GET "/technique_breakdown/:name" [name year month day page per]
      (rikishi_technique_breakdown/handler name year month day page per)))


  ;; ****** Tournament information ***********
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list

  (context ["/tournament"] []
    ;; list of all tournaments data exists for
    (GET "/list" [page per]
      (tournament_list/handler page per))

    ;; TODO -- add tournament champion and location.
    ;; details about tournament, e.g. rikishi records
    (GET "/details/:year/:month" [year month page per]
      (tournament_details/handler year month page per)))


  ;; ******** Technique information **********
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list

  (context ["/technique"] []
    ;; technique list
    ;; takes optional :year :month :day params
    ;; defaults to returning all sets of--
    ;; technique, technique_en, and technique_category
    ;; found in database, else returns them for
    ;; specified :year, :month, :day
    (GET "/list" [year month day page per]
      (technique_list/handler year month day page per))

    ;; list of all categories and technique keys classified within
    (GET "/categories" [page per]
      (technique_categories/handler page per))

    ;; rikishi wins and losses by a certain technique
    ;; takes optional :year :month :day params
    (GET "/details/:technique" [technique year month day page per]
      (technique_details/handler technique year month day page per))

    ;; rikishi wins and losses by a certain technique category
    ;; takes optional :year :month :day params
    (GET "/category_details/:category" [category year month day page per]
      (technique_category_details/handler category year month day page per)))

  (route/not-found "Route Not Found"))


(def app
  (-> app-routes
    (wrap-params)
    (wrap-cors #".*") ; lock down to just this app's front-end when deploying
    (wrap-with-logger {:response-keys [:headers]})
    (wrap-json-response)))
