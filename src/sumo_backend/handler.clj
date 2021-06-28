(ns sumo-backend.handler)
(require '[compojure.core :refer :all])
(require '[compojure.route :as route])
(require '[ring.middleware.json :refer [wrap-json-response]])
(require '[ring.middleware.params :refer :all])
(require '[ring.util.response :refer [response]])
(require '[jumblerg.middleware.cors :refer [wrap-cors]])
(require '[sumo-backend.utils :as utils])
(require '[sumo-backend.mysql :as mysql])
(require '[sumo-backend.rank :as rank])
(require '[sumo-backend.technique :as technique])

;; TODO--
;; plan out ui in terms of 
;; what calls we need where
;; design the api to match the UI needs

;; TODO--
;; see about deriving urls for theses matches 
;; on nhk japan site

(defroutes app-routes
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; ******* Rikishi information *************
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (context ["/rikishi"] []
    
    ;; Route for highest rank achieved, and tournaments
    ;; rikishi held that rank? could also be derived 
    ;; from rikishi-rank-over-time
    
    ;; list of all rikishi
    (GET "/list" [page per]
      (response 
       (utils/paginate-list
        (merge
         {:item-list (mysql/list-rikishi)}
         (when page {:page page})
         (when per {:per per})
         (when (and (nil? page) (nil? per)) {:all true})))))
    
    ;; specific rikishi record
    (GET "/:name/details" [name page per] 
      (response 
       (utils/paginate-list
        (merge
         {:item-list (mysql/get-rikishi name)}
         (when page {:page page})
         (when per {:per per})
         (when (and (nil? page) (nil? per)) {:all true})))))
    
    ;; rikishi current rank
    ;; current rank is rank in last basho rikishi competed in
    (GET "/:name/current_rank" [name page per]
      (response 
       (utils/paginate-list
        (merge
         {:item-list [(rank/get-rikishi-current-rank {:rikishi name})]}
         (when page {:page page})
         (when per {:per per})
         (when (and (nil? page) (nil? per)) {:all true})))))
    
    ;; list of rikishi's rank changes over time
    (GET "/:name/rank_over_time" [name page per]
      (response 
       (utils/paginate-list
        (merge
         {:item-list (rank/get-rikishi-rank-over-time {:rikishi name})}
         (when page {:page page})
         (when per {:per per})
         (when (and (nil? page) (nil? per)) {:all true})))))
    )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; ****** Tournament information ***********
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (context ["/tournament"] []

    ; list of all tournaments data exists for
    (GET "/list" [page per]
      (response 
       (utils/paginate-list
        (merge
         {:item-list (mysql/list-tournaments)}
         (when page {:page page})
         (when per {:per per})
         (when (and (nil? page) (nil? per)) {:all true})))))

    ; details about tournament: rikishi records, location?
    ; add makikoshi or kachikoshi to each rikishi too in the merge with
    (GET "/:year/:month" [year month page per]
      (response
       (utils/paginate-list
        (merge
         {:item-list (apply 
                      merge ;; need to do a merge with here?
                      (concat
                       (mysql/get-wins-in-tournament {:year year :month month})
                       (mysql/get-losses-in-tournament {:year year :month month})))}
         (when page {:page page})
         (when per {:per per})
         (when (and (nil? page) (nil? per)) {:all true})))))
    )
  
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; ******** Technique information **********
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (context "/technique" []
    
    ; technique list
    ; takes optional :year :month :day params
    ; defaults to returning all sets of technique,
    ; technique_en, and technique_category found in database
    (GET "/list" [year month day page per]
      (response
       (utils/paginate-list
        (merge
         {:item-list (mysql/list-techniques {:year year :month month :day day})}
         (when page {:page page})
         (when per {:per per})
         (when (and (nil? page) (nil? per)) {:all true})))))
    
    ; list of all categories and technique keys classified within
    (GET "/categories" [page per]
      (response
       (utils/paginate-list
        (merge
         {:item-list (technique/get-categories)}
         (when page {:page page})
         (when per {:per per})
         (when (and (nil? page) (nil? per)) {:all true})))))
    
     ;; could return stats about how many times this technique gets used
     ;; who uses it and what rank uses it...
     ;; (GET "/:technique" [])
    
    )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (context ["/bout"] []
    
    ;; all bouts.
    ;; e.g. /bout/list?year=2021&winner=endo
    (GET "/list" 
      [winner loser technique technique_category 
       is_playoff year month day page per]
      (response
       (mysql/get-bout-list
        (merge
         {:winner winner
          :loser loser
          :technique technique
          :technique-category technique_category
          :is-playoff is_playoff
          :year year
          :month month
          :day day
          :paginate true}
         (if page {:page page} nil)
         (if per  {:per per} nil)))))

    ;; all bouts :rikishi is in.
    ;; takes optional :rank param for what rank rikishi was in the bout
    ;; e.g. /bout/list/endo?rank=maegashira_1&year=2020&month=1&day=1&per=1&page=1
    (GET "/list/:rikishi" 
      [rikishi winner loser technique technique_category 
       rank is_playoff year month day page per]
      (response
       (mysql/get-bout-list
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
          :day day
          :paginate true}
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
       (mysql/get-bout-list
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
          :day day
          :paginate true}
         (if page {:page page} nil)
         (if per  {:per per} nil)))))
    )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
  ;;   - Pass nothing back for matchup, defaults to specified rank only
  ;;   - Routes take optional :page and :per
  ;;   - Defaults to returning :page "1" with :per "15"
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
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
       (mysql/get-bout-list
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
          :day day
          :paginate true} ; higher ranks have lower rank-value
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
       (mysql/get-bout-list
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
          :day day
          :paginate true}
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
       (mysql/get-bout-list
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
          :day day
          :paginate true} ; higher ranks have lower rank-value
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
    )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; ********************** UPSET ***********************
  ;;   - Lists of upsets rikishi achieved or surrendered 
  ;;   - routes take optional :comparision fn
  ;;       from these choices, "=", ">", ">=", "<", "<="
  ;;   - routes take optional :delta param
  ;;       specifies how many rank levels the upset 
  ;;       should be across e.g. :delta 5 means an upset 
  ;;       by a rikishi 5 levels lower ranked
  ;;   - routes take optional :year, :month, :day params
  ;;   - routes take optional :page and :per
  ;;   - defaulting to returning :page "1" with :per "15"
  ;;   - defaulting to using :comparison <=, :delta ##Inf
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (context "/upset" []

    ;; all upsets where the rikishi ranks meet the passed in delta
    ;; (GET "/:delta" [delta comparison year month day page per])

    ;; TODO - 
    ;; rewrite the functions used to get upsets to
    ;; be direct sql queries. stepping through the 
    ;; entire bout list is too slow

    ;; get all upsets that :rikishi won (defeated higher ranked opponent)
    (GET "/win/:rikishi" [rikishi delta comparison year month day page per]
      (response
       (utils/paginate-list
        (merge
         {:item-list
          (rank/wins-vs-higher-rank
           (merge
            {:rikishi rikishi
             :year year
             :month month
             :day day}
            ;; (when comparison {:comparison (or (get comparison-map comparison) <=)})
            (when delta {:delta (Integer/parseInt delta)})))}
         (when page {:page page})
         (when per {:per per})))))

    ;; get all bouts where :rikishi was upset (lost to lower ranked opponent)
    (GET "/lose/:rikishi" [rikishi delta comparison year month day page per]
      (response
       (utils/paginate-list
        (merge
         {:item-list
          (rank/losses-to-lower-rank
           (merge
            {:rikishi rikishi
             :year year
             :month month
             :day day}
            ;; (when comparison {:comparison (or (get comparison-map comparison) <=)})
            (when delta {:delta (Integer/parseInt delta)})))}
         (when page {:page page})
         (when per {:per per}))))))

    ;; TODO--
    ;; Ben has techniques are grouped into 4 categories
    ;; chest to chest - moving forward
    ;; chset to chest - throwing side
    ;; arms lenght - moving forward
    ;; arms length - using opponents weight against them
  
    ;; color code the japanese technique word
    ;; colors for the type of technique
    ;; so ex: hatakikomi is colored red for its category color
    ;; categories have colors!
  
    ; wins by technique against rank
    ; loses to technique agaisnt rank
  
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (context "/perform" []
    
    ;; all bouts, wins, and losses by rank
    ;; (GET "/:rank")
    ;; (GET "/win/:rank")
    ;; (GET "/lose/:rank")
    
    ;; all bouts, wins, and losses by rank and technique
    ;; (GET "/:rank/:technique" [technique year month day page per]
    ;;   (response
    ;;    (mysql/get-bout-list
    ;;     (merge
    ;;      {:technique technique
    ;;       :year year
    ;;       :month month
    ;;       :day day
    ;;       :paginate true}
    ;;      (when page {:page page})
    ;;      (when per {:per per})))))
    ;;(GET "/win/:rank/:technique" )
    ;;(GET "/lose/:rank/:technique")
    )
  
  (route/not-found "Route Not Found"))

(def app
  (-> app-routes
      (wrap-params)
      (wrap-cors #".*") ; lock down to just this app's front-end when deploying
      (wrap-json-response)))
