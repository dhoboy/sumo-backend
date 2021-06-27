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

;; Ideas for future routes--
;; something like /bout/list/<rank>
;; all these bouts, but by rank?

;; TODO--
;; plan out ui in terms of 
;; what calls we need where
;; design the api to match the UI needs

;; TODO--
;; see about deriving urls for theses matches 
;; on nhk japan site

(defroutes app-routes
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; * Rikishi information *
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
  ;; * Tournament information *
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (context ["/tournament"] []

    ; list of all tournaments
    (GET "/list" [page per]
      (response 
       (utils/paginate-list
        (merge
         {:item-list (mysql/list-tournaments)}
         (when page {:page page})
         (when per {:per per})
         (when (and (nil? page) (nil? per)) {:all true})))))
    )
  
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; * Lists of bouts against specific rikishi *
  ;;  ("How does Endo do in general, or against Takakeisho?")
  ;;  - all routes take these optional params
  ;;     :winner => rikishi name
  ;;     :loser  => rikikshi name
  ;;     :is_playoff => pass true to get playoff matches only 
  ;;     :year, :month, :day => specify as you wish
  ;;  - routes take optional :page and :per
  ;;  - defaulting to returning :page "1" with :per "15"
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (context ["/bout"] []
    
    ; TODO -- add technique to this  
    ;   /bout/list/endo/takakeisho?technique=oshidashi&winner=takakeisho
    
    ;; all bouts.
    ;; e.g. /bout/list?year=2021&winner=endo
    (GET "/list" [winner loser is_playoff year month day page per]
      (response
       (mysql/get-bout-list
        (merge
         {:winner winner
          :loser loser
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
    (GET "/list/:rikishi" [rikishi winner loser rank is_playoff year month day page per]
      (response
       (mysql/get-bout-list
        (merge
         {:rikishi rikishi
          :winner winner
          :loser loser
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
    (GET "/list/:rikishi/:opponent" [rikishi opponent winner loser rank opponent_rank is_playoff year month day page per]
      (response
       (mysql/get-bout-list
        (merge
         {:rikishi rikishi
          :opponent opponent
          :winner winner
          :loser loser
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

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; * Upsets rikishi achieved or surrendered *
  ;;   - routes take optional :comparision fn
  ;;      from these choices, "=", ">", ">=", "<", "<="
  ;;   - routes take optional :delta param
  ;;      specifies how many rank levels the upset 
  ;;      should be across e.g. :delta 5 means an upset 
  ;;      by a rikishi 5 levels lower ranked
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
         (when per {:per per})))))
    )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; * How rikishi fares against certain ranks *
  ;;   ("How does endo do against Ozeki?")
  ;;   - all routes take these optional params
  ;;      :at_rank     => rank rikishi was at in bout
  ;;      :matchup     => "includes_higher_ranks", "higher_ranks_only",
  ;;                      "includes_lower_ranks", "lower_ranks_only"
  ;;      :is_playoff  => pass true to get playoff matches only
  ;;      :year, :month, :day => specify as you wish
  ;;   - pass nothing back for matchup, defaults to specified rank only
  ;;   - routes take optional :page and :per
  ;;   - defaulting to returning :page "1" with :per "15"
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  (context "/fare" []

    ; TODO - add technique to this as optional param
    ;   /fare/endo/ozeki?technique=oshidashi
    ;   /fare/win/endo/ozeki?technique_category=push
    
    ;; bout list of :rikishi vs :against_rank,
    ;; e.g. /fare/endo/ozeki?&matchup=higher_ranks_only
    ;;  - "give me all bouts where endo faced higher rank than ozeki"
    ;; e.g. /fare/endo/ozeki?at_rank=maegashira_1&matchup=includes_lower_ranks
    ;;  - "give me all bouts where endo was maegashira #1 and faced ozeki or lower rank"
    (GET "/:rikishi/:against_rank" 
      [rikishi against_rank at_rank matchup is_playoff year month day page per]
      (response 
       (mysql/get-bout-list
        (merge
         {:rikishi rikishi
          :against-rank (rank/rank-keyword-to-str against_rank)
          :against-rank-value (rank/get-rank-value {:rank against_rank :year year :month month})
          :at-rank (rank/rank-keyword-to-str at_rank)
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
      [rikishi against_rank at_rank matchup is_playoff year month day page per]
      (response
       (mysql/get-bout-list
        (merge
         {:rikishi rikishi
          :against-rank (rank/rank-keyword-to-str against_rank)
          :against-rank-value (rank/get-rank-value {:rank against_rank :year year :month month})
          :at-rank (rank/rank-keyword-to-str at_rank)
          :winner rikishi
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
      [rikishi against_rank at_rank matchup is_playoff year month day page per]
      (response
       (mysql/get-bout-list
        (merge
         {:rikishi rikishi
          :against-rank (rank/rank-keyword-to-str against_rank)
          :against-rank-value (rank/get-rank-value {:rank against_rank :year year :month month})
          :at-rank (rank/rank-keyword-to-str at_rank)
          :looser rikishi
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
  
  ;; general info about techniques
  (context "/technique" []
    ; technique list
    ; category list
    ; get technique category
    ; list techniques in each category
    )
  
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; * Lists of bouts against techniques *
  ;;   ("How does endo do against oshidashi?")
  ;;  - all routes take these optional params
  ;;     ...
  ;;     :is_playoff => pass true to get playoff matches only 
  ;;     :year, :month, :day => specify as you wish
  ;;  - routes take optional :page and :per
  ;;  - defaulting to returning :page "1" with :per "15"
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  (context "/perform" []
    ; also take technique category for all these
    ; wins by technique, all rikishi
    ; loses to technique, all rikishi
    ; wins by technique by rikishi
    ; loses to technique by rikishi
    ; wins by technique against rikishi
    ; wins by technique against rank
    ; loses to technique agaisnt rank
    ; loses to technique agaisnt rikishi
    )
  
  (route/not-found "Route Not Found"))

(def app
  (-> app-routes
      (wrap-params)
      (wrap-cors #".*") ; lock down to just this app's front-end when deploying
      (wrap-json-response)))
