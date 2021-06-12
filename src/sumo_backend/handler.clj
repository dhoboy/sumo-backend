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

;; TODO --
;; add support for this with rank's value 
;; stored in the database so it can be queried
;; against directly
(def comparison-map
  {">" > ">=" >= "=" = "<" < "<=" <=})

;; Ideas for future routes--
;; something like /bout/list/<rank>
;; all these bouts, but by rank?

;; TODO--
;; plan out ui in terms of 
;; what calls we need where
;; design the api to match the UI needs

;; TODO--
;; build in support for is-playoff for all of these routes

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
    (GET "/:name/current-rank" [name page per]
      (response 
        (utils/paginate-list
          (merge
            {:item-list [(rank/get-rikishi-current-rank {:rikishi name})]}
            (when page {:page page})
            (when per {:per per})
            (when (and (nil? page) (nil? per)) {:all true})))))
    
    ;; list of rikishi's rank changes over time
    (GET "/:name/rank-over-time" [name page per]
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
  
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; * Lists of bouts *
  ;;  - routes take optional :year, :month, :day params
  ;;  - routes take optional :page and :per
  ;;  - defaulting to returning :page "1" with :per "15"
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (context ["/bout"] []

    ;; all bouts, takes optional :winner and :is-playoff params
    ;; e.g. /bout/list?year=2021&winner=endo
    (GET "/list" [winner is-playoff year month day page per]
      (response
        (mysql/get-bout-list
          (merge
            {:winner winner
             :is-playoff is-playoff
             :year year
             :month month
             :day day
             :paginate true}
             (if page {:page page} nil)
             (if per  {:per per} nil)))))

    ;; all bouts rikishi is in.
    ;; takes optional :winner, :looser, :rank param
    ;; :rank param is for what rank rikishi was in the bout
    ;; on this route where you can only reason about one rikishi
    ;; only that rikishi is valid to pass as winner or looser...
    ;; revist this route naming maybe /list/win/:rikishi..?
    ;; e.g. /bout/list/endo?year=2020&month=1&day=1&per=1&page=1
    (GET "/list/:rikishi" [rikishi winner looser rank is-playoff year month day page per]
      (response
        (mysql/get-bout-list
          (merge
            {:rikishi rikishi
             :winner winner
             :looser looser
             :rank (rank/rank-keyword-to-str rank)
             :is-playoff is-playoff
             :year year
             :month month
             :day day
             :paginate true}
             (if page {:page page} nil)
             (if per  {:per per} nil)))))

    ;; all bouts :rikishi is in with :opponent, takes optional :winner, :looser params
    ;; e.g. /bout/list/endo/takakeisho?winner=endo
    (GET "/list/:rikishi/:opponent" [rikishi opponent winner looser rank opponent-rank is-playoff year month day page per]
      (response
        (mysql/get-bout-list
          (merge
            {:rikishi rikishi
             :opponent opponent
             :winner winner
             :looser looser
             :rank (rank/rank-keyword-to-str rank)
             :opponent-rank (rank/rank-keyword-to-str opponent-rank)
             :is-playoff is-playoff
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
                  (when comparison {:comparison (or (get comparison-map comparison) <=)})
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
                  (when comparison {:comparison (or (get comparison-map comparison) <=)})
                  (when delta {:delta (Integer/parseInt delta)})))}
              (when page {:page page})
              (when per {:per per})))))
  )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; * How rikishi fares against certain ranks *
  ;;   - routes take optional :comparision fn
  ;;      from these choices, "=", ">", ">=", "<", "<="
  ;;   - routes take optional :year, :month, :day params
  ;;   - routes take optional :page and :per
  ;;   - defaulting to returning :page "1" with :per "15"
  ;;   - defaulting to using :comparison =
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  (context "/fare" []
    
    ;; bout list of :rikishi vs certain :against-rank,
    ;; with optional rikishi :at-rank
    (GET "/:rikishi/:against-rank" [rikishi against-rank at-rank year month day page per]
      (response 
       (mysql/get-bout-list
          (merge
            {:rikishi rikishi
             :against-rank (rank/rank-keyword-to-str against-rank)
             :at-rank (rank/rank-keyword-to-str at-rank)
             :year year
             :month month
             :day day
             :paginate true}
             (if page {:page page} nil) ; can't these be when's?
             (if per  {:per per} nil)))))
    
    ;; wins :rikishi had against :against-rank, e.g. maegashira_1
    ;; with optional rikishi :at-rank
    (GET "/win/:rikishi/:against-rank" [rikishi against-rank at-rank year month day page per]
      (response
        (mysql/get-bout-list ; this returns way faster than the stepping through each bout stuff...
          (merge
            {:rikishi rikishi
             :against-rank (rank/rank-keyword-to-str against-rank)
             :at-rank (rank/rank-keyword-to-str at-rank)
             :winner rikishi
             :year year
             :month month
             :day day
             :paginate true}
         (when page {:page page})
         (when per {:per per})))))
        
    ;; losses :rikishi had to :against-rank
    ;; with optional rikishi :at-rank
    (GET "/lose/:rikishi/:against-rank" [rikishi against-rank at-rank year month day page per]
      (response
        (mysql/get-bout-list ; this returns way faster than the stepping through each bout stuff...
          (merge
            {:rikishi rikishi
             :against-rank (rank/rank-keyword-to-str against-rank)
             :at-rank (rank/rank-keyword-to-str at-rank)
             :looser rikishi
             :year year
             :month month
             :day day
             :paginate true}
         (when page {:page page})
         (when per {:per per})))))
    
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
    
    ;; TODO--
    ;; see about deriving urls for theses matches 
    ;; on nhk japan site

    ;; only thing these slow ones have right now is
    ;; they support a comparision fn.
    ;; comparison fn can be done when on sql route when
    ;; rank values get stored in the sql database
    
    ;; wins :rikishi had against :rank
    ;; (GET "/win/slow/:rikishi/:rank" [rikishi rank comparison year month day page per]
    ;;   (response
    ;;     (utils/paginate-list
    ;;      (merge
    ;;       {:item-list
    ;;        (rank/wins-vs-rank
    ;;         (merge
    ;;          {:rikishi rikishi
    ;;           :against-rank (keyword rank)
    ;;           :year year
    ;;           :month month
    ;;           :day day}
    ;;          (when comparison {:comparison (or (get comparison-map comparison) =)})))}
    ;;       (when page {:page page})
    ;;       (when per {:per per})))))
    
    ;; losses :rikishi had against :rank
    ;; (GET "/lose/slow/:rikishi/:rank" [rikishi rank comparison year month day page per]
    ;;   (response
    ;;     (utils/paginate-list
    ;;       (merge
    ;;         {:item-list
    ;;           (rank/losses-to-rank
    ;;             (merge
    ;;               {:rikishi rikishi
    ;;                :against-rank (keyword rank)
    ;;                :year year
    ;;                :month month
    ;;                :day day}
    ;;               (when comparison {:comparison (or (get comparison-map comparison) =)})))}
    ;;       (when page {:page page})
    ;;       (when per {:per per})))))
  )
            
  (route/not-found "Route Not Found"))

(def app
  (-> app-routes
      (wrap-params)
      (wrap-cors #".*") ; lock down to just this app's front-end when deploying
      (wrap-json-response)))
