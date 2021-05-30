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

(def comparison-map
  {">" > ">=" >= "=" = "<" < "<=" <=})

;; Ideas for future routes--
;; something like /bout/list/<rank>
;; all these bouts, but by rank?
;; also, bout list by rikishi at rank?

;; TODO 
;; -- maegashira and juryo ranks dont pass to these routes easily

(defroutes app-routes
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; * Rikishi information *
  ;;   - routes take optional :page and :per
  ;;   - default to returning all as one list
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (context ["/rikishi"] []
    
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
            {:item-list (mysql/list-bouts)}
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

    ;; all bouts, takes optional :winner param
    ;; e.g. /bout/list?year=2021&winner=endo
    (GET "/list" [winner year month day page per]
      (response
        (mysql/get-bout-list
          (merge
            {:winner winner
             :year year
             :month month
             :day day
             :paginate true}
             (if page {:page page} nil)
             (if per  {:per per} nil)))))

    ;; all bouts rikishi is in, takes optional :winner param
    ;; e.g. /bout/list/endo?year=2020&month=1&day=1&per=1&page=1
    (GET "/list/:rikishi" [rikishi winner year month day page per]
      (response
        (mysql/get-bout-list
          (merge
            {:rikishi rikishi
             :winner winner
             :year year
             :month month
             :day day
             :paginate true}
             (if page {:page page} nil)
             (if per  {:per per} nil)))))

    ;; all bouts :rikishi is in with :opponent, takes optional :winner, :looser params
    ;; e.g. /bout/list/endo/takakeisho?winner=endo
    (GET "/list/:rikishi/:opponent" [rikishi opponent winner looser year month day page per]
      (response
        (mysql/get-bout-list
          (merge
            {:rikishi rikishi
             :opponent opponent
             :winner winner
             :looser looser
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
    
    ;; wins :rikishi had against :rank
    (GET "/win/:rikishi/:rank" [rikishi rank comparison year month day page per]
      (response 
        (utils/paginate-list
          (merge
            {:item-list
              (rank/wins-vs-rank
                (merge
                  {:rikishi rikishi
                   :rank-str rank
                   :year year
                   :month month
                   :day day}
                   (when comparison {:comparison (or (get comparison-map comparison) =)})))}
              (when page {:page page})
              (when per {:per per})))))
    
    ;; losses :rikishi had against :rank
    (GET "/lose/:rikishi/:rank" [rikishi rank comparison year month day page per]
      (response
        (utils/paginate-list
          (merge
            {:item-list
              (rank/losses-to-rank
                (merge
                  {:rikishi rikishi
                   :rank-str rank
                   :year year
                   :month month
                   :day day}
                   (when comparison {:comparison (or (get comparison-map comparison) =)})))}
              (when page {:page page})
              (when per {:per per})))))
  )
            
  (route/not-found "Route Not Found"))

(def app
  (-> app-routes
      (wrap-params)
      (wrap-cors #".*") ; lock down to just this app's front-end when deploying
      (wrap-json-response)))
