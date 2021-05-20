(ns sumo-backend.handler)
(require '[compojure.core :refer :all])
(require '[compojure.route :as route])
(require '[ring.middleware.json :refer [wrap-json-response]])
(require '[ring.middleware.params :refer :all])
(require '[ring.util.response :refer [response]])
(require '[jumblerg.middleware.cors :refer [wrap-cors]])
(require '[sumo-backend.mysql :as mysql])
  
;; add a route that lets you get
 ;; upsets by rikishi by delta
 ;; "rikishi/upsets/endo/win/5"
 ;;  something like that would mean
 ;;  all the bouts where endo
 ;;  beat someone 5 ranks higher than
 ;;  himself.

 ;; "rikishi/upsets/endo/loose/5"
 ;;  all the bouts where he lost
 ;;  to someone 5 ranks lower
 ;;
 ;; or even something like
 ;; "bouts/upsets/5"
 ;;  just all 5 level upsets
 ;;
 ;; also "rikishi/upsets/endo/win"
 ;;  for all the times he beat a higher ranker
 ;;
 ;; "rikishi/upsets/endo/loose"
 ;;  all the time he lost to a lower ranker
 ;;
 ;; -- these 2 aren't upsets but still interesting
 ;; "rikishi/endo/win/<rank>"
 ;;  all the time he beat a particular rank
 ;;
 ;; "rikishi/endo/loose/<rank>"
 ;;  all the time he lost to a particular rank
 ;;

(defroutes app-routes
  ;;;;;;;;;;;;;;
  ;; BY RIKISHI
  ;;;;;;;;;;;;;;

  (context ["/rikishi"] []

    ; list of all rikishi
    (GET "/list" []
      (response (mysql/list-rikishi)))
      
    ; specific rikishi record
    (GET "/details/:name" [name] 
      (response (mysql/get-rikishi name)))
  )

  ;;;;;;;;;;;;;;;;;;;
  ;; BY TOURNAMENT
  ;;;;;;;;;;;;;;;;;;;

  (context ["/tournament"] []

    (GET "/list" []
      (response (mysql/list-bouts)))
    )
  
  ;;;;;;;;;;;;;;
  ;; BY BOUT
  ;;;;;;;;;;;;;;
  
  (context ["/bout"] []

    ;; all these are basically the same ...
    ;; also, something like /bout/list/<rank>
    ;; all these bouts, but by rank?
    ;; also, bout list by rikishi at rank?

    ;; all bouts in specified optional date params
    ;; takes optional winner param
    ;; e.g. /bout/list?year=2021
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

    ;; all bouts rikishi is in
    ;; takes optional date and winner, looser params
    ;; e.g. /bout/list/endo?year=2020&month=1&day=1&per=1&page=1
    (GET "/list/:rikishi" [rikishi winner looser year month day page per]
      (response
        (mysql/get-bout-list
          (merge
            {:rikishi rikishi
             :winner winner
             :looser looser
             :year year
             :month month
             :day day
             :paginate true}
             (if page {:page page} nil)
             (if per  {:per per} nil)))))

    ;; all bouts rikishi is in with opponent
    ;; takes optional date and winner, looser params
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

  (context "/upset" []
    (GET "/list" []
      (response "coming soon"))
  
  )

  (route/not-found "Not Found"))

(def app
  (-> app-routes
      (wrap-params)
      (wrap-cors #".*")
      (wrap-json-response)))
