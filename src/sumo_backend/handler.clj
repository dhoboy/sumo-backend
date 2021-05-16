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
  
  ;;;;;;;;;;;;;;
  ;; BY BOUT
  ;;;;;;;;;;;;;;
  
  (context ["/bouts"] []

    ; all tournaments data exists for
    (GET "/list" []
      (response (mysql/list-bouts)))
    
    ; all bouts rikishi is in
    ; e.g. /bouts/endo?year=2020&month=1&day=1&per=1&page=1
    (GET "/:name" [name year month day page per]
      (response
        (mysql/get-bouts-by-rikishi
          (merge ; get-bouts-by-rikishi handles default pagination
            {:name name
             :year year
             :month month
             :day day}
             (if page {:page page} nil)
             (if per  {:per per} nil)))))

    ;; re-write this
    ; all bouts on specified year/month/day
    (GET "/:year/:month/:day" [year month day]
      (response (mysql/get-bouts-by-date year month day)))
  
    ; all bouts in specified year/month
    (GET "/:year/:month" [year month]
      (response (mysql/get-bouts-by-date year month)))
  
    ; all bouts in specified year
    (GET "/:year" [year]
      (response (mysql/get-bouts-by-date year)))
  )

  (route/not-found "Not Found"))

;; (def app
;;  (wrap-json-response
;;    (wrap-cors
;;      (wrap-params app-routes)
;;     #".*")))

(def app
  (-> app-routes
      (wrap-params)
      (wrap-cors #".*")
      (wrap-json-response)))
