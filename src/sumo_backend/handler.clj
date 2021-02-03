(ns sumo-backend.handler)
(require '[compojure.core :refer :all])
(require '[compojure.route :as route])
(require '[ring.middleware.json :refer [wrap-json-response]])
(require '[ring.util.response :refer [response]])
(require '[jumblerg.middleware.cors :refer [wrap-cors]])
(require '[sumo-backend.functions :as func])

;; TODO some higher level organization, otherwise looks good
;;
;; I like the approach of using endpoint params instead of query params.
;;
;; You can nest all of the /rikishi/ endpoints and all of the /bouts/ endpoints
;; with compojure.core/context, like:
;;
;;   (context ["/bouts"] []
;;
;;     (GET "/list" [] ... )
;;
;;     (GET "/:year" [year] ... )
;;
;;     ...
;;   )

(defroutes app-routes
  ;;;;;;;;;;;;;;
  ;; BY RIKISHI
  ;;;;;;;;;;;;;;

  ; list of all rikishi
  (GET "/rikishi/list" []
    (response (func/list-rikishi)))

  ;; TODO nest under /rikishi/*

  ; specific rikishi record
  (GET "/:name" [name] 
    (response (func/get-rikishi name)))

  ; all bouts rikishi is in
  (GET "/:name/bouts" [name]
    (response (func/get-bouts-by-rikishi name)))

  ; all bouts rikishi is in on specified year/month/day
  (GET "/:name/bouts/:year/:month/:day" [name year month day]
    (response (func/get-bouts-by-rikishi name year month day)))

  ; all bouts rikishi is in in specified year/month
  (GET "/:name/bouts/:year/:month" [name year month]
   (response (func/get-bouts-by-rikishi name year month)))

  ; all bouts rikishi is in in specified year
  (GET "/:name/bouts/:year" [name year]
    (response (func/get-bouts-by-rikishi name)))
  
  ;;;;;;;;;;;;;;
  ;; BY BOUT
  ;;;;;;;;;;;;;;

  ; all tournaments data exists for
  (GET "/bouts/list" []
    (response (func/list-bouts)))

  ; all bouts on specified year/month/day
  (GET "/bouts/:year/:month/:day" [year month day]
    (response (func/get-bouts-by-date year month day)))
  
  ; all bouts in specified year/month
  (GET "/bouts/:year/:month" [year month]
    (response (func/get-bouts-by-date year month)))
  
  ; all bouts in specified year
  (GET "/bouts/:year" [year]
    (response (func/get-bouts-by-date year)))

  (route/not-found "Not Found"))

(def app
  (wrap-json-response (wrap-cors app-routes #".*")))


  

  
