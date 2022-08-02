(ns sumo-backend.api.rikishi-rank-over-time
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.tournament :refer [get-rikishi-rank-over-time]]
    [sumo-backend.utils :refer [paginate-list]]))


;; list of rikishi's rank changes over time
(defn handler
  [name page per]
  (response
    (paginate-list
      (merge
        {:item-list (get-rikishi-rank-over-time {:rikishi name})}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))


(comment ;; add to Rikishi Detail page
  (println (get-rikishi-rank-over-time {:rikishi "ENDO"})))
