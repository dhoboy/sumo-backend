(ns sumo-backend.api.rikishi-results-over-time
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.tournament :refer [get-rikishi-results-over-time]]
    [sumo-backend.utils :refer [paginate-list]]))


;; list of rikishi tournament results over time
(defn handler
  [name {:strs [page per]}]
  (response
    (paginate-list
      (merge
        {:item-list (get-rikishi-results-over-time {:rikishi name})}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))


(comment ;; add to Rikishi Detail page... later maybe
  (println (get-rikishi-results-over-time {:rikishi "ENDO"})))
