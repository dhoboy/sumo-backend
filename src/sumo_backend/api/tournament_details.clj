(ns sumo-backend.api.tournament-details
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.tournament :refer [build-rikishi-tournament-records]]
    [sumo-backend.utils :refer [paginate-list]]))


;; TODO -- add tournament champion and location.
;; details about tournament, e.g. rikishi records
(defn handler
  [year month {:strs [page per]}]
  (response
    (paginate-list
      (merge
        {:item-list (build-rikishi-tournament-records {:year year :month month})}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))
