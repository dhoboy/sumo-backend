(ns sumo-backend.api.rikishi-current-rank
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.tournament :refer [get-rikishi-current-rank]]
    [sumo-backend.utils :refer [paginate-list]]))


;; rikishi current rank
;; current rank is rank in last basho rikishi competed in
(defn handler
  [name {:strs [page per]}]
  (response
    (paginate-list
      (merge
        {:item-list [(get-rikishi-current-rank {:rikishi name})]}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))


(comment
  (println (get-rikishi-current-rank {:rikishi "ENDO"})))
