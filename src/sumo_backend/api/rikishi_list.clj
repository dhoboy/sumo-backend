(ns sumo-backend.api.rikishi-list
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.rikishi :refer [list-rikishi]]
    [sumo-backend.data.tournament :refer [get-rikishi-current-rank]]
    [sumo-backend.utils :refer [paginate-list]]))


;; list of all rikishi

(defn handler
  [page per]
  (response
    (paginate-list
      (merge
        {:item-list
         (map
           #(assoc
              %
              :rank (get-rikishi-current-rank {:rikishi (:name %)}))
           (list-rikishi))}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))
