(ns sumo-backend.api.rikishi-details
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.rikishi :refer [get-rikishi]]
    [sumo-backend.utils :refer [paginate-list]]))


;; specific rikishi record
(defn handler
  [name page per]
  (response
    (paginate-list
      (merge
        {:item-list (get-rikishi name)}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))


(comment
  (println (get-rikishi "ENDO")))
