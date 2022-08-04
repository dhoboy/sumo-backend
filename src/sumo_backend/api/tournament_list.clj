(ns sumo-backend.api.tournament-list
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.tournament :refer [list-tournaments]]
    [sumo-backend.utils :refer [paginate-list]]))


;; list of all tournaments data exists for
(defn handler
  [page per]
  (response
    (paginate-list
      (merge
        {:item-list (list-tournaments)}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))
