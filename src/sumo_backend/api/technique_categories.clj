(ns sumo-backend.api.technique-categories
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.technique :refer [get-categories]]
    [sumo-backend.utils :refer [paginate-list]]))


;; list of all categories and technique keys classified within
(defn handler
  [page per]
  (response
    (paginate-list
      (merge
        {:item-list (get-categories)}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))


(comment
  (println (get-categories)))
