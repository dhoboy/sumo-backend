(ns sumo-backend.api.technique-list
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.technique :refer [list-techniques]]
    [sumo-backend.utils :refer [paginate-list]]))


;; technique list
;; takes optional :year :month :day params
;; defaults to returning all sets of--
;; technique, technique_en, and technique_category
;; found in database, else returns them for
;; specified :year, :month, :day
(defn handler
  [year month day page per]
  (response
    (paginate-list
      (merge
        {:item-list (list-techniques {:year year :month month :day day})}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))
