(ns sumo-backend.api.technique-details
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.technique :refer [get-all-losses-to-technique
                                         get-all-wins-by-technique]]
    [sumo-backend.utils :refer [add-percent-to-list paginate-list]]))


;; rikishi wins and losses by a certain technique
;; takes optional :year :month :day params
(defn handler
  [technique year month day page per]
  (response
    (paginate-list
      (merge
        {:item-list
         (conj
           []
           {:wins-by-technique
            (add-percent-to-list
              (get-all-wins-by-technique
                {:technique technique
                 :year year
                 :month month
                 :day day}))}
           {:losses-to-technique
            (add-percent-to-list
              (get-all-losses-to-technique
                {:technique technique
                 :year year
                 :month month
                 :day day}))})}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))


(comment
  (conj
    []
    {:wins-by-technique
      (add-percent-to-list
        (get-all-wins-by-technique
          {:technique "oshidashi"}))}
    {:losses-to-technique
      (add-percent-to-list
       (get-all-losses-to-technique
         {:technique "oshidashi"}))}))
