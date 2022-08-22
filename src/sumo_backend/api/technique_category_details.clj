(ns sumo-backend.api.technique-category-details
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.technique :refer [get-all-losses-to-technique-category
                                         get-all-wins-by-technique-category]]
    [sumo-backend.utils :refer [add-percent-to-list paginate-list]]))


;; rikishi wins and losses by a certain technique category
;; takes optional :year :month :day params
(defn handler
  [category {:strs [year month day page per]}]
  (response
    (paginate-list
      (merge
        {:item-list
         (conj
           []
           {:rikishi-wins-by-technique-category
            (add-percent-to-list
              (get-all-wins-by-technique-category
                {:category category
                 :year year
                 :month month
                 :day day}))}
           {:rikishi-losses-to-technique-category
            (add-percent-to-list
              (get-all-losses-to-technique-category
                {:category category
                 :year year
                 :month month
                 :day day}))})}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))


(comment
  (conj
    []
    {:rikishi-wins-by-technique-category
     (add-percent-to-list
       (get-all-wins-by-technique-category
         {:category "force"}))}
    {:rikishi-losses-to-technique-category
     (add-percent-to-list
       (get-all-losses-to-technique-category
         {:category "force"}))}))
