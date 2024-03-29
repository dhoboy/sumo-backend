(ns sumo-backend.api.rikishi-technique-breakdown
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.technique :refer [get-rikishi-losses-to-technique
                                         get-rikishi-losses-to-technique-category
                                         get-rikishi-wins-by-technique
                                         get-rikishi-wins-by-technique-category]]
    [sumo-backend.utils :refer [add-percent-to-list paginate-list]]))


;; TODO -- add in groups of wins / losses occuring at each rank?
;; basic stats on techniques and categories rikishi wins / looses by
(defn handler
  [name {:strs [year month day page per]}]
  (response
    (paginate-list
      (merge
        {:item-list
         (conj
           []
           {:wins_by_technique_category
            (add-percent-to-list
              (filter
                #(some? (:technique_category %))
                (get-rikishi-wins-by-technique-category
                  {:rikishi name :year year :month month :day day})))}
           {:wins_by_technique
            (add-percent-to-list
              (get-rikishi-wins-by-technique
                {:rikishi name :year year :month month :day day}))}
           {:losses_to_technique_category
            (add-percent-to-list
              (filter
                #(some? (:technique_category %))
                (get-rikishi-losses-to-technique-category
                  {:rikishi name :year year :month month :day day})))}
           {:losses_to_technique
            (add-percent-to-list
              (get-rikishi-losses-to-technique
                {:rikishi name :year year :month month :day day}))})}
        (when page {:page page})
        (when per {:per per})
        (when (and (nil? page) (nil? per)) {:all true})))))


(comment ;; add to Rikishi Detail page
  (println
    (conj
      []
      {:wins_by_technique_category
        (add-percent-to-list
          (get-rikishi-wins-by-technique-category
            {:rikishi "ENDO"}))}
      {:wins_by_technique
        (add-percent-to-list
          (get-rikishi-wins-by-technique
            {:rikishi "ENDO"}))}
      {:losses_to_technique_category
        (add-percent-to-list
          (get-rikishi-losses-to-technique-category
           {:rikishi "ENDO"}))}
      {:losses_to_technique
        (add-percent-to-list
          (get-rikishi-losses-to-technique
            {:rikishi "ENDO"}))})))


(comment
  (println
    (filter
      #(some? (:technique_category %))
      (get-rikishi-wins-by-technique-category
        {:rikishi "MITAKEUMI" :year 2021 :month 5 }))))
