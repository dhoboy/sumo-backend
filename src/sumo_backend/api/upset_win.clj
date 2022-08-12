(ns sumo-backend.api.upset-win
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.bout :refer [get-bout-list]]))


;; get all upsets that :rikishi won (defeated higher ranked opponent)
(defn handler
  [rikishi
   {:strs [rank_delta matchup technique technique_category is_playoff year
           month day page per]}]
  (response
    (get-bout-list
      (merge
        {:winner rikishi
         :rank-delta rank_delta
         :technique technique
         :technique-category technique_category
         :is-playoff is_playoff
         :year year
         :month month
         :day day}
        (when (= matchup "includes_larger")
          {:comparison ">="})
        (when (= matchup "larger_only")
          {:comparison ">"})
        (when (= matchup "includes_smaller")
          {:comparison "<="})
        (when (= matchup "smaller_only")
          {:comparison "<"})
        (when page {:page page})
        (when per {:per per})))))
