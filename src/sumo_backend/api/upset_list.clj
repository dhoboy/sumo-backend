(ns sumo-backend.api.upset-list
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.bout :refer [get-bout-list]]))


;; all upsets where the rikishi ranks meet the passed in rank-delta
;; e.g. /upset/list?rank_delta=10&matchup=includes_larger&technique_category=push
;; - "give me all upsets of 10 rank levels or higher where the technique category was push"
(defn handler
  [{:strs [rank_delta matchup technique technique_category is_playoff year
           month day page per]}]
  (response
    (get-bout-list
      (merge
        {:rank-delta rank_delta
         :technique technique
         :technique-category technique_category
         :is-playoff is_playoff
         :year year
         :month month
         :day day
         :paginate true}
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
