(ns sumo-backend.api.upset-loose
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.bout :refer [get-bout-list]]))


;; get all bouts where :rikishi was upset (lost to lower ranked opponent)
(defn handler
  [rikishi
   rank_delta
   matchup
   technique
   technique_category
   is_playoff
   year
   month
   day
   page
   per]
  (response
    (get-bout-list
      (merge
        {:loser rikishi
         :rank-delta rank_delta
         :technique technique
         :technique-category technique_category
         :is-playoff is_playoff
         :year year
         :month month
         :day day}
        ;; :paginate true}
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
