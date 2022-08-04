(ns sumo-backend.api.fare-loose
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.bout :refer [get-bout-list]]
    [sumo-backend.data.tournament :refer [get-rank-value rank-keyword-to-str]]))


;; losses :rikishi had to :against_rank
(defn handler
  [rikishi
   against_rank
   at_rank
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
        {:rikishi rikishi
         :against-rank (rank-keyword-to-str against_rank)
         :against-rank-value (get-rank-value {:rank against_rank :year year :month month})
         :at-rank (rank-keyword-to-str at_rank)
         :loser rikishi
         :technique technique
         :technique-category technique_category
         :is-playoff is_playoff
         :year year
         :month month
         :day day}
        ;; :paginate true} ; higher ranks have lower rank-value
        (when (= matchup "includes_higher_ranks")
          {:comparison "<="})
        (when (= matchup "higher_ranks_only")
          {:comparison "<"})
        (when (= matchup "includes_lower_ranks")
          {:comparison ">="})
        (when (= matchup "lower_ranks_only")
          {:comparison ">"})
        (when page {:page page})
        (when per {:per per})))))
