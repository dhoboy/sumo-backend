(ns sumo-backend.api.fare-list
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.bout :refer [get-bout-list]]
    [sumo-backend.data.tournament :refer [get-rank-value rank-keyword-to-str]]))


;; bout list of :rikishi vs :against_rank,
;; e.g. /fare/endo/ozeki?&matchup=higher_ranks_only
;;  - "give me all bouts where endo faced higher rank than ozeki"
;; e.g. /fare/endo/ozeki?at_rank=maegashira_1&matchup=includes_lower_ranks
;;  - "give me all bouts where endo was maegashira #1 and faced ozeki or lower rank"
(defn handler
  [rikishi
   against_rank
   {:strs [at_rank matchup technique technique_category is_playoff year month
           day page per]}]
  (response
    (get-bout-list
      (merge
        {:rikishi rikishi
         :against-rank (rank-keyword-to-str against_rank)
         :against-rank-value (get-rank-value {:rank against_rank :year year :month month})
         :at-rank (rank-keyword-to-str at_rank)
         :technique technique
         :technique-category technique_category
         :is-playoff is_playoff
         :year year
         :month month
         :day day
         :paginate true}
        ;; higher ranks have lower rank-value
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
