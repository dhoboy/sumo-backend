(ns sumo-backend.api.bout-list-rikishi-opponent
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.bout :refer [get-bout-list]]
    [sumo-backend.data.tournament :refer [rank-keyword-to-str]]))


;; all bouts :rikishi is in with :opponent.
;; takes optional :rank param for what rank rikishi was in the bout
;; takes optional :opponent_rank param for what rank opponent was in the bout
;; e.g. /bout/list/endo/takakeisho?winner=endo
(defn handler
  [rikishi
   opponent
   {:strs [winner loser technique technique_category rank opponent_rank
           is_playoff year month day page per]}]
  (response
    (get-bout-list
      (merge
        {:rikishi rikishi
         :opponent opponent
         :winner winner
         :loser loser
         :technique technique
         :technique-category technique_category
         :rank (rank-keyword-to-str rank)
         :opponent-rank (rank-keyword-to-str opponent_rank)
         :is-playoff is_playoff
         :year year
         :month month
         :day day}
        (if page {:page page} nil)
        (if per  {:per per} nil)))))
