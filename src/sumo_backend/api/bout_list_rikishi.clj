(ns sumo-backend.api.bout-list-rikishi
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.bout :refer [get-bout-list]]
    [sumo-backend.data.tournament :refer [rank-keyword-to-str]]))


;; TODO: not sure if this should be a separate endpoint or combined with bout_list

;; all bouts :rikishi is in.
;; takes optional :rank param for what rank rikishi was in the bout
;; e.g. /bout/list/endo?rank=maegashira_1&year=2020&month=1&day=1&per=1&page=1
(defn handler
  [rikishi
   {:strs [winner loser technique technique_category rank is_playoff year month
           day page per]}]
  (response
    (get-bout-list
      (merge
        {:rikishi rikishi
         :winner winner
         :loser loser
         :technique technique
         :technique-category technique_category
         :rank (rank-keyword-to-str rank)
         :is-playoff is_playoff
         :year year
         :month month
         :day day
         :paginate true}
        (if page {:page page} nil)
        (if per  {:per per} nil)))))
