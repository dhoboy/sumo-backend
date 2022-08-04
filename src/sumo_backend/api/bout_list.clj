(ns sumo-backend.api.bout-list
  (:require
    [ring.util.response :refer [response]]
    [sumo-backend.data.bout :refer [get-bout-list]]))


;; all bouts.
;; e.g. /bout/list?year=2021&winner=endo
(defn handler
  [winner
   loser
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
        {:winner winner
         :loser loser
         :technique technique
         :technique-category technique_category
         :is-playoff is_playoff
         :year year
         :month month
         :day day}
        ;; :paginate true}
        (if page {:page page} nil)
        (if per  {:per per} nil)))))
