(ns sumo-backend.api.tournament)
(require '[sumo-backend.service.mysql :as db])
(require '[sumo-backend.utils :as utils])

;; Everything to do with Tournaments

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; List of all rikishi's results for a given tournament
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn build-rikishi-tournament-records
  "for a given tournament year and month, 
   compile all rikishi wins and losses for that tournament"
  [{:keys [year month]}]
  (map
    (fn [[rikishi results]]
      (merge 
        {:rikishi rikishi
         :results
           (cond
             (>= (:wins results) 8) (assoc results :result "kachikoshi")
             (< (:wins results) 8) (assoc results :result "machikoshi")
             :else (assoc results :result "rikishi match data incomplete"))}
        (db/get-rikishi-rank-in-tournament
          {:rikishi rikishi :year year :month month})))
    (reduce
      (fn [acc next]
        (let [rikishi-name   (:rikishi next)
              rikishi-in-acc (get acc rikishi-name)]
          (assoc
            acc
            rikishi-name
            (merge
              (or rikishi-in-acc {:wins 0 :losses 0})
              (dissoc next :rikishi)))))
      {}
      (concat
        (db/get-wins-in-tournament {:year year :month month})
        (db/get-losses-in-tournament {:year year :month month})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; List of specific rikishi's results over time
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-rikishi-results-over-time
  "returns rikishi results over all tournaments we have data for"
  ([{:keys [rikishi]}] ; top-level
    (if (db/rikishi-exists? rikishi)
      (get-rikishi-results-over-time
        rikishi
        (db/list-tournaments)
        [])
      {:error (str "Data does not exist for rikishi " rikishi)}))
  ([rikishi [{:keys [year month] :as tournament} & rest] results-over-time] ; inner fn
    (if (empty? tournament)
      results-over-time ; checked every tournament, return what you've got
      (let [wins (:wins
                  (first
                    (db/get-wins-in-tournament
                      {:year year :month month :rikishi rikishi})))
            losses (:losses
                    (first
                      (db/get-losses-in-tournament
                        {:year year :month month :rikishi rikishi})))]
        (get-rikishi-results-over-time
          rikishi
          rest
          (merge
            results-over-time
            (merge
              {:year year
               :month month
               :results 
                 {:wins wins
                  :losses losses
                  :result
                    (cond
                      (nil? wins) "rikishi match data incomplete"
                      (>= wins 8) "kachikoshi"
                      (< wins 8) "machikoshi")}}
              (db/get-rikishi-rank-in-tournament 
                {:rikishi rikishi :year year :month month}))))))))
  

