(ns sumo-backend.data.bout
  (:require
    [clojure.java.jdbc :as jdbc]
    [honeysql.core :as sql]
    [sumo-backend.data.database :as db]))


;;
;; Namespace that provides optionally paginated Bout List queries
;;

;; build bout list queries
(defn build-rikishi-bout-history-query
  "given a :rikishi and :opponent, returns all bouts between the two.
   optionally takes--
     :winner, :loser, :rank, :opponent-rank,
     :is-playoff, :year, :month, :day, and :total-only params"
  [{:keys [rikishi opponent winner loser technique technique-category
           rank opponent-rank is-playoff year month day total-only]}]
  [:select (if (some? total-only) [[:%count.* :total]] :*)
   :from :bout
   :where
   (concat
     [:and
      [:or
       [:and [:= :east rikishi] [:= :west opponent]]
       [:and [:= :east opponent] [:= :west rikishi]]]]
     (when winner [[:= :winner winner]])
     (when loser [[:= :loser loser]])
     (when technique [[:= :technique technique]])
     (when technique-category [[:= :technique_category technique-category]])
     (when rank [[:or
                  [:and [:= :east rikishi] [:= :east_rank rank]]
                  [:and [:= :west rikishi] [:= :west_rank rank]]]])
     (when opponent-rank [[:or
                           [:and [:= :east opponent] [:= :east_rank opponent-rank]]
                           [:and [:= :west opponent] [:= :west_rank opponent-rank]]]])
     (when is-playoff [[:= :is_playoff 1]]) ; fight twice on same day to break tie
     (when year [[:= :year year]])
     (when month [[:= :month month]])
     (when day [[:= :day day]]))])


(defn build-bouts-by-rikishi-query
  "gets all bouts by :rikishi.
   optionally takes--
     :winner, :loser, :rank, :is-playoff,
     :year, :month, :day, and :total-only params"
  [{:keys [rikishi winner loser technique technique-category
           rank is-playoff year month day total-only]}]
  [:select (if (some? total-only) [[:%count.* :total]] :*)
   :from :bout
   :where
   (let [rikishi-clause [:or [:= :east rikishi] [:= :west rikishi]]]
     (if (or winner loser technique technique-category rank is-playoff year month day)
       (concat
         [:and rikishi-clause]
         (when winner [[:= :winner winner]])
         (when loser [[:= :loser loser]])
         (when technique [[:= :technique technique]])
         (when technique-category [[:= :technique_category technique-category]])
         (when rank [[:or
                      [:and [:= :east rikishi] [:= :east_rank rank]]
                      [:and [:= :west rikishi] [:= :west_rank rank]]]])
         (when is-playoff [[:= :is_playoff 1]]) ; fight twice on same day to break tie
         (when year [[:= :year year]])
         (when month [[:= :month month]])
         (when day [[:= :day day]]))
       rikishi-clause))])


(defn build-rikishi-bouts-against-rank-query
  "gets all bouts by :rikishi against :against-rank.
   optionally takes--
     :at-rank, :winner, :loser, :technique, :technique-category,
     :comparison, :is-playoff, :year, :month, :day, and :total-only params"
  [{:keys [rikishi against-rank against-rank-value at-rank comparison winner loser
           technique technique-category is-playoff year month day total-only]}]
  [:select (if (some? total-only) [[:%count.* :total]] :*)
   :from :bout
   :where
   (let [rikishi-clause [:or [:= :east rikishi] [:= :west rikishi]]
         against-rank-clause (if (and
                                   (some? comparison)
                                   (some? against-rank-value)
                                   (not= comparison "="))
                               [:or
                                [:and
                                 [:= :east rikishi]
                                 [(keyword comparison)
                                  :west_rank_value against-rank-value]]
                                [:and
                                 [:= :west rikishi]
                                 [(keyword comparison)
                                  :east_rank_value against-rank-value]]]
                               [:or
                                [:and [:= :east rikishi] [:= :west_rank against-rank]]
                                [:and [:= :west rikishi] [:= :east_rank against-rank]]])]
     (if (or at-rank winner loser technique technique-category is-playoff year month day)
       (concat
         [:and rikishi-clause against-rank-clause]
         (when winner [[:= :winner winner]])
         (when loser [[:= :loser loser]])
         (when technique [[:= :technique technique]])
         (when technique-category [[:= :technique_category technique-category]])
         (when at-rank [[:or
                         [:and [:= :east rikishi] [:= :east_rank at-rank]]
                         [:and [:= :west rikishi] [:= :west_rank at-rank]]]])
         (when is-playoff [[:= :is_playoff 1]]) ; rikishi face each other twice on same day to break tie
         (when year [[:= :year year]])
         (when month [[:= :month month]])
         (when day [[:= :day day]]))
       [:and rikishi-clause against-rank-clause]))])


(defn build-upset-query
  "gets all bouts that are an upset of specified :rank-delta.
   optionally takes--
   (or :winner :loser), :comparison, :is-playoff, :technique
   :technique-category, :year, :month, :day, and :total-only params"
  [{:keys [winner loser rank-delta comparison technique technique-category
           is-playoff year month day total-only] :or {comparison "="}}]
  [:select (if (some? total-only) [[:%count.* :total]] :*)
   :from :bout
   :where
   (let [all-upsets-clause [:or
                            [(keyword comparison)
                             [:- :west_rank_value :east_rank_value] rank-delta]
                            [(keyword comparison)
                             [:- :east_rank_value :west_rank_value] rank-delta]]
         winner-clause [:and
                        [:= :winner winner]
                        [:or
                         [:and
                          [:= :east winner]
                          [(keyword comparison)
                           [:- :east_rank_value :west_rank_value] rank-delta]]
                         [:and
                          [:= :west winner]
                          [(keyword comparison)
                           [:- :west_rank_value :east_rank_value] rank-delta]]]]
         loser-clause [:and
                       [:= :loser loser]
                       [:or
                        [:and
                         [:= :east loser]
                         [(keyword comparison)
                          [:- :west_rank_value :east_rank_value] rank-delta]]
                        [:and
                         [:= :west loser]
                         [(keyword comparison)
                          [:- :east_rank_value :west_rank_value] rank-delta]]]]]
     (concat
       [:and]
       (when (and (nil? winner) (nil? loser)) [all-upsets-clause])
       (when winner [winner-clause])
       (when loser [loser-clause])
       (when technique [[:= :technique technique]])
       (when technique-category [[:= :technique_category technique-category]])
       (when is-playoff [[:= :is_playoff 1]]) ; fight twice on same day to break tie
       (when year [[:= :year year]])
       (when month [[:= :month month]])
       (when day [[:= :day day]])))])


(defn build-bouts-by-date-query
  "gets all bouts.
   optionally takes--
     :winner, :loser, :is-playoff,
     :year, :month, :day, and :total-only params"
  [{:keys [winner loser technique technique-category is-playoff
           year month day total-only]}]
  [:select (if (some? total-only) [[:%count.* :total]] :*)
   :from :bout
   :where
   (if (or winner loser technique technique-category is-playoff year month day)
     (concat
       [:and]
       (when winner [[:= :winner winner]])
       (when loser [[:= :loser loser]])
       (when technique [[:= :technique technique]])
       (when technique-category [[:= :technique_category technique-category]])
       (when is-playoff [[:= :is_playoff 1]]) ; fight twice on same day to break tie
       (when year [[:= :year year]])
       (when month [[:= :month month]])
       (when day [[:= :day day]]))
     true)])


;; runs bout-list query against the database, with optional limit and offset pagination
(defn run-bout-list-query
  "returns a bout list using the appropriate query, optionally paginated"
  [{:keys [rikishi opponent against-rank rank-delta page per] :as params}]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (apply sql/build
          (apply merge
            (cond ; bring in appropriate query given passed in params
              (and rikishi opponent) (build-rikishi-bout-history-query params)
              (and rikishi against-rank) (build-rikishi-bouts-against-rank-query params)
              rank-delta (build-upset-query params)
              rikishi (build-bouts-by-rikishi-query params)
              :else (build-bouts-by-date-query params))
            (when (and page per) ; optionally add pagination
              [:order-by [[:year :desc] [:month :desc] [:day :asc]]
               :limit (Integer/parseInt per)
               :offset (* (- (Integer/parseInt page) 1) (Integer/parseInt per))])))))
    (println "No Mysql DB")))


;; top level bout-list-by-criteria function
;; @bslawski, should I memoize fns like this that make DB calls?
(defn get-bout-list
  "given a set of criteria, returns a bout list.
   pass { :paginate true } to get the response paginated.
   also takes :page and :per string params to step through response pages"
  [{:keys [paginate page per] :or {page "1" per "15"} :as params}]
  (if paginate
    {:pagination
     {:page (Integer/parseInt page)
      :per (Integer/parseInt per)
      :total (:total (first (run-bout-list-query (merge {:total-only true} params))))}
     :items (run-bout-list-query (merge {:page page :per per} params))}
    (run-bout-list-query params)))
