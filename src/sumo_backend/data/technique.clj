(ns sumo-backend.data.technique
  (:require [honeysql.core :as sql]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [sumo-backend.utils :as utils]
            [sumo-backend.data.database :as db]))


;;
;; Namespace that categorizes techniques and provides info about them
;;


;; FYI: we will have to manually categorize
;; new techniques as we encounter them
;; by adding them to the appropriate map here
;; and then running data-format/add-technique-to-datafiles
;; to write these categories to the data files
(def categories
  {:force
   #{:tsuridashi
     :abisetaoshi
     :kimedashi
     :yoritaoshi
     :yorikiri}
   :push
   #{:tsukitaoshi
     :okuritaoshi
     :okuridashi
     :oshitaoshi
     :tsukidashi
     :tsukiotoshi
     :oshidashi}
   :throw
   #{:okurihikiotoshi
     :kainahineri
     :kakenage
     :tottari
     :komatasukui
     :shitatenage
     :katasukashi
     :uwatedashinage}
   :dodge
   #{:hikiotoshi
     :hatakikomi}
   :default
   #{:fusen}})


(defn get-category
  "given a technique key or technique_ja string,
   return what category it is in"
  [technique]
  (first
    (filter
      #(contains?
         (% categories)
         (if (keyword? technique)
           technique
           (utils/str->keyword technique)))
      (keys categories))))


(defn get-categories
  "return categories map"
  []
  categories)


;;
;; Database Queries
;;

(defn list-techniques
  "list all techniques used in bouts optionally constrained
   to passed in year, month, day params"
  [{:keys [year month day]}]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:technique :technique_en :technique_category]
          :modifiers [:distinct]
          :from :bout
          :where
          (if (or year month day)
            (concat
              [:and]
              (when year [[:= :year year]])
              (when month [[:= :month month]])
              (when day [[:= :day day]]))
            true))))
    (println "No Mysql DB")))


;; wins / losses for rikishi with technique
(defn get-rikishi-wins-by-technique
  "returns techniques rikishi has won by and frequency"
  [{:keys [rikishi year month day]}]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:technique :technique_en :technique_category [:%count.technique :count]]
          :modifiers [:distinct]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :winner rikishi]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by [:technique :technique_en :technique_category]
          :order-by [[:%count.technique :desc]])))
    (println "No Mysql DB")))


(comment
  (println (get-rikishi-wins-by-technique {:rikishi "ENDO"})))


(defn get-rikishi-wins-by-technique-category
  "returns technique categories rikishi has won by and frequency"
  [{:keys [rikishi year month day]}]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:technique_category [:%count.technique_category :count]]
          :modifiers [:distinct]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :winner rikishi]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :technique_category
          :order-by [[:%count.technique_category :desc]])))
    (println "No Mysql DB")))


(comment
  (println (get-rikishi-wins-by-technique-category {:rikishi "ENDO"})))


(defn get-rikishi-losses-to-technique
  "returns techniques rikishi has lost to and frequency"
  [{:keys [rikishi year month day]}]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:technique :technique_en :technique_category [:%count.technique :count]]
          :modifiers [:distinct]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :loser rikishi]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by [:technique :technique_en :technique_category]
          :order-by [[:%count.technique :desc]])))
    (println "No Mysql DB")))


(comment
  (println (get-rikishi-losses-to-technique {:rikishi "ENDO"})))


(defn get-rikishi-losses-to-technique-category
  "returns technique categories rikishi has lost to and frequency"
  [{:keys [rikishi year month day]}]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:technique_category [:%count.technique_category :count]]
          :modifiers [:distinct]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :loser rikishi]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :technique_category
          :order-by [[:%count.technique_category :desc]])))
    (println "No Mysql DB")))


;; all wins / losses for technique
(defn get-all-wins-by-technique
  "returns rikishi and number of times they have won with technique"
  [{:keys [technique year month day]}]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:winner [:%count.winner :count]]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :technique technique]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :winner
          :order-by [[:%count.winner :desc]])))
    (println "No Mysql DB")))


(defn get-all-wins-by-technique-category
  "returns rikishi and number of times they have won with technique category"
  [{:keys [category year month day]}]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:winner [:%count.winner :count]]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :technique_category category]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :winner
          :order-by [[:%count.winner :desc]])))
    (println "No Mysql DB")))


(defn get-all-losses-to-technique
  "returns rikishi and number of times they have lost to technique"
  [{:keys [technique year month day]}]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:loser [:%count.loser :count]]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :technique technique]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :loser
          :order-by [[:%count.loser :desc]])))
    (println "No Mysql DB")))


(defn get-all-losses-to-technique-category
  "returns rikishi and number of times they have lost to technique category"
  [{:keys [category year month day]}]
  (if-let [conn (db/db-conn)]
    (jdbc/query
      conn
      (sql/format
        (sql/build
          :select [:loser [:%count.loser :count]]
          :from :bout
          :where
          (concat
            [:and]
            [[:= :technique_category category]]
            (when year [[:= :year year]])
            (when month [[:= :month month]])
            (when day [[:= :day day]]))
          :group-by :loser
          :order-by [[:%count.loser :desc]])))
    (println "No Mysql DB")))


;; not sure if this is needed, leaving for now
(defn techniques-used
  "returns map of techniques used in the passed in tournament year and month.
   map keys are the technique Japanese name for each technique
   e.g. {:oshidashi {:jp 'oshidashi' :en 'Frontal push out' :cat 'push'}}"
  [{:keys [year month]}]
  (if-let [conn (db/db-conn)]
    (reduce
      (fn [acc {:keys [technique technique_en technique_category]}]
        (assoc
          acc
          (keyword (clojure.string/lower-case technique_en))
          {:en technique_en :jp technique :cat technique_category}))
      {}
      (filter
        #(some? (:technique %))
        (jdbc/query
          conn
          (sql/format
            (sql/build
              :select [:technique :technique_en :technique_category]
              :modifiers [:distinct]
              :from :bout
              :where
              [:and
               [:= :year year]
               [:= :month month]])))))
    (println "No Mysql DB")))
