(ns sumo-backend.data-format)
(require '[cheshire.core :refer :all]) ; parses json
(require '[clojure.string :as str])
(require '[sumo-backend.technique :as technique])

; This namespace writes to files and transfers data between formats

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; * Writing technique info to datafiles *
;;   - atom for all techniques found in the datafiles,
;;     updated when new data is loaded.
;;     used in the function: add-techniques-to-datafiles
;;     e.g. of this atom with data:
;;       {"rear push out" {:en "Rear push out" 
;;                         :jp "Okuridashi"
;;                         :cat :push}
;;        "thrust down"   {:en "Thrust down"
;;                         :jp "Tsukiotoshi"
;;                         :cat :push}
;;        "okuridashi"    {:en "Rear push out"
;;                         :jp "Okuridashi"
;;                         :cat :push}
;;         "tsukiotoshi"  {:en "Thrust down"
;;                         :jp "Tsukiotoshi"
;;                         :cat :push}
;;        ...}
;;   - before loading data files into the database,
;;     you can load a repl and pass data filepaths to
;;     the add-techniques-to-datafiles fn below
;;     to update all files with technique, technique_ja, 
;;     and technique_category.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO -- i'm having issues changing namespaces
;; in the repl and accessing the vars inside

(def technique-info (atom {}))

(defn update-technique-info
  "derive all technique info from passed in file
   and update atom with any new technique info found"
  [data]
  (swap!
    technique-info
    (fn [current-state]
      (merge-with ; merge with by preserving what you have
        (fn [prev next] ; don't replace anything non-nil with nil
          {:en (or (:en prev) (:en next))
           :jp (or (:jp prev) (:jp next))
           :cat (or (:cat prev) (:cat next))})
        current-state
        (reduce ; reduce down all technique info from this file
          (fn [acc {:keys [technique technique_ja]}]
            (assoc ; assoc technique and technique_ja keys
             acc
             (str/lower-case (or technique "no-technique"))
             {:en technique
              :jp technique_ja
              :cat (technique/get-category technique_ja)}
             (str/lower-case (or technique_ja "no-technique_ja"))
             {:en technique
              :jp technique_ja
              :cat (technique/get-category technique_ja)}))
          {}
          (:data data))))))

(defn add-technique-to-datafiles
  "not all files have technique_ja and no files
   start with technique_category, backfill the
   files with this function"
  [filepath]
  (let [data (parse-string (slurp filepath) true)]
    (update-technique-info data)
    (spit ; write the technique info back to the file
      (clojure.string/replace filepath #".json" " copy.json")
      (generate-string 
        {:data 
         (map 
           (fn [{:keys [technique technique_ja technique_category] :as bout}]
             (if (or (nil? technique) 
                     (nil? technique_ja) 
                     (nil? technique_category)) ; technique data incomplete
               (apply 
                assoc
                bout
                (concat
                  (when (and (nil? technique) technique_ja)
                    [:technique (:en (get @technique-info (str/lower-case technique_ja)))])
                  (when (and (nil? technique_ja) technique)
                    [:technique_ja (:jp (get @technique-info (str/lower-case technique)))])
                  (when (and (nil? technique_category) (or technique technique_ja))
                    [:technique_category (:cat (get @technique-info
                                                    (or (str/lower-case technique) 
                                                        (str/lower-case technique_ja))))])))
               bout))
           (:data data))}
        {:pretty true}))))
