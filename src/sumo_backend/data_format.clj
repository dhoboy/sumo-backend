(ns sumo-backend.data-format)
(require '[cheshire.core :refer :all]) ; parses json
(require '[clojure.string :as str])
(require '[sumo-backend.technique :as technique])
(require '[sumo-backend.utils :as utils])

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

;; TODO -- technique_ja is deprecated, move to using
;; technique, technique_en, technique_category
;; the japanese technique will be just called "technique"

(def technique-info (atom {}))

(def technique-info-filepath "./metadata/technique-info.json")

(defn technique-info-file-exists?
  "check that technique-info file exists"
  []
  (>
    (count
      (filter
        #(.isFile %)
        (utils/path->obj technique-info-filepath)))
    0))

(defn reset-technique-info
  "resets technique-info atom back to initial state"
  []
  (reset! technique-info {}))

;; parse string takes true after filepath to read in all keys as keywords
;; pass nothing after filepath to read in all keys as strings
;; pass custom function to decide how to read in keys
(defn initialize-technique-info
  "technique-info is backed up to a json file
   in the metadata folder. this function restores
   the values in that file to the atom"
  []
  (when technique-info-file-exists?
    (swap!
      technique-info
      merge
      (parse-string
        (slurp technique-info-filepath)
        (fn [k] ; keys of json objects in file
          (if (or (= k "en") (= k "jp") (= k "cat"))
            (keyword k)
            k))))))

(defn update-technique-info
  "derive all technique info from passed in bout datafile
   and update atom with any new technique info found,
   also saves to the technique-info.json file in metadata/"
  [data]
  (spit
    technique-info-filepath
    (generate-string
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
              (:data data)))))
      {:pretty true})))

(defn add-technique-to-basho-file
  "not all files have technique_ja and no files
   start with technique_category, backfills the
   file at the passed in filepath with more technique info"
  [filepath]
  (let [data (parse-string (slurp filepath) true)]
    (update-technique-info data)
    (spit ; write the technique info back to the datafile
      filepath
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

(defn add-technique-to-basho-dir
  "runs add-technique-to-basho-file over all files in passed in dir
   defaults to default-data-dir"
  [& args]
  (let [custom-path  (utils/path->obj (first args))
        default-path (utils/path->obj utils/default-data-dir)
        all-files    (->> (or custom-path default-path)
                          (filter #(.isFile %))
                          (map str)
                          (filter 
                            #(some? 
                              (re-find #".json$" %))))
        file-count   (count all-files)]
    (if (> file-count 0)
      (dorun
        (map add-technique-to-basho-file all-files))
      (println (str "File: '" (or (first args) utils/default-data-dir) "' not found")))))
