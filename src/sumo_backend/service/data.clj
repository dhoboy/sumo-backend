(ns sumo-backend.service.data)
(require '[sumo-backend.utils :as utils])
(require '[clojure.string :as str])
(require '[simple-time.core :as time])
(require '[clj-http.client :as http-client])
(require '[cheshire.core :refer :all]) ; parses json
(require '[sumo-backend.api.technique :as technique])
(require '[clojure.core.async :as async :refer [>! >!! <! <!! go go-loop chan alt! timeout]])

; This namespace fetches and formats basho data as needed for use in this project

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; * Updating technique info on basho data *
;;   - atom for all techniques the system has so far,
;;     it gets updated when new data is loaded.
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
;;        "tsukiotoshi"  {:en "Thrust down"
;;                         :jp "Tsukiotoshi"
;;                         :cat :push}
;;        ...}
;;   - when updating existing files,
;;     before loading them into the database,
;;     you can load a repl and pass update functions
;;     and data filepaths to update-basho-dir
;;     to update all files with: technique, technique_en, 
;;     and technique_category.
;;   - there is also a pipeline of jobs that will fetch and process
;;     new data and update it before saving to a file.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Managing the technique-info atom
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
;; pass parse string a custom function to decide how to read in keys
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
  "derive all technique info from passed in bout data array
   and update atom with any new technique info found
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
            (reduce ; reduce down all technique info from this data
              (fn [acc {:keys [technique technique_en]}]
                (assoc ; assoc technique and technique_en keys
                  acc
                  (str/lower-case (or technique "no-technique"))
                  {:en technique_en
                   :jp technique
                   :cat (technique/get-category technique)}
                  (str/lower-case (or technique_en "no-technique_en"))
                  {:en technique_en
                   :jp technique
                   :cat (technique/get-category technique)}))
              {}
              data))))
            {:pretty true})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Re-naming and classifying techniques in bouts
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn rename-bout-technique-keys
  "given a bout, re-names:
   'technique' -> 'technique_en', and
   'technique_ja' -> 'technique'. 
   if bout technique keys have already been renamed, 
   just returns them as is"
  [{:keys [technique technique_ja technique_en] :as bout}]
  (if (and (nil? technique_en) (some? technique_ja))
    (assoc
      (dissoc bout :technique :technique_ja)
      :technique technique_ja
      :technique_en technique)
    bout))

(defn complete-bout-technique-info
  "given a bout, adds technique, technique_en, 
   and technique_category from technique-info atom
   if bout is missing any of them, otherwise returns the bout as is.
   Assumes technique and technique_en keys. Run rename-bout-technique-keys 
   before running this to update the keys."
   [{:keys [technique technique_en technique_category] :as bout}]
   (if (or (nil? technique)
           (nil? technique_en)
           (nil? technique_category))
     (apply ; bout technique info is incomplete, complete it
       assoc
       bout
       (concat
         (when (and (nil? technique) technique_en)
           [:technique (:jp (get @technique-info (str/lower-case technique_en)))])
         (when (and (nil? technique_en) technique)
           [:technique_en (:en (get @technique-info (str/lower-case technique)))])
         (when (and (nil? technique_category) (or technique technique_en))
           [:technique_category (:cat (get @technique-info
                                        (or (str/lower-case technique)
                                            (str/lower-case technique_en))))])))
     bout)) ; bout technique info is complete, just return bout

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; REPL functions for updating existing files
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; available :func for update-basho-dir
(defn update-technique-keys-in-basho-file
  "takes a filepath and renames 
   'technique' -> 'technique_en' and
   'technique_ja' -> 'technique'
   for all bouts in the file"
  [filepath]
  (let [document (parse-string (slurp filepath) true)]
    (spit ; write the updated data back to the datafile
      filepath
      (generate-string
        {:data (map rename-bout-technique-keys (:data document))}
        {:pretty true}))))

; available :func for update-basho-dir
; don't forget to run initialize-technique-info before running this
(defn add-technique-to-basho-file
  "not all files have technique_en and no files
   start with technique_category, backfills the
   file at the passed in filepath with more technique info"
  [filepath]
  (let [document (parse-string (slurp filepath) true)]
    (update-technique-info (:data document))
    (spit ; write the technique info back to the datafile
      filepath
      (generate-string 
        {:data (map complete-bout-technique-info (:data document))}
        {:pretty true}))))

(defn update-basho-dir
  "runs passed in func over all files in passed in dir,
   defaults to default-data-dir"
  [{:keys [func dir]}]
  (if (nil? func)
    (println "Must pass a function :func to update-basho-dir")
    (let [custom-path  (utils/path->obj dir)
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
          (map func all-files))
        (println (str "File: '" (or dir utils/default-data-dir) "' not found"))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core/Async Pipeline for Fetching, formating, and writing new data
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; all these channels live outside of the jobs that use them.
;; each job parking takes from the channel it reads from,
;; does it's work, and puts its finished work on the next channel
;; in the sequence.

;; these independent channels holding the work that 
;; got put there from jobs is the "pipeline of jobs".

;; each one of these independent processes, like fetch-data is a "job".

;;;;;;;;;;;;;
;; Channels 
;;;;;;;;;;;;;

(def fetch-chan (chan))  ;; holds {:year 2021 :month 7 :day 1} maps to fetch data for
(def update-chan (chan)) ;; holds parsed response documents to update
(def write-chan (chan))  ;; holds updated documents to write to file

;;;;;;;;;;
;; Fetch
;;;;;;;;;;

(def fetch-error-log "./metadata/fetch-errors.log")

; helper
(defn log-error
  "convenience wrapper for spitting to fetch error log"
  [message]
    (println message)
    (spit
      fetch-error-log
      (str 
        (time/format (time/now) :date-time) 
        " - " 
        message 
        "\n")
      :append true))

; helper 
(defn build-fetch-url
  "takes { :year :month :day } map and builds fetch url"
  [{:keys [year month day]}]
  (str
    "https://www3.nhk.or.jp/nhkworld/en/tv/sumo/tournament/"
    year
    (utils/zero-pad month)
    "/day"
    day
    ".json"))

; helper 
(defn handle-fetch-response
  "puts parsed success responses on update-chan
   and logs out returned errors"
  [resp date url]
  (if (= (:status resp) 200)  
    (go
      (>!
        update-chan
        (with-meta (parse-string (:body resp) true) date)))
    (log-error (str "Data fetch for " date " at url " url " errored with response " resp))))

; job
(defn fetch-data
  "takes from fetch-chan, fetches data for a given tournament day, 
   handles timeouts, and puts non-timed out calls on update-chan"
  []
  (go-loop [date (<! fetch-chan)] ; parking take is initial binding
    ;; build the call, put on call chan, alt! with a timeout, handle response
    (let [call (chan)
          url (build-fetch-url date)]
      (go 
        (>! 
          call 
          (try 
            (http-client/get url)
            (catch Exception e (str "Exception: " (.getMessage e))))))
      (alt!
        (timeout 5000) (log-error (str "Data fetch for " date " at url " url " timed out in 5000ms"))
        call ([resp] (handle-fetch-response resp date url))))
    (recur (<! fetch-chan)))) ; re-bind with a parking take of the next date

;;;;;;;;;;;;
;; Update 
;;;;;;;;;;;;

; job
(defn update-data
  "pulls from update-chan and updates the data,
   updates the shared technique-info atom, and
   puts updated data document onto write-chan"
  []
  (go-loop [document (<! update-chan)]
    (let [data-with-renamed-keys (map rename-bout-technique-keys (:data document))]
      (update-technique-info data-with-renamed-keys)
      (>!
        write-chan
        (assoc
          document
          :data (map complete-bout-technique-info data-with-renamed-keys))))
    (recur (<! update-chan))))

;;;;;;;;;;;;
;; Write
;;;;;;;;;;;;

; helper
(defn get-new-bout-data-filename
  "for the passed in :year, :month, and :day
   generate the filename where this data will be written"
  [{:keys [year month day]}]
  (str "day" day "__" (utils/zero-pad month) "_" year ".json"))

; job
(defn write-data
  "pulls from write-chan, writes the document to file"
  []
  (go-loop [document (<! write-chan)] 
    (let [{:keys [year month day] :as date} (meta document)
          filedir (str utils/default-data-dir "/" year "/" (utils/zero-pad month) "/")
          filename (get-new-bout-data-filename date)]
      (when (not (.exists (clojure.java.io/file filedir)))
        (.mkdir (clojure.java.io/file filedir)))
      (spit
        (str filedir filename)
        (generate-string document {:pretty true}))
      (println "File" (str filedir filename) "written!"))
    (recur (<! write-chan))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Start Fetch->Update->Write Pipeline
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn start-data-pipeline
  "Start the data fetch->update->write pipeline.
   Blocking put {:year :month :day} maps on the 
   fetch-chan to begin the process"
  []
  (when (empty? @technique-info) (initialize-technique-info))
  (fetch-data)
  (update-data)
  (write-data))
