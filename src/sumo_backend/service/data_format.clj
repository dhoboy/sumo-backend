(ns sumo-backend.service.data-format)
(require '[sumo-backend.utils :as utils])
(require '[clojure.string :as str])
(require '[clj-http.client :as http-client])
(require '[cheshire.core :refer :all]) ; parses json
(require '[sumo-backend.api.technique :as technique])
(require '[clojure.core.async :as async :refer [>! <! >!! <!! go chan buffer
                                                sliding-buffer close! thread
                                                alt! alt!! alts! alts!! timeout]])

; This namespace formats basho data as needed for use in this project

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; * Updating technique info on data *
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
;;     you can load a repl and pass data filepaths to
;;     the add-techniques-to-datafiles fn below
;;     to update all files with: technique, technique_en, 
;;     and technique_category.
;;   - there is also a pipeline of jobs that will process all newly
;;     fetched data and update it before saving to a file.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO -- technique_ja is deprecated, move to using
;; technique, technique_en, technique_category
;; the japanese technique will be just called "technique"

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
  "derive all technique info from passed in bout data
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
              (:data data)))))
            {:pretty true})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; REPL functions for updating existing files
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn add-technique-to-basho-file
  "not all files have technique_en and no files
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
           (fn [{:keys [technique technique_en technique_category] :as bout}]
             (if (or (nil? technique)
                     (nil? technique_en)
                     (nil? technique_category)) ; technique data incomplete
               (apply 
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Clojure.core/async Pipeline for fetching new data
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; download single day's match data
(defn fetch-data
  "fetches data for a given tournament day
   returns the response on a channel."
  [{:keys [year month day] :or {year 2021 month 7 day 1}}]
  (let [out  (chan)
        call (chan)]
    ;; put http call onto call channel
    (go
      (>!
       call
       (http-client/get
         (str
           "https://www3.nhk.or.jp/nhkworld/en/tv/sumo/tournament/"
           year
           (utils/zero-pad month)
           "/day"
           day
           ".json"))))
    ;; put alt of the 5 sec timeout and call on out channel
    (go
      (>!
       out
       (alt! 
         (timeout 5000) "timed out"
         call ([resp] resp))))
    out))

(defn fetch-data-no-alt
  "fetches data for a given tournament day
   returns the response on a channel."
  [{:keys [year month day] :or {year 2021 month 7 day 1}}]
  (let [out (chan)]
    (go
      (>!
       out
       (http-client/get
         (str
           "https://www3.nhk.or.jp/nhkworld/en/tv/sumo/tournament/"
           year
           (utils/zero-pad month)
           "/day"
           day
           ".json"))))
    out))

;; parse single day's response body into clojure data structures
(defn parse-response
  "parses fetched data response passed in on in channel
   puts it on out channel"
  [in]
  (let [out (chan)
        resp (<!! in)] ; let [resp (go (<! in))] errors with "can't put nil on channel" @blawski?
    (go ;(while true ; i think these while true's cause infinite loops to spin out of control?
      (>!
       out
       (cond 
         (= (:status resp) 200) (parse-string (:body resp) true)
         (= resp "timed out") (println "handle this timeout")
         :else (println "handle this error case: " resp))));)
    out))
    

(defn handle-failure-response
  "handle when fetch fails and log out to a file"
  []
  (println "coming soon"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Update response with technique info and rename keys
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn rename-technique-keys
  "renames rename 'technique' to 'technique_en' and 
   'technique_ja' to 'technique' on data passed in on channel"
  [in]
  (let [out (chan)]
    (go ;(while true
      (>!
        out
        {:data
         (map
           (fn [{:keys [technique technique_ja] :as bout}]
             (assoc
               (dissoc bout :technique :technique_ja)
               :technique technique_ja
               :technique_en technique))
           (:data (<! in)))}));)
    out))

; i dont think this is needed, can just use technique-info atom
(defn technique-info-channel
  "initializes from technique-info file, if passed an 
   in channel, updates technique-info with the data
   on that channel. uses technique-info atom under the hood"
  [{:keys [in] :or {in (timeout 100)}}]
  (let [out (chan)]
    (go (while true
      (>!
        out
        (alt!
          in ([info-to-add] (update-technique-info info-to-add))
          :default (if (empty? @technique-info)
                     (initialize-technique-info)
                     @technique-info)))))
    out))

;; also make this update technique-info shared file
;; this output channel contains things to merge into 
;; technique-info-channel
(defn technique-update
  "updates any missing technique info to technique-info atom
   and adds technique-category to each bout in data"
  [in]
  (let [out (chan (sliding-buffer 1))
        info (if (empty? @technique-info)
               (initialize-technique-info)
               @technique-info)]
    (go ;(while true
      (>!
        out
        {:data
         (map
          (fn [{:keys [technique technique_en] :as bout}]
            (apply
              assoc
              bout
              (concat
                (when (and (nil? technique) technique_en)
                  [:technique (:jp (get info (str/lower-case technique_en)))])
                (when (and (nil? technique_en) technique)
                  [:technique_en (:en (get info (str/lower-case technique)))])
                (when (or technique technique_en)
                  [:technique_category (:cat (get info
                                                (or (str/lower-case technique)
                                                  (str/lower-case technique_en))))]))))
          (:data (<! in)))}));)
    out))
  
(defn update-technique-info-from-channels
  "takes updated data from in and writes it to technique-info atom
   and puts it back on the out channel"
  [in]
  (let [out (chan)
        data (<!! in)]
    ;(go (println (<! in))) ; this println works
    ;(go (update-technique-info (<! in))) 
    ;(go (>! out (<! in))) ; this hangs forever, b/c i already took from the channel?
    (update-technique-info data)
    (go (>! out data))
    out))

(defn get-new-bout-data-filename 
  "for the passed in :year, :month, and :day
   generate the filename where this data will be written"
  [{:keys [year month day]}]
  (str "day" day "__" (utils/zero-pad month) "_" year ".json"))
  
(defn get-new-bout-data
  "starts processes for getting new bout data 
   and saving it to a file"
  [{:keys [year month day] :as date}]
  (let [filedir (str utils/default-data-dir "/" year "/" (utils/zero-pad month) "/")
        filename (get-new-bout-data-filename date)
        data (-> date
                 fetch-data
                 parse-response
                 rename-technique-keys
                 technique-update
                 update-technique-info-from-channels
                 <!!)]
    (when (not (.exists (clojure.java.io/file filedir)))
      (.mkdir (clojure.java.io/file filedir)))
    (spit 
      (str filedir filename)
      (generate-string data {:pretty true}))
    (println "File" (str filedir filename) "written!")))


;; this just started running like an infinite loop when they had while true's in them--
;; set up pipeline of all these channels
;; (def date-chan (chan))
;; (def fetch-out (fetch-data date-chan))
;; (def parse-out (parse-response fetch-out))
;; (def rename-technique-keys-out (rename-technique-keys parse-out))
;; (def technique-update-out (technique-update rename-technique-keys-out))

;; (defn printer
;;   [in]
;;   (go (while true (println (<! in)))))

;; (printer technique-update-out)

;; this also started running as an infinite loop until i took those while true's out
;; i also need to derive the filename from the dates
;; (-> {:year 2021 :month 7 :day 1}
;;     fetch-data
;;     parse-response
;;     rename-technique-keys
;;     technique-update
;;     <!!)