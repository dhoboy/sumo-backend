;; TODO Make a main function
;; If you specify a main namespace like sumo-backend.core,
;; lein will import its dependencies when you start the repl.
;; E.g.:
;; core ns: https://github.com/bslawski/clj-ml/blob/master/src/clj_ml/core.clj
;; project.clj: https://github.com/bslawski/clj-ml/blob/master/project.clj

; load the repl from the root of the project directory
; run this file in the repl to populate the database
; n.b.: day15__09_2019 has a takakeisho and mitakeumi playoff

; (load-file "./src/sumo_backend/process_json.clj")

(ns sumo-backend.process-json)
(require '[cheshire.core :refer :all]) ; parses json
(require '[clojure.java.jdbc :as j])   ; writes to mysql



;; TODO use a single namespace for DB functions
;; Any DB config or interface should be in a single
;; namespace, probably a renamed sumo-backend.functions
;; This namespace would be for translating among
;; JSON, hash-maps and lists of hash-maps formatted for MySQL,
;; strings for saving to flatfiles, or any other formatting
;; that needs to happen.
;; Other namespaces would use this namespace to translate
;; data read from multiple locations into whatever format
;; is needed to process or write it.
;; E.g.:
;;
;; sumo-backend.db ----------|
;;                           |
;; sumo-backend.data-format -|---> sumo-backend.predictor
;;                           |
;; sumo-backend.s3 ----------|
;;
;;
;; To write a prediction report, the predictor ns would:
;; - Call db to get the data that it needs
;; - Call data-format to parse the DB data into a more usuable format
;;   (parse timestamps from java.sql.Timestamp to simple-time.datetime,
;;    parse JSON strings into hash-maps, etc)
;; - Use the data to make some sort of prediction
;; - Call data-format to turn the prediction data into a string
;; - Pass that string to s3 to write as a flatfile
;;
;; There are different ways that could be organized
;; (s3 could use data-format to stringify data, rather than expecting strings)
;; but in general, there should be one namespace to interact with each resource,
;; one namespace for each group of shared functions (string utils, math utils, etc),
;; then higher-level namespaces that use those lower-level namespaces to do things.
;; This way, if there is a bug with MySQL or you want to change DBs altogether,
;; there is only one namespace to change.


(def mysql-db {:dbtype "mysql"
               :dbname "sumo"
               :user "FILL-ME-IN"
               :password "FILL-ME-IN"})

(defn write-rikishi
  "write rikishi info to the database"
  [rikishi]
  (j/insert-multi! mysql-db :rikishi
    [{:name (:name rikishi)
      :image (:image rikishi)
      :name_ja (:name_ja rikishi)}
    ]))

(defn get-bout-winner
  "determines winner for passed in east and west records"
  [east west]
  (if (= (:result east) "win")
    (:name east)
    (:name west)))

(defn get-date
  "takes in a filepath and parses out the date"
  [filepath]
  (let [parts (clojure.string/split filepath #"\/")]
    {:year (nth parts 2)
     :month (nth parts 3)
     :day (subs (nth (clojure.string/split (nth parts 4) #"__") 0) 3) 
    }))

(defn write-bout
  "write a bout's information to the databae"
  [east west winning-technique date]
  (j/insert-multi! mysql-db :bout
    [{:east (:name east) :east_rank (:rank east) 
      :west (:name west) :west_rank (:rank west)
      :winner (get-bout-winner east west) 
      :winning_technique winning-technique
      :year (:year date) :month (:month date) :day (:day date)}
    ]))

(defn read-basho-file
  "read in a file representing one day's sumo basho results, write it to the database"
  [filepath]
  (let [data (parse-string (slurp filepath) true)]
  (map
    (fn [record]
      ; write unique rikishi records
      (when (= (j/query mysql-db ["SELECT * FROM rikishi WHERE name = ?", (:name (:east record))]) [])
        (write-rikishi (:east record)))
      (when (= (j/query mysql-db ["SELECT * FROM rikishi WHERE name = ?", (:name (:west record))]) [])
        (write-rikishi (:west record)))
      ; write all match data to database
      (write-bout (:east record) (:west record) (:technique record) (get-date filepath))
    )
    (:data data))))
  

; read in the files in /data directory 
(def data-dir (file-seq (clojure.java.io/file "./data")))
(let [all-files (filter #(some? (re-find #".json$" %)) (map str (filter #(.isFile %) data-dir)))]
  (map read-basho-file all-files))

