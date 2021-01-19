; load the repl from the root of the project directory
; run this file in the repl to populate the database
; n.b.: day15__09_2019 has a takakeisho and mitakeumi playoff

; (load-file "./src/sumo_backend/process_json.clj")
(ns sumo-backend.process-json)
(require '[cheshire.core :refer :all]) ; parses json
(require '[clojure.java.jdbc :as j])   ; writes to mysql

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

