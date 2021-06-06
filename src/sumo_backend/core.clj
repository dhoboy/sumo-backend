(ns sumo-backend.core)
(require '[cheshire.core :refer :all]) ; parses json
(require '[sumo-backend.mysql :as db])

;; TODO Make a main function
;; If you specify a main namespace like sumo-backend.core,
;; lein will import its dependencies when you start the repl.
;; E.g.:
;; core ns: https://github.com/bslawski/clj-ml/blob/master/src/clj_ml/core.clj
;; project.clj: https://github.com/bslawski/clj-ml/blob/master/project.clj

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

; load the repl from the root of the project directory
; run this file in the repl to populate the database
; n.b.: day15__09_2019 has a takakeisho and mitakeumi playoff

; (load-file "./src/sumo_backend/process_json.clj")
  
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Load in data from file at given path ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn load-data-from-file
  "load-data-file <path>"
  [path]
  (let [file (clojure.java.io/file path)]
    (if (.isFile file)
      (println "yay its a file")
      (println "boo its not a file"))))

; read in the files in /data directory  
; thread macro all this
(def data-dir
  (file-seq
    (clojure.java.io/file "./data")))

(let [all-files (filter
                  #(some?
                    (re-find #".json$" %))
                  (map
                    str 
                    (filter #(.isFile %) data-dir)))]
  (map db/read-basho-file all-files))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;; Main Function ;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn print-help
  "prints out what you can do in the main function"
  []
  (println 
    (str
      "\n** Welcome to the Grand Sumo API!\n"
      "USAGE: lein run <command> ~OR~ lein run <command> <path>\n\n"
      "Available commands are:\n"
      " initialize\n"
      "   -> This creates the mysql database used by this project\n"
      " load-all-data\n"
      "   -> This resets the database to what is found in the 'data/' dir in this project\n"
      " load-data-dir <path>\n" 
      "   -> This loads additional data at the specified dir path into the database\n"
      " load-data-file <path>\n" 
      "   -> This loads additional data at the specified file path into the database\n"
      " tear-down\n" 
      "   -> This drops all mysql tables associated with this project\n"
      "\nOnce your mysql database is populated,\n"
      "You can run 'lein ring server headless' to start this API. **\n")))

(defn -main
  [& args]
  (case (first args)
    "initialize"     (println "initialize this database")
    "load-all-data"  (println "load all this data")
    "load-data-dir"  (println (str "load data from this dir " (last args)))
    "load-data-file" (load-data-from-file (last args))
    "tear-down"      (println "drop all tables created for this project")
    (print-help)))
