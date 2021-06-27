(ns sumo-backend.core)
(require '[cheshire.core :refer :all]) ; parses json
(require '[sumo-backend.mysql :as db])
(require '[sumo-backend.rank :as rank])
(require '[sumo-backend.utils :as utils])

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

; n.b.: day15__09_2019 has a takakeisho and mitakeumi playoff
  
;;;;;;;;;;;;;;
;; Load Data
;;;;;;;;;;;;;;

(defn load-data
  "loads all un-loaded data from the optional
   passed in (file or dir) path or
   the default-data-dir into the mysql database.
   writes tournament-rank-values for all tournaments
   where data was read."
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
    (cond
      (= file-count 1) (->> all-files
                            (map db/read-basho-file)
                            doall ; returns value so tournament-rank-values can be written
                            (map rank/write-tournament-rank-values)
                            dorun) ; make this map run
      (> file-count 1) (->> all-files
                            (db/read-basho-dir) ; returns a doall
                            (filter some?)
                            (into #{})
                            (map rank/write-tournament-rank-values)
                            dorun) ; make this map run
      :else (println (str "File: '" (or (first args) utils/default-data-dir) "' not found")))))

;;;;;;;;;;;;;
;; Explain 
;;;;;;;;;;;;;

(def explain-sumo '(
  ";; Sumo tournaments are 15 days.\n"
  ";; Tournaments are identified\n"
  ";; by their year and month.\n"
  ";; There are 6 tournaments per year:\n"
  ";; Jan, Mar, May, July, Sept, & Nov.\n"
  ";; Wrestlers are called 'Rikishi'.\n"
  ";; Rikishi have one bout per day\n"
  ";; each tournament.\n"
  ";; Best record after 15 days\n"
  ";; wins the tournament.\n"
  ";; If Rikishi are tied on the last\n"
  ";; day, a playoff match is held\n"
  ";; to determine the winner.\n"
  ";; This is the only case where\n"
  ";; a Rikishi has more than one\n"
  ";; match on a given day.\n\n"))

;; maybe mention here about technique...
(def explain-data '(
  ";; Data for this project\n"
  ";; is loaded into a Mysql database\n"
  ";; named 'sumo'. Be sure this database\n"
  ";; exists before loading data.\n"
  ";; Data is loaded from JSON files\n"
  ";; with this naming convention:\n"
  ";; -> day<number>__<month>_<year>.json.\n"
  ";; e.g. 'day1__03_2021.json'\n"
  ";; Parts separated by one or more _.\n"
  ";; Use as many _'s as you want for\n"
  ";; ease of filename readability.\n"
  ";; A file contains matches held\n"
  ";; on the day of its filename.\n"
  ";; The 'default-data-dir'\n"
  ";; for this project is '/tournament_data'.\n"
  ";; See there for examples.\n\n"

  ";; N.B.: When loading a directory,\n"
  ";; in order to optimize load time\n"
  ";; if full tournament data, defined as\n"
  ";; data for 15 days of a tournament,\n"
  ";; already exists in the database\n"
  ";; it is assumed all data for that\n"
  ";; tournament has already been loaded\n"
  ";; and no files for that tournament\n"
  ";; will be read.\n\n"

  ";; Load additional files\n"
  ";; for tournaments with 15 days of data\n"
  ";; already in the database\n"
  ";; by passing the filepath\n"
  ";; directly to this command.\n"
  ";; Files at paths passed directly\n"
  ";; will always be read, and duplicate\n"
  ";; data will not be written.\n"))

(defn print-explain
  "prints more info on sumo in general and 
   storing and loading data for this project"
  [& args]
  (println
    (str
      "\n"
      "******* Grand Sumo API *******\n"))
  (case (first args)
    "sumo" (println (apply str explain-sumo))
    "data" (println (apply str explain-data))
    (println (apply str (concat explain-sumo explain-data)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;; Main Function ;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn print-help
  "prints out what you can do in the main function"
  []
  (println 
    (str
      "\n** Welcome to the Grand Sumo API!\n"
      "USAGE: lein run <command> <optional path or subject>\n\n"
      "Available commands are:\n"
      " -> explain <optional subject>\n"
      "      Provides a quick overview of subject.\n" 
      "      Available subjects are 'sumo' and 'data'.\n"
      "      Defaults to explaining all subjects.\n"
      " -> initialize\n"
      "      Creates the mysql tables used by this project.\n"
      " -> tear-down\n"
      "      Drops mysql tables created by this project.\n" 
      " -> load-data <optional path>\n"
      "      Loads all previously un-loaded data\n" 
      "      from the optionally passed file or directory path.\n"
      "      If no path passed, loads from the default-data-dir, which\n"
      "      is initially set to '/data' in this project repository.\n"
      "\nOnce your mysql database is populated,\n"
      "You can run 'lein ring server-headless' to start this API. **\n")))

(defn -main ; runs as if you booted repl, runs main and calls with bash args as args
  [& args]
  (case (first args)
    "explain"          (print-explain (first (drop 1 args)))
    "initialize"       (db/create-tables)
    "tear-down"        (db/drop-tables)
    "load-data"        (load-data (first (drop 1 args)))
    (print-help)))
