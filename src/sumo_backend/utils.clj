(ns sumo-backend.utils
  (:require [clojure.string :as str]
            [clojure.java.io :as io]))


;;
;; Namespace providing helper functions that accomplish common tasks
;; This namespace will have no project specific dependencies, so it can be
;; used anywhere throughout the project.
;;

;; let user point to their own custom data path?
(def default-data-dir "./tournament_data")


(defn in?
  "true if collection contains elm"
  [coll elm]
  (some #(= elm %) coll))


;; Paginate a list of items
(defn paginate-list
  "Given a sequence :item-list, returns that list paginated.
   Takes :page and :per params to step through response pages.
   Given :all true, will return entire list un-paginated."
  [{:keys [item-list page per all] :or {page 1 per 15}}]
  (if all
    {:pagination {:total (count item-list)}
     :items item-list}
    (let [page (if (string? page) (Integer/parseInt page) page)
          per (if (string? per) (Integer/parseInt per) per)]
      {:pagination {:page page :per per :total (count item-list)}
       :items (take per (drop (* (- page 1) per) item-list))})))


;; Get a Rikishi's opponent from a bout
(defn get-bout-opponent
  "for a given rikishi and bout
   return the bout opponent name string"
  [{:keys [rikishi bout]}]
  (or
    (and
      (= (str/upper-case (:east bout)) (str/upper-case rikishi)) (:west bout))
    (and
      (= (str/upper-case (:west bout)) (str/upper-case rikishi)) (:east bout))))


;; Get the winner from a given pair of east and west maps
(defn get-bout-winner
  "determines winner for passed in east and west records"
  [east west]
  (if (= (:result east) "win")
    (:name east)
    (:name west)))


;; Get the looser from a given pair of east and west maps
(defn get-bout-loser
  "determines loser for passed in east and west records"
  [east west]
  (if (= (:result east) "win")
    (:name west)
    (:name east)))


;; Get the date from a filepath
(defn get-date
  "takes in a filepath and parses out the date.
   file can be nested along any path, but filename
   must be in this format: 'day<num>_<month>_<year>.json'
   Any number of underscores can be used to seperate date parts.
   Ex: I find this more readable 'day1__03_2021.json'"
  [filepath]
  (let [parts (-> filepath
                (str/split #"\/")
                last
                (str/split #"\.")
                first
                (str/split #"\_"))
        date (->> parts
               (map #(str/split % #"day"))
               flatten
               (filter #(not= "" %)))]
    {:day (Integer/parseInt (nth date 0))
     :month (Integer/parseInt (nth date 1))
     :year (Integer/parseInt (nth date 2))}))


;; Translate string filepath to file object
(defn path->obj
  "given a path string, return
   it wrapped as a file object"
  [path]
  (if (some? path) ; some? is (not (nil? __))
    (file-seq
      (io/file
        path))
    nil))


;; Translate string to keyword
(defn str->keyword
  "given a string, returns it as a keyword.
   handles input validation checking so you don't
   have to everywhere"
  [str]
  (when
    (and
      (string? str)
      (not (str/blank? str)))
    (-> str
      str/trim
      str/lower-case
      keyword)))


;; Zero pad number strings
(defn zero-pad
  "given any passed in number as a string or number,
   returns a zero-padded string for that number.
   e.g. given 7 or '7' => '07'"
  [input]
  (let [val (if (string? input)
              (try
                (Integer/parseInt input)
                (catch Exception _ nil))
              input)]
    (cond
      (or (nil? val) (< val 0)) nil
      (< val 10) (str 0 val)
      :else (str val))))


;; Add percent to list
(defn add-percent-to-list
  "given a list where each item has a :count key,
   adds :percent of each item in the list. If an item
   doesn't have :count key it gets :percent 0"
  [coll]
  (let [total (reduce + (map #(or (:count %) 0) coll))]
    (map
      (fn [elem]
        (assoc
          elem
          :percent
          (float (/ (or (:count elem) 0) total))))
      coll)))


;; Slawski Macros

(defmacro when-let-all
  #_{:clj-kondo/ignore [:unresolved-symbol]}
  [bindings & then]
  (cond
    (empty? bindings)
    `(do ~@then)
    (= 1 (mod (count bindings) 2))
    (throw (Exception. "when-let-all requires an even number of bindings"))
    :else
    (let [[var-name body & other-bindings] bindings]
      `(when-let [~var-name ~body]
         (when-let-all ~other-bindings ~@then)))))
