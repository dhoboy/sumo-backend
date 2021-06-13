(ns sumo-backend.utils)

;; Utils functions accomplishing common tasks

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Paginate a list of items
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get a Rikishi's opponent from a bout 
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-bout-opponent
  "for a given rikishi and bout
   return the bout opponent name string"
  [{:keys [rikishi bout]}]
  (or
   (and (= (:east bout) (clojure.string/upper-case rikishi)) (:west bout))
   (and (= (:west bout) (clojure.string/upper-case rikishi)) (:east bout))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Did the rikishi win/lose given bout?
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn rikishi-win-or-lose-bout
  "given a rikishi, bout, and an outcome
   return true or false
   according to the expression"
  [{:keys [rikishi outcome bout]}]
  ((or 
    (and (= outcome "lose") not=)
    (and (= outcome "win") =))
   (:winner bout) (clojure.string/upper-case rikishi)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get the winner from a given pair of east and west maps
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-bout-winner
  "determines winner for passed in east and west records"
  [east west]
  (if (= (:result east) "win")
    (:name east)
    (:name west)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get the looser from a given pair of east and west maps
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-bout-loser
  "determines loser for passed in east and west records"
  [east west]
  (if (= (:result east) "win")
    (:name west)
    (:name east)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Get the date from a filepath
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-date
  "takes in a filepath and parses out the date.
   file can be nested along any path, but filename
   must be in this format: 'day<num>_<month>_<year>.json'
   Any number of underscores can be used to seperate date parts.
   Ex: I find this more readable 'day1__03_2021.json'"
  [filepath]
  (let [parts (-> filepath
                  (clojure.string/split #"\/")
                  last
                  (clojure.string/split #"\.")
                  first
                  (clojure.string/split #"\_"))
        date (->> parts
                  (map #(clojure.string/split % #"day"))
                  flatten
                  (filter #(not= "" %)))]
    {:day (Integer/parseInt (nth date 0))
     :month (Integer/parseInt (nth date 1))
     :year (Integer/parseInt (nth date 2))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Compare Rikishi bout history according to passed in function
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn rikishi-comparison
  "for a given rikishi
   process all bout data, comparing
   wins/losses according to passed in 
   success-criteria function"
  [rikishi outcome success-criteria results [bout & rest]] ; "endo" "lose" ...
  (if (nil? bout) ; no more bouts, return results
    results
    (if (and
          (rikishi-win-or-lose-bout
            {:rikishi rikishi
             :outcome outcome
             :bout bout})
          (success-criteria bout))
      (rikishi-comparison ; save this bout and continue
        rikishi
        outcome
        success-criteria
        (conj results bout)
        rest)
      (rikishi-comparison ; move on, don't save this bout
        rikishi
        outcome
        success-criteria
        results
        rest))))
