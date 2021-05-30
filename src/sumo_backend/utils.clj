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
