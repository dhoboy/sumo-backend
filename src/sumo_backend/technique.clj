(ns sumo-backend.technique)

;; we will have to manually categorize
;; new techniques as we encounter them
;; by adding them to the appropriate map here
;; and then running data-format/add-technique-to-datafiles
;; to write these categories to the data files
(def categories
  {:force
    #{:tsuridashi
      :abisetaoshi
      :kimedashi
      :yoritaoshi
      :yorikiri}
   :push
     #{:tsukitaoshi
       :okuritaoshi
       :okuridashi
       :oshitaoshi
       :tsukidashi
       :tsukiotoshi
       :oshidashi}
   :throw
     #{:okurihikiotoshi
       :kainahineri
       :kakenage
       :tottari
       :komatasukui
       :shitatenage
       :katasukashi
       :uwatedashinage}
   :dodge
     #{:hikiotoshi
       :hatakikomi}
   :default
     #{:fusen}
  })

(defn get-technique-key
  "given a technique_ja string
   returns it as a keyword"
  [technique_ja]
  (when
    (and
      (string? technique_ja)
      (not (clojure.string/blank? technique_ja)))
    (-> technique_ja
        clojure.string/trim
        clojure.string/lower-case
        keyword)))

(defn get-category
  "given a technique key or technique_ja string,
   return what category it is in"
  [technique]
  (first
    (filter
      #(contains?
        (% categories)
        (if (keyword? technique)
          technique
          (get-technique-key technique)))
      (keys categories))))
