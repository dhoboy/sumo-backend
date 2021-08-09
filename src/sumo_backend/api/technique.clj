(ns sumo-backend.api.technique)
(require '[sumo-backend.utils :as utils])

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
          (utils/str->keyword technique)))
      (keys categories))))

(defn get-categories
  "return categories map"
  []
  categories)
