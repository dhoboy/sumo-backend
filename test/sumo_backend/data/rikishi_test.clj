(ns sumo-backend.data.rikishi-test
  (:require [clojure.test :refer [deftest is testing]]
            [sumo-backend.data.rikishi :refer [get-rikishi
                                               list-rikishi
                                               rikishi-exists?]]))


(deftest rikishi
  (testing "get-rikishi"
    (is (=
          "ENDO"
          (:name (first (get-rikishi "endo"))))
      "Returns record for passed in rikishi")

    (is (=
          '(:id :name :image :name_ja)
          (keys (first (get-rikishi "endo"))))
      "Returns record with :id, :name, :image, :name_ja keys")

    (is (=
          '()
          (get-rikishi "not a rikishi"))
      "Returns an empty list if rikishi record does not exist"))


  (testing "list-rikishi"
    (is (=
          '(:id :name :image :name_ja)
          (keys (first (list-rikishi))))
      "Returns records with :id, :name, :image, :name_ja keys"))


  (testing "rikishi-exists?"
    (is (=
          false
          (rikishi-exists? "Sam"))
      "Invalid Rikishi name returns false")

    (is (=
          true
          (rikishi-exists? "Endo"))
      "Valid Rikishi name returns true")

    (is (=
          false
          (rikishi-exists? nil))
      "Invalid Rikishi name returns false")))
