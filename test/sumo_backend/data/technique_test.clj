(ns sumo-backend.data.technique-test
  (:require [clojure.test :refer [deftest is testing]]
            [sumo-backend.data.technique :as technique]))


(deftest base-technique-info
  (testing "get-category"
    (is (=
          :push
          (technique/get-category "oshidashi"))
      "Returns category keyword for valid technique")

    (is (=
          nil
          (technique/get-category "asdf"))
      "Returns nil for invalid technique"))


  (testing "list-techniques"
    (is (=
          '(:technique :technique_en :technique_category)
          (keys (first (technique/list-techniques {}))))
      "Returns list of maps with :technique, :technique_en, and :technique_category keys")))


(deftest rikishi-results-via-techniques
  (testing "get-rikishi-wins-by-technique"
    (is (=
          '(:technique :technique_en :technique_category :count)
          (keys (first (technique/get-rikishi-wins-by-technique {:rikishi "endo"}))))
      "Returns list of maps with :technique, :technique_en, :technique_category, and :count keys")

    (is (=
          '()
          (technique/get-rikishi-wins-by-technique {}))
      "Returns empty list without passing in a rikishi name"))


  (testing "get-rikishi-wins-by-technique-category"
    (is (=
          '(:technique_category :count)
          (keys (first (technique/get-rikishi-wins-by-technique-category {:rikishi "endo"}))))
      "Returns list of maps with :technique_category and :force keys")

    (is (=
          '()
          (technique/get-rikishi-wins-by-technique-category {}))
      "Returns empty list without passing in a rikishi name"))


  (testing "get-rikishi-losses-to-technique"
    (is (=
          '(:technique :technique_en :technique_category :count)
          (keys (first (technique/get-rikishi-losses-to-technique {:rikishi "endo"}))))
      "Returns list of maps with :technique, :technique_en, :technique_category and :count keys")

    (is (=
          '()
          (technique/get-rikishi-losses-to-technique {}))
      "Returns empty list without passing in a rikishi name"))


  (testing "get-rikishi-losses-to-technique-category"
    (is (=
          '(:technique_category :count)
          (keys (first (technique/get-rikishi-losses-to-technique-category {:rikishi "endo"}))))
      "Returns list of maps with :technique, :technique_en, :technique_category and :count keys")

    (is (=
          '()
          (technique/get-rikishi-losses-to-technique-category {}))
      "Returns empty list without passing in a rikishi name")))


(deftest technique-based-results
  (testing "get-all-wins-by-technique"
    (is (=
          '(:winner :count)
          (keys (first (technique/get-all-wins-by-technique {:technique "oshidashi"}))))
      "Returns list of maps with :winner and :count keys")

    (is (=
          '()
          (technique/get-all-wins-by-technique {}))
      "Returns empty list without passing in a technique name"))


  (testing "get-all-wins-by-technique-category"
    (is (=
          '(:winner :count)
          (keys (first (technique/get-all-wins-by-technique-category {:category "force"}))))
      "Returns list of maps with :winner and :count keys")

    (is (=
          '()
          (technique/get-all-wins-by-technique-category {}))
      "Returns empty list without passing in a category"))


  (testing "get-all-losses-to-technique"
    (is (=
          '(:loser :count)
          (keys (first (technique/get-all-losses-to-technique {:technique "oshidashi"}))))
      "Returns list of maps with :loser and :count keys")

    (is (=
          '()
          (technique/get-all-losses-to-technique {}))
      "Returns empty list without passing in a technique"))


  (testing "get-all-losses-to-technique-category"
    (is (=
          '(:loser :count)
          (keys (first (technique/get-all-losses-to-technique-category {:category "push"}))))
      "Returns empty list without passing in technique category name")

    (is (=
          '()
          (technique/get-all-losses-to-technique-category {}))
      "Returns empty list without passing in a category")))
