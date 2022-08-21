(ns sumo-backend.utils-test
  (:require [clojure.test :refer [deftest is testing]]
            [sumo-backend.utils :as utils]))


(deftest utils
  (testing "in?"
    (is (= true (utils/in? #{"hi"} "hi"))
      "Finds member in collection")
    (is (= nil (utils/in? #{"hi"} "bye"))
      "Returns nil if member not in collection")
    (is (= nil (utils/in? "str" false))
      "Returns nil on nonsense input"))


  (testing "paginate-list"
    (is (=
          {:pagination {:page 1 :per 15 :total 3} :items '(1 2 3)}
          (utils/paginate-list {:item-list [1 2 3]}))
      "Paginates default list succesfully")
    (is (=
          {:pagination {:page 2 :per 2 :total 3} :items '(3)}
          (utils/paginate-list {:item-list [1 2 3] :page 2 :per 2}))
      "Paginates list with page and per params successfully")
    (is (=
          {:pagination {:total 3} :items [1 2 3]}
          (utils/paginate-list {:item-list [1 2 3] :all true}))
      "Returns list unpaginated with {:all true} param"))


  (testing "get-date"
    (is (=
          {:day 1 :month 3 :year 2021}
          (utils/get-date "day1__03_2021.json"))
      "Parses date out of filename")
    (is (=
          {:day 1 :month 3 :year 2021}
          (utils/get-date "day1__3_2021.json"))
      "Parses date out of filename without zero padding")
    (is (=
          {:day 1 :month 3 :year 2021}
          (utils/get-date "day1_3_____2021.json"))
      "Parses date out of filename with random underscores"))


  (testing "str->keyword"
    (is (=
          :sumo
          (utils/str->keyword "sumo"))
      "Translates arbitrary string into keyword"))


  (testing "zero-pad"
    (is (=
          "07"
          (utils/zero-pad 7))
      "Zero pads a number less than 10")
    (is (=
          "07"
          (utils/zero-pad "7"))
      "Zero pads a string representation of a number less than 10")
    (is (=
          "700"
          (utils/zero-pad 700))
      "Doesn't zero-pad a number greater than 10")
    (is (=
          "700"
          (utils/zero-pad "700"))
      "Doesn't zero-pad a string representation of a number greater than 10")
    (is (=
          nil
          (utils/zero-pad -4))
      "Returns nil for negative number")
    (is (=
          nil
          (utils/zero-pad "-4"))
      "Returns nil for string representation of a negative number")
    (is (=
          nil
          (utils/zero-pad "not a number")))
    "Returns nil for non-numeric string")


  (testing "add-percent-to-list"
    (is (=
          '({:count 15 :percent 0.5} {:count 15 :percent 0.5})
          (utils/add-percent-to-list '({:count 15} {:count 15})))
      "Adds :percent key to each map in the list with a :count key")
    (is (=
          '({:otherkey "something else" :percent 0.0} {:count 15 :percent 1.0})
          (utils/add-percent-to-list '({:otherkey "something else"} {:count 15})))
      "Adds :percent 0.0 to maps in a list without a :count key")))
