(ns sumo-backend.routes-test
  (:require [clojure.test :refer [deftest is testing]]
            [cheshire.core :refer [parse-string]] ; parses json
            [ring.mock.request :as mock]
            [sumo-backend.routes :refer [app]]))


(def routes
  ["/rikishi/list"
   "/rikishi/details/endo"
   "/rikishi/current_rank/endo"
   "/rikishi/rank_over_time/endo"
   "/rikishi/results_over_time/endo"
   "/rikishi/technique_breakdown/endo"
   "/tournament/list"
   "/tournament/details/2022/07"
   "/technique/list"
   "/technique/details/oshidashi"
   "/technique/category_details/force"
   "/technique/categories"
   "/bout/list"
   "/bout/list/endo"
   "/bout/list/endo/takakeisho"
   "/fare/endo/ozeki"
   "/fare/win/endo/ozeki"
   "/fare/lose/endo/ozeki"
   "/upset/list"
   "/upset/win/endo"
   "/upset/lose/endo"])


;; (is (= expected actual) message)
(defn standard-success-response
  "tests routes for standard success response"
  [path]
  (let [resp (app (mock/request :get path))
        headers (:headers resp)
        body (parse-string (:body resp) true)]
    (is (= "application/json; charset=utf-8" (get headers "Content-Type"))
      "response header specifies json")
    (is (= 200 (:status resp))
      "response status is 200")
    (is (= true (contains? body :pagination))
      "response body contains pagination")
    (is (= true (contains? body :items))
      "response body contains items")))


(deftest test-routes
  (dorun
    (map
      #(testing %
         (standard-success-response %))
      routes))

  (testing "not-found route"
    (let [response (app (mock/request :get "/invalid"))]
      (is (= (:status response) 404)))))
