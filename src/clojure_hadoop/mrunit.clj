(ns clojure-hadoop.mrunit
  ;;^{:doc "Helpers for writing MRUnit tests"}
  (:use [clojure-hadoop.imports :as imp])
  (:import [org.apache.hadoop.mrunit MapDriver ReduceDriver MapReduceDriver]))

(imp/import-io)
(imp/import-mapred)
(imp/import-mapred-lib)

;; public MapReduceDriver<K1, V1, K2, V2> withConfiguration(Configuration configuration) {

(defn set-expected-outputs [driver expected]
  (doall (map (fn [[key value]] (.withOutput driver key value)) expected)))

(defn set-inputs [driver expected]
  (doall (map (fn [[key value]] (.withInput driver key value)) expected)))

(defn set-input-values [driver values]
  (doall (map (fn [value] (.withInputValue driver value)) values)))

(defn test-mapper [mapper input & expected]
  (let [driver (MapDriver. mapper)]
    (doto driver
      (.withInput  (first input) (second input))
      (set-expected-outputs expected)
      (.runTest))))

(defn test-reducer [reducer input-key input-values & expected]
  (let [driver (ReduceDriver. reducer)]
    (doto driver
      (.withInputKey input-key)
      (set-input-values input-values)
      (set-expected-outputs expected)
      (.runTest))))

(defn test-map-reduce [mapper reducer inputs expected]
  (let [driver (MapReduceDriver. mapper reducer)]
    (doto driver
      (set-inputs inputs)
      (set-expected-outputs expected)
      (.runTest))))

