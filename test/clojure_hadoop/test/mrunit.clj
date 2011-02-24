
(ns clojure-hadoop.test.mrunit
  (:require [clojure-hadoop.imports :as imp])
  (:import [org.apache.hadoop.mrunit MapDriver ReduceDriver])
  (:use clojure.test)
  (:use clojure-hadoop.mrunit))

(imp/import-io)
(imp/import-mapred)
(imp/import-mapred-lib)

(defn text  [s] (Text. s))
(defn longw [x] (LongWritable. x))

(deftest test-identity-mapper 
  (test-mapper (IdentityMapper.)
               [(text "foo") (text "bar")]
               [(text "foo") (text "bar")]))

(deftest test-inverse 
  (test-mapper (InverseMapper.)
               [(text "foo") (text "bar")]
               [(text "bar") (text "foo")]))

(deftest test-token-count 
  (test-mapper (TokenCountMapper.)
               [(text "foo") (text "my dog has fleas")]
               [(text "my") (longw 1)]
               [(text "dog") (longw 1)]
               [(text "has") (longw 1)]
               [(text "fleas") (longw 1)]))

(deftest test-identity-reducer
  (test-reducer (IdentityReducer.) (text "foo")
                [(text "bar") 
                 (text "baz")]
                [(text "foo") (text "bar")]
                [(text "foo") (text "baz")]))

(deftest test-sum
  (test-reducer (LongSumReducer.) (text "foo")
                [(longw 5) 
                 (longw 4)]
                [(text "foo") (longw 9)]))
