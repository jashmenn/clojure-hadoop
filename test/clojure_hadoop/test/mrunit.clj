
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
               [[(text "foo") (text "bar")]]
               [[(text "foo") (text "bar")]]))

(deftest test-inverse 
  (test-mapper (InverseMapper.)
               [[(text "foo") (text "bar")]]
               [[(text "bar") (text "foo")]]))

(deftest test-token-count 
  (test-mapper (TokenCountMapper.)
               [[(text "foo") (text "my dog has fleas")]]
               [[(text "my") (longw 1)]
                [(text "dog") (longw 1)]
                [(text "has") (longw 1)]
                [(text "fleas") (longw 1)]]))

(deftest test-identity-reducer
  (test-reducer (IdentityReducer.) (text "foo")
                [(text "bar") 
                 (text "baz")]
                [[(text "foo") (text "bar")]
                 [(text "foo") (text "baz")]]))

(deftest test-sum
  (test-reducer (LongSumReducer.) (text "foo")
                [(longw 5) 
                 (longw 4)]
                [[(text "foo") (longw 9)]]))

(deftest test-word-count
  (test-map-reduce (TokenCountMapper.) (LongSumReducer.) 
     [[(text "_") (text "my dog has fleas")]
      [(text "_") (text "my cat has fleas")]]
     [[(text "cat") (longw 1)]
      [(text "dog") (longw 1)]
      [(text "fleas") (longw 2)]
      [(text "has") (longw 2)]
      [(text "my")  (longw 2)]]))

(deftest test-jobconf
  (def jobconf 
       (doto (JobConf.)
         (.set "foo.bar.baz" "hello")))

  (defn my-mapper []
    (proxy [IdentityMapper] [] 
      (configure [jobconf] 
        (is (= "hello" (.get jobconf "foo.bar.baz"))))))

  (test-map-reduce (my-mapper) (IdentityReducer.) 
     [[(text "k") (text "a")]
      [(text "k") (text "b")]]
     [[(text "k") (text "a")]
      [(text "k") (text "b")]]
     jobconf))


