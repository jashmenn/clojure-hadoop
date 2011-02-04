(defproject com.atti.wdm/clojure-hadoop "1.3.1-SNAPSHOT"
  :description "Library to aid writing Hadoop jobs in Clojure."
  :url "http://github.com/alexott/clojure-hadoop"
  :license {:name "Eclipse Public License 1.0"
            :url "http://opensource.org/licenses/eclipse-1.0.php"
            :distribution "repo"
            :comments "Same license as Clojure"}
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.apache.hadoop/hadoop-core "0.20.2"]
                 [log4j/log4j "1.2.16"]]
  :dev-dependencies [[swank-clojure "1.2.1"]]
  :jvm-opts ["-Xmx1400m"]
  :aot [clojure-hadoop.config
        clojure-hadoop.defjob
        clojure-hadoop.gen
        clojure-hadoop.imports
        clojure-hadoop.job
        clojure-hadoop.load
        clojure-hadoop.wrap
        ;; TODO: Remove them? Only needed for the tests.
        clojure-hadoop.examples.wordcount1 
        clojure-hadoop.examples.wordcount2
        clojure-hadoop.examples.wordcount3
        clojure-hadoop.examples.wordcount4
        clojure-hadoop.examples.wordcount5])
