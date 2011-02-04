(ns clojure-hadoop.config
  (:require [clojure-hadoop.imports :as imp]
            [clojure-hadoop.load :as load])
  (:use [clojure.string :only [trim]])
  (:import (org.apache.hadoop.io.compress
            DefaultCodec GzipCodec BZip2Codec)))

;; This file defines configuration options for clojure-hadoop.
;;
;; The SAME options may be given either on the command line (to
;; clojure_hadoop.job) or in a call to defjob.
;;
;; In defjob, option names are keywords.  Values are symbols or
;; keywords.  Symbols are resolved as functions or classes.  Keywords
;; are converted to Strings.
;;
;; On the command line, option names are preceeded by "-".
;;
;; Options are defined as methods of the conf multimethod.
;; Documentation for individual options appears with each method,
;; below.

(imp/import-io)
(imp/import-fs)
(imp/import-mapred)
(imp/import-mapred-lib)

(defn- ^String as-str [s]
  (cond (keyword? s) (name s)
        (class? s) (.getName ^Class s)
        (fn? s) (throw (Exception. "Cannot use function as value; use a symbol."))
        :else (str s)))

(defmulti conf (fn [jobconf key value] key))

(defmethod conf :job [jobconf key value]
  (let [f (load/load-name value)]
    (conf jobconf :job-fn f)))

(defmethod conf :job-fn [jobconf key value]  
  (doseq [[k v] (value)]
    (conf jobconf k v)))

(defmethod conf :name [^JobConf jobconf key value]  
  (.setJobName jobconf value))

;; Job input paths, separated by commas, as a String.
(defmethod conf :input [^JobConf jobconf key value]
  (FileInputFormat/setInputPaths jobconf (as-str value)))

;; Job output path, as a String.
(defmethod conf :output [^JobConf jobconf key value]
  (FileOutputFormat/setOutputPath jobconf (Path. (as-str value))))

;; When true or "true", deletes output path before starting.
(defmethod conf :replace [^JobConf jobconf key value]
  (when (= (as-str value) "true")
    (.set jobconf "clojure-hadoop.job.replace" "true")))

;; The mapper function.  May be a class name or a Clojure function as
;; namespace/symbol.  May also be "identity" for IdentityMapper.
(defmethod conf :map [^JobConf jobconf key value]
  (let [value (as-str value)]
    (cond
      (= "identity" value)
      (.setMapperClass jobconf IdentityMapper)

      (.contains value "/")
      (.set jobconf "clojure-hadoop.job.map" value)

      :else
      (.setMapperClass jobconf (Class/forName value)))))

;; Sets number of map tasks
(defmethod conf :map-count [^JobConf jobconf key value]
  (cond
    (string? value)
    (.setNumMapTasks jobconf (Integer/parseInt (trim value)))

    :else
    (throw (Exception. "map-count should be number!"))))

;; The reducer function.  May be a class name or a Clojure function as
;; namespace/symbol.  May also be "identity" for IdentityReducer or
;; "none" for no reduce stage.
(defmethod conf :reduce [^JobConf jobconf key value]
  (let [value (as-str value)]
    (cond
      (= "identity" value)
      (.setReducerClass jobconf IdentityReducer)

      (= "none" value)
      (.setNumReduceTasks jobconf 0)

      (.contains value "/")
      (.set jobconf "clojure-hadoop.job.reduce" value)

      :else
      (.setReducerClass jobconf (Class/forName value)))))

<<<<<<< HEAD:src/clojure_hadoop/config.clj
;; Sets number of reducer tasks
(defmethod conf :reduce-count [^JobConf jobconf key value]
  (cond
    (string? value)
    (.setNumReduceTasks jobconf (Integer/parseInt (trim value)))

    :else
    (throw (Exception. "map-count should be number!"))))

;;
(defmethod conf :combine [^JobConf jobconf key value]
  (let [value (as-str value)]
    (cond
      (= "identity" value)
      (.setCombinerClass jobconf IdentityReducer)

      (.contains value "/")
      (doto jobconf
        (.setCombinerClass (Class/forName "clojure_hadoop.job_combiner"))
        (.set "clojure-hadoop.job.combiner" value))

      :else
      (.setCombinerClass jobconf (Class/forName value)))))

;; The mapper setup function.  Must be a Clojure function as
;; namespace/symbol.
(defmethod conf :map-setup [#^JobConf jobconf key value]
  (let [value (as-str value)]
    (.set jobconf "clojure-hadoop.job.map.setup" value)))

;; The reducer setup function.  Must be a Clojure function as
;; namespace/symbol.
(defmethod conf :reduce-setup [#^JobConf jobconf key value]
  (let [value (as-str value)]
    (.set jobconf "clojure-hadoop.job.reduce.setup" value)))

;; The mapper reader function, converts Hadoop Writable types to
;; native Clojure types.
(defmethod conf :map-reader [^JobConf jobconf key value]
  (.set jobconf "clojure-hadoop.job.map.reader" (as-str value)))

;; The mapper writer function; converts native Clojure types to Hadoop
;; Writable types.
(defmethod conf :map-writer [^JobConf jobconf key value]
  (.set jobconf "clojure-hadoop.job.map.writer" (as-str value))
  (.set jobconf "clojure-hadoop.job.combiner.writer" (as-str value)))

;; The mapper output key class; used when the mapper writer outputs
;; types different from the job output.
(defmethod conf :map-output-key [^JobConf jobconf key value]
  (.setMapOutputKeyClass jobconf (Class/forName value)))

;; The mapper output value class; used when the mapper writer outputs
;; types different from the job output.
(defmethod conf :map-output-value [^JobConf jobconf key value]
  (.setMapOutputValueClass jobconf (Class/forName value)))

;; The job output key class.
(defmethod conf :output-key [^JobConf jobconf key value]
  (.setOutputKeyClass jobconf (Class/forName value)))

;; The job output value class.
(defmethod conf :output-value [^JobConf jobconf key value]
  (.setOutputValueClass jobconf (Class/forName value)))

;; The reducer reader function, converts Hadoop Writable types to
;; native Clojure types.
(defmethod conf :reduce-reader [^JobConf jobconf key value]
  (.set jobconf "clojure-hadoop.job.reduce.reader" (as-str value))
  (.set jobconf "clojure-hadoop.job.combiner.reader" (as-str value)))

;; The reducer writer function; converts native Clojure types to
;; Hadoop Writable types.
(defmethod conf :reduce-writer [^JobConf jobconf key value]
  (.set jobconf "clojure-hadoop.job.reduce.writer" (as-str value)))

;; The input file format.  May be a class name or "text" for
;; TextInputFormat, "kvtext" fro KeyValueTextInputFormat, "seq" for
;; SequenceFileInputFormat.
(defmethod conf :input-format [^JobConf jobconf key value]
  (let [val (as-str value)]
    (cond
      (= "text" val)
      (.setInputFormat jobconf TextInputFormat)

      (= "kvtext" val)
      (.setInputFormat jobconf KeyValueTextInputFormat)

      (= "seq" val)
      (.setInputFormat jobconf SequenceFileInputFormat)

      :else
      (.setInputFormat jobconf (Class/forName value)))))

;; The output file format.  May be a class name or "text" for
;; TextOutputFormat, "seq" for SequenceFileOutputFormat.
(defmethod conf :output-format [^JobConf jobconf key value]
  (let [val (as-str value)]
    (cond
      (= "text" val)
      (.setOutputFormat jobconf TextOutputFormat)

      (= "seq" val)
      (.setOutputFormat jobconf SequenceFileOutputFormat)

      :else
      (.setOutputFormat jobconf (Class/forName value)))))

;; If true, compress job output files.
(defmethod conf :compress-output [^JobConf jobconf key value]
  (let [val (.toLowerCase (as-str value))]
    (cond
     (= "true" val)
     (FileOutputFormat/setCompressOutput jobconf true)

     (= "false" val)
     (FileOutputFormat/setCompressOutput jobconf false)

     :else
     (throw (Exception. (str "compress-output value must be true or false, but given '" val "'"))))))

;; Codec to use for compressing job output files.
(defmethod conf :output-compressor [^JobConf jobconf key value]
  (let [val (as-str value)] ;; TODO: conv to lowercase?
    (cond
     (= "default" val)
     (FileOutputFormat/setOutputCompressorClass jobconf DefaultCodec)

     (= "gzip" val)
     (FileOutputFormat/setOutputCompressorClass jobconf GzipCodec)

     (= "bzip2" val)
     (FileOutputFormat/setOutputCompressorClass jobconf BZip2Codec)

     :else
     (FileOutputFormat/setOutputCompressorClass jobconf (Class/forName value)))))

;; Type of compression to use for sequence files.
(defmethod conf :compression-type [^JobConf jobconf key value]
  (let [val (as-str value)] ;; TODO: conv to lowercase?
    (cond
     (= "block" val)
     (SequenceFileOutputFormat/setOutputCompressionType
      jobconf SequenceFile$CompressionType/BLOCK)

     (= "none" val)
     (SequenceFileOutputFormat/setOutputCompressionType
      jobconf SequenceFile$CompressionType/NONE)

     (= "record" val)
     (SequenceFileOutputFormat/setOutputCompressionType
      jobconf SequenceFile$CompressionType/RECORD))))

(defn parse-command-line-args [^JobConf jobconf args]
  (when (empty? args)
    (throw (Exception. "Missing required options.")))
  (when-not (even? (count args))
    (throw (Exception. "Number of options must be even.")))
  (doseq [[k v] (partition 2 args)]
    (conf jobconf (keyword (subs k 1)) v)))

(defn print-usage []
  (println "Usage: java -cp [jars...] clojure_hadoop.job [options...]
Required options are:
 -input     comma-separated input paths
 -output    output path
 -map       mapper function, as namespace/name or class name
 -reduce    reducer function, as namespace/name or class name
OR
 -job       job definition function, as namespace/name

Mapper or reducer function may also be \"identity\".
Reducer function may also be \"none\".

Other available options are:
 -input-format      Class name or \"text\" or \"seq\" (SeqFile)
 -output-format     Class name or \"text\" or \"seq\" (SeqFile)
 -output-key        Class for job output key
 -output-value      Class for job output value
 -map-count         Number of Mapper instances
 -map-setup         Namespace/name of setup function for mapper
 -map-output-key    Class for intermediate Mapper output key
 -map-output-value  Class for intermediate Mapper output value
 -map-reader        Mapper reader function, as namespace/name
 -map-writer        Mapper writer function, as namespace/name
 -reduce-count      Number of Reducer instances
 -reduce-setup      Namespace/name of setup function for reducer
 -reduce-reader     Reducer reader function, as namespace/name
 -reduce-writer     Reducer writer function, as namespace/name
 -combine           Combiner function, as namespace/name or class name
 -name              Job name
 -replace           If \"true\", deletes output dir before start
 -compress-output   If \"true\", compress job output files
 -output-compressor Compression class or \"gzip\",\"bzip2\",\"default\"
 -compression-type  For seqfiles, compress \"block\",\"record\",\"none\"
"))

