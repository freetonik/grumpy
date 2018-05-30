(ns grumpy.fs
  (:require
    [grumpy.core :as grumpy])
  (:import
    [java.nio.file FileSystems Path Files CopyOption StandardCopyOption]
    [java.nio.file.attribute FileAttribute]))


(defn path [p]
  (.getPath (FileSystems/getDefault) p (grumpy/array String [])))


(defn atomic-spit [dst content]
  (let [[_ prefix suffix] (re-matches #"(.*)(\.[^.]+)" (str (.getFileName (path dst))))
        temp (Files/createTempFile (.getParent (path dst)) prefix suffix (grumpy/array FileAttribute []))]
    (spit (.toFile temp) content)
    (Files/move temp (path dst) (grumpy/array CopyOption [StandardCopyOption/ATOMIC_MOVE]))))
