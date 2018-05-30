(ns grumpy.db
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [datascript.core :as d]
    [grumpy.core :as grumpy]
    [grumpy.fs :as fs]))


(def expected-db-version 2)


(def db-version (Long/parseLong (grumpy/from-config "DB_VERSION" "1")))


(defn migrate! [version f]
  (when (< db-version version)
    (println "Migrating DB to version" version)
    (doseq [post-id (grumpy/post-ids)
            :let [file (str "grumpy_data/posts/" post-id "/post.edn")
                  post (edn/read-string (slurp file))]]
      (try
        (spit file (pr-str (f post)))
        (catch Exception e
          (println "Can’t convert" file)
          (.printStackTrace e))))))


(migrate! 2
  (fn [post]
    (let [[pic] (:pictures post)]
      (cond-> post
        true (dissoc :pictures)
        (some? pic) (assoc :picture { :url pic })))))


(when (not= db-version expected-db-version)
  (spit "grumpy_data/DB_VERSION" (str expected-db-version))
  (alter-var-root #'db-version (constantly expected-db-version)))


(def schema
  { :post/id      {:db/unique :db.unique/identity}
    :post/created {:db/index true}
    :post/updated {}
    :post/author  {}
    :post/body    {}
    :post/picture {:db/type :db.type/ref}
    :post/picture-original  {:db/type :db.type/ref}
    :post/telegram-messages {:db/type :db.type/ref
                             :db/cardinality :db.cardinality/many}

    :picture/url              {}
    :picture/content-type     {}
    :picture/dimensions       {}
    :picture/telegram-message {:db/type :db.type/ref}
    
    :telegram/message_id {}
    :telegram/photo      {} })


(defn pic->entity [pic]
  (when (some? pic)
    (grumpy/some-map
      :picture/url (:url pic)
      :picture/content-type (:content-type pic)
      :picture/dimensions (:dimensions pic)
      :picture/telegram-message (not-empty
                                  (grumpy/some-map
                                    :telegram/message_id (:telegram/message_id pic)
                                    :telegram/photo      (:telegram/photo pic))))))


(defn post->tx [post]
  [(grumpy/some-map
     :post/id      (:id post)
     :post/created (:created post)
     :post/updated (:updated post)
     :post/author  (:author post)
     :post/body    (:body post)
     :post/picture (pic->entity (:picture post))
     :post/picture-original (pic->entity (:picture-original post))
     :post/telegram-messages (when-some [mid (:telegram/message_id post)]
                               [{:telegram/message_id mid}]))])


(defn import-posts-tx []
  (->> (grumpy/post-ids)
       (reverse)
       (map #(-> (str "grumpy_data/posts/" % "/post.edn") (slurp) (edn/read-string)))
       (mapcat post->tx)))


(defn persist-datoms! [db]
  (fs/atomic-spit "grumpy_data/posts.edn" (pr-str (:eavt db))))


(defn load-datoms []
  (edn/read-string {:readers d/data-readers} (slurp "grumpy_data/posts.edn")))


(defonce *db
  (if (.exists (io/file "grumpy_data/posts.edn"))
    (d/conn-from-datoms (load-datoms) schema)
    (let [conn (d/create-conn schema)]
      (d/transact! conn (import-posts-tx))
      (persist-datoms! @conn)
      conn)))


(d/listen! *db ::persist
  (fn [report]
    (persist-datoms! (:db-after report))))