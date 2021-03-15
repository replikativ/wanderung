(ns wanderung.core-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [wanderung.core :refer [migrate]]
            [datahike.api :as d]
            [datomic.client.api :as dt])
  (:import [java.util UUID]))

(def schema [{:db/ident       :name
              :db/valueType   :db.type/string
              :db/cardinality :db.cardinality/one}
             {:db/ident       :sibling
              :db/valueType   :db.type/ref
              :db/cardinality :db.cardinality/many}])
(def datahike-cfg {:store {:backend :mem
                           :id      "wanderung-test"}})
(def datomic-cfg {:server-type :dev-local
                  :storage-dir :mem
                  :system      "dev"})
(def datomic-db-cfg {:db-name "wanderung-test"})

(defn database-reset-fixture
  [test-function]
  (let [datomic-client (dt/client datomic-cfg)]

    (dt/delete-database datomic-client datomic-db-cfg)
    (dt/create-database datomic-client datomic-db-cfg)

    (d/delete-database datahike-cfg)
    (d/create-database datahike-cfg)

    (let [datomic-conn (dt/connect datomic-client datomic-db-cfg)]
      (dt/transact datomic-conn {:tx-data schema}))

    (test-function)
    (dt/delete-database datomic-client datomic-db-cfg)))

(use-fixtures :each database-reset-fixture)

(deftest datomic->datahike-test
  (testing "Migrate data from Datomic to Datahike"
    (let [datomic-conn (dt/connect (dt/client datomic-cfg) datomic-db-cfg) ]

      (dt/transact datomic-conn {:tx-data schema})

      ;; generate data in Datomic
      (dotimes [n 5]
        (let [possible-siblings (->> (dt/db datomic-conn)
                                     (dt/q '[:find ?e :where [?e :name _]])
                                     (take 10)
                                     vec)
              new-entities (->> (repeatedly 10 (fn [] (str (UUID/randomUUID))))
                                (map (fn [entity]
                                       (if (empty? possible-siblings)
                                         {:name entity}
                                         {:name entity :sibling (rand-nth possible-siblings)}))))]
          (dt/transact datomic-conn {:tx-data (vec new-entities)})))

      ;; migrate to Datahike
      (migrate [:datomic-cloud :datahike] (merge datomic-db-cfg datomic-cfg) datahike-cfg)

      (let [datahike-conn (d/connect datahike-cfg)
            q1 '[:find (count ?e)
                 :where [?e :name _]]
            q2 '[:find ?n
                 :where [?e :name ?n]]
            datomic-db (dt/db datomic-conn)]
        (is (= (dt/q q1 datomic-db)
               (d/q q1 @datahike-conn)))
        (is (= (->> (mapv :db/ident schema)
                    (concat [:db.entity/attrs :db/ident :db.entity/preds])
                    (into #{}))
               (->> @datahike-conn :rschema :db/ident (into #{}))))
        (is (= (into #{} (dt/q q2 datomic-db))
               (d/q q2 @datahike-conn)))))))
