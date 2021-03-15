(ns wanderung.datahike-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [wanderung.core :refer [migrate]]
            [datahike.api :as d])
  (:import [java.util UUID]))

(def schema [{:db/ident       :name
              :db/valueType   :db.type/string
              :db/cardinality :db.cardinality/one}
             {:db/ident       :sibling
              :db/valueType   :db.type/ref
              :db/cardinality :db.cardinality/many}])
(def source-cfg {:store {:backend :mem
                         :id "wanderung-source"}})
(def target-cfg {:store {:backend :mem
                         :path "wanderung-target"}})


(defn database-reset-fixture
  [test-function]

  (d/delete-database source-cfg)
  (d/create-database source-cfg)

  (d/delete-database target-cfg)
  (d/create-database target-cfg)

  (let [source-conn (d/connect source-cfg)]
      (d/transact source-conn schema))
  (test-function)

  (d/delete-database source-cfg)
  (d/delete-database target-cfg))

(use-fixtures :each database-reset-fixture)

(deftest datahike->datahike-test
  (testing "Migrate data from Datomic to Datahike"
    (let [source-conn (d/connect source-cfg)]
      (dotimes [n 5]
        (let [possible-siblings (->> @source-conn
                                     (d/q '[:find ?e :where [?e :name _]])
                                     (take 10)
                                     vec)
              new-entities (->> (repeatedly 10 (fn [] (str (UUID/randomUUID))))
                                (map (fn [entity]
                                       (if (empty? possible-siblings)
                                         {:name entity}
                                         {:name entity :sibling (rand-nth possible-siblings)}))))]
          (d/transact source-conn (vec new-entities))))

      (migrate [:datahike :datahike] source-cfg target-cfg)

      (let [target-conn (d/connect target-cfg)
            q1 '[:find (count ?e)
                 :where [?e :name _]]
            q2 '[:find ?n
                 :where [?e :name ?n]]]
        (is (=(d/q q1 @target-conn)
              (d/q q1 @source-conn)))
        (is (= (->> @target-conn :rschema :db/ident (into #{}))
               (->> @source-conn :rschema :db/ident (into #{}))))
        (is (= (d/q q2 @target-conn)
               (d/q q2 @source-conn)))
        (is (= (-> @target-conn :schema :name)
               (-> @source-conn :schema :name)))))))
