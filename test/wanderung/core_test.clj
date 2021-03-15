(ns wanderung.core-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [wanderung.core :refer [migrate]]
            [datahike.api :as d]
            [datomic.client.api :as dt])
  (:import [java.util UUID]))
