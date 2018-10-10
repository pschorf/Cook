;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns cook.test.mesos.monitor
  (:require [clj-time.core :as time]
            [clojure.test :refer :all]
            [cook.mesos :as mesos]
            [cook.mesos.api :as api]
            [cook.mesos.monitor :as monitor]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [cook.test.testutil :refer [restore-fresh-database! setup] :as testutil]
            [datomic.api :as d :refer [db]]
            [metrics.counters :as counters])
  (:import (java.util UUID)
           (org.joda.time Interval)))

(deftest test-set-stats-counters!
  (let [conn (restore-fresh-database! "datomic:mem://test-set-stats-counters!")
        counter #(counters/value (counters/counter (conj % "pool-no-pool")))
        job1 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 1., :mem 128., :user "alice"}
        job2 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 2., :mem 256., :user "bob"}
        job3 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 4., :mem 512., :user "sally"}
        stats-atom (atom {})
        run-job (fn [job] @(d/transact conn [[:db/add [:job/uuid (:uuid job)] :job/state :job.state/running]]))]

    (monitor/set-stats-counters! (db conn) stats-atom
                                 (util/get-pending-job-ents (db conn))
                                 (util/get-running-job-ents (db conn))
                                 "no-pool")
    (is (= 0 (counter ["running" "all" "cpus"])))
    (is (= 0 (counter ["running" "all" "jobs"])))
    (is (= 0 (counter ["running" "all" "mem"])))
    (is (= 0 (counter ["starved" "all" "cpus"])))
    (is (= 0 (counter ["starved" "all" "jobs"])))
    (is (= 0 (counter ["starved" "all" "mem"])))
    (is (= 0 (counter ["waiting" "all" "cpus"])))
    (is (= 0 (counter ["waiting" "all" "jobs"])))
    (is (= 0 (counter ["waiting" "all" "mem"])))
    (is (= 0 (counter ["total" "users"])))
    (is (= 0 (counter ["starved" "users"])))
    (is (= 0 (counter ["hungry" "users"])))
    (is (= 0 (counter ["satisfied" "users"])))

    (testutil/create-jobs! conn {::api/jobs [job1 job2]})

    (monitor/set-stats-counters! (db conn) stats-atom
                                 (util/get-pending-job-ents (db conn))
                                 (util/get-running-job-ents (db conn))
                                 "no-pool")
    (is (= 0 (counter ["running" "all" "jobs"])))
    (is (= 0 (counter ["running" "all" "cpus"])))
    (is (= 0 (counter ["running" "all" "mem"])))
    (is (= 2 (counter ["waiting" "all" "jobs"])))
    (is (= 3 (counter ["waiting" "all" "cpus"])))
    (is (= 384 (counter ["waiting" "all" "mem"])))
    (is (= 1 (counter ["waiting" "alice" "jobs"])))
    (is (= 1 (counter ["waiting" "alice" "cpus"])))
    (is (= 128 (counter ["waiting" "alice" "mem"])))
    (is (= 1 (counter ["waiting" "bob" "jobs"])))
    (is (= 2 (counter ["waiting" "bob" "cpus"])))
    (is (= 256 (counter ["waiting" "bob" "mem"])))
    (is (= 2 (counter ["starved" "all" "jobs"])))
    (is (= 3 (counter ["starved" "all" "cpus"])))
    (is (= 384 (counter ["starved" "all" "mem"])))
    (is (= 1 (counter ["starved" "alice" "jobs"])))
    (is (= 1 (counter ["starved" "alice" "cpus"])))
    (is (= 128 (counter ["starved" "alice" "mem"])))
    (is (= 1 (counter ["starved" "bob" "jobs"])))
    (is (= 2 (counter ["starved" "bob" "cpus"])))
    (is (= 256 (counter ["starved" "bob" "mem"])))
    (is (= 2 (counter ["total" "users"])))
    (is (= 2 (counter ["starved" "users"])))
    (is (= 0 (counter ["hungry" "users"])))
    (is (= 0 (counter ["satisfied" "users"])))

    (run-job job1)
    (monitor/set-stats-counters! (db conn) stats-atom
                                 (util/get-pending-job-ents (db conn))
                                 (util/get-running-job-ents (db conn))
                                 "no-pool")
    (is (= 1 (counter ["running" "all" "jobs"])))
    (is (= 1 (counter ["running" "all" "cpus"])))
    (is (= 128 (counter ["running" "all" "mem"])))
    (is (= 1 (counter ["running" "alice" "jobs"])))
    (is (= 1 (counter ["running" "alice" "cpus"])))
    (is (= 128 (counter ["running" "alice" "mem"])))
    (is (= 1 (counter ["waiting" "all" "jobs"])))
    (is (= 2 (counter ["waiting" "all" "cpus"])))
    (is (= 256 (counter ["waiting" "all" "mem"])))
    (is (= 0 (counter ["waiting" "alice" "jobs"])))
    (is (= 0 (counter ["waiting" "alice" "cpus"])))
    (is (= 0 (counter ["waiting" "alice" "mem"])))
    (is (= 1 (counter ["waiting" "bob" "jobs"])))
    (is (= 2 (counter ["waiting" "bob" "cpus"])))
    (is (= 256 (counter ["waiting" "bob" "mem"])))
    (is (= 1 (counter ["starved" "all" "jobs"])))
    (is (= 2 (counter ["starved" "all" "cpus"])))
    (is (= 256 (counter ["starved" "all" "mem"])))
    (is (= 0 (counter ["starved" "alice" "jobs"])))
    (is (= 0 (counter ["starved" "alice" "cpus"])))
    (is (= 0 (counter ["starved" "alice" "mem"])))
    (is (= 1 (counter ["starved" "bob" "jobs"])))
    (is (= 2 (counter ["starved" "bob" "cpus"])))
    (is (= 256 (counter ["starved" "bob" "mem"])))
    (is (= 2 (counter ["total" "users"])))
    (is (= 1 (counter ["starved" "users"])))
    (is (= 0 (counter ["hungry" "users"])))
    (is (= 1 (counter ["satisfied" "users"])))

    (mesos/kill-job conn [(:uuid job1)])
    (monitor/set-stats-counters! (db conn) stats-atom
                                 (util/get-pending-job-ents (db conn))
                                 (util/get-running-job-ents (db conn))
                                 "no-pool")
    (is (= 0 (counter ["running" "all" "jobs"])))
    (is (= 0 (counter ["running" "all" "cpus"])))
    (is (= 0 (counter ["running" "all" "mem"])))
    (is (= 1 (counter ["waiting" "all" "jobs"])))
    (is (= 2 (counter ["waiting" "all" "cpus"])))
    (is (= 256 (counter ["waiting" "all" "mem"])))
    (is (= 0 (counter ["waiting" "alice" "jobs"])))
    (is (= 0 (counter ["waiting" "alice" "cpus"])))
    (is (= 0 (counter ["waiting" "alice" "mem"])))
    (is (= 1 (counter ["waiting" "bob" "jobs"])))
    (is (= 2 (counter ["waiting" "bob" "cpus"])))
    (is (= 256 (counter ["waiting" "bob" "mem"])))
    (is (= 1 (counter ["starved" "all" "jobs"])))
    (is (= 2 (counter ["starved" "all" "cpus"])))
    (is (= 256 (counter ["starved" "all" "mem"])))
    (is (= 0 (counter ["starved" "alice" "jobs"])))
    (is (= 0 (counter ["starved" "alice" "cpus"])))
    (is (= 0 (counter ["starved" "alice" "mem"])))
    (is (= 1 (counter ["starved" "bob" "jobs"])))
    (is (= 2 (counter ["starved" "bob" "cpus"])))
    (is (= 256 (counter ["starved" "bob" "mem"])))
    (is (= 1 (counter ["total" "users"])))
    (is (= 1 (counter ["starved" "users"])))
    (is (= 0 (counter ["hungry" "users"])))
    (is (= 0 (counter ["satisfied" "users"])))

    (run-job job2)
    (monitor/set-stats-counters! (db conn) stats-atom
                                 (util/get-pending-job-ents (db conn))
                                 (util/get-running-job-ents (db conn))
                                 "no-pool")
    (is (= 1 (counter ["running" "all" "jobs"])))
    (is (= 2 (counter ["running" "all" "cpus"])))
    (is (= 256 (counter ["running" "all" "mem"])))
    (is (= 1 (counter ["running" "bob" "jobs"])))
    (is (= 2 (counter ["running" "bob" "cpus"])))
    (is (= 256 (counter ["running" "bob" "mem"])))
    (is (= 0 (counter ["waiting" "all" "jobs"])))
    (is (= 0 (counter ["waiting" "all" "cpus"])))
    (is (= 0 (counter ["waiting" "all" "mem"])))
    (is (= 0 (counter ["waiting" "alice" "jobs"])))
    (is (= 0 (counter ["waiting" "alice" "cpus"])))
    (is (= 0 (counter ["waiting" "alice" "mem"])))
    (is (= 0 (counter ["waiting" "bob" "jobs"])))
    (is (= 0 (counter ["waiting" "bob" "cpus"])))
    (is (= 0 (counter ["waiting" "bob" "mem"])))
    (is (= 0 (counter ["starved" "all" "jobs"])))
    (is (= 0 (counter ["starved" "all" "cpus"])))
    (is (= 0 (counter ["starved" "all" "mem"])))
    (is (= 0 (counter ["starved" "alice" "jobs"])))
    (is (= 0 (counter ["starved" "alice" "cpus"])))
    (is (= 0 (counter ["starved" "alice" "mem"])))
    (is (= 0 (counter ["starved" "bob" "jobs"])))
    (is (= 0 (counter ["starved" "bob" "cpus"])))
    (is (= 0 (counter ["starved" "bob" "mem"])))
    (is (= 1 (counter ["total" "users"])))
    (is (= 0 (counter ["starved" "users"])))
    (is (= 0 (counter ["hungry" "users"])))
    (is (= 1 (counter ["satisfied" "users"])))

    (mesos/kill-job conn [(:uuid job2)])
    (monitor/set-stats-counters! (db conn) stats-atom
                                 (util/get-pending-job-ents (db conn))
                                 (util/get-running-job-ents (db conn))
                                 "no-pool")
    (is (= 0 (counter ["running" "all" "jobs"])))
    (is (= 0 (counter ["running" "all" "cpus"])))
    (is (= 0 (counter ["running" "all" "mem"])))
    (is (= 0 (counter ["waiting" "all" "jobs"])))
    (is (= 0 (counter ["waiting" "all" "cpus"])))
    (is (= 0 (counter ["waiting" "all" "mem"])))
    (is (= 0 (counter ["waiting" "alice" "jobs"])))
    (is (= 0 (counter ["waiting" "alice" "cpus"])))
    (is (= 0 (counter ["waiting" "alice" "mem"])))
    (is (= 0 (counter ["waiting" "bob" "jobs"])))
    (is (= 0 (counter ["waiting" "bob" "cpus"])))
    (is (= 0 (counter ["waiting" "bob" "mem"])))
    (is (= 0 (counter ["starved" "all" "jobs"])))
    (is (= 0 (counter ["starved" "all" "cpus"])))
    (is (= 0 (counter ["starved" "all" "mem"])))
    (is (= 0 (counter ["starved" "alice" "jobs"])))
    (is (= 0 (counter ["starved" "alice" "cpus"])))
    (is (= 0 (counter ["starved" "alice" "mem"])))
    (is (= 0 (counter ["starved" "bob" "jobs"])))
    (is (= 0 (counter ["starved" "bob" "cpus"])))
    (is (= 0 (counter ["starved" "bob" "mem"])))
    (is (= 0 (counter ["total" "users"])))
    (is (= 0 (counter ["starved" "users"])))
    (is (= 0 (counter ["hungry" "users"])))
    (is (= 0 (counter ["satisfied" "users"])))

    (testutil/create-jobs! conn {::api/jobs [job3]})
    (with-redefs [share/get-share (constantly {:cpus 0 :mem 0})]

      (monitor/set-stats-counters! (db conn) stats-atom
                                   (util/get-pending-job-ents (db conn))
                                   (util/get-running-job-ents (db conn))
                                   "no-pool"))
    (is (= 0 (counter ["running" "all" "jobs"])))
    (is (= 0 (counter ["running" "all" "cpus"])))
    (is (= 0 (counter ["running" "all" "mem"])))
    (is (= 1 (counter ["waiting" "all" "jobs"])))
    (is (= 4 (counter ["waiting" "all" "cpus"])))
    (is (= 512 (counter ["waiting" "all" "mem"])))
    (is (= 0 (counter ["waiting" "alice" "jobs"])))
    (is (= 0 (counter ["waiting" "alice" "cpus"])))
    (is (= 0 (counter ["waiting" "alice" "mem"])))
    (is (= 0 (counter ["waiting" "bob" "jobs"])))
    (is (= 0 (counter ["waiting" "bob" "cpus"])))
    (is (= 0 (counter ["waiting" "bob" "mem"])))
    (is (= 1 (counter ["waiting" "sally" "jobs"])))
    (is (= 4 (counter ["waiting" "sally" "cpus"])))
    (is (= 512 (counter ["waiting" "sally" "mem"])))
    (is (= 0 (counter ["starved" "all" "jobs"])))
    (is (= 0 (counter ["starved" "all" "cpus"])))
    (is (= 0 (counter ["starved" "all" "mem"])))
    (is (= 0 (counter ["starved" "alice" "jobs"])))
    (is (= 0 (counter ["starved" "alice" "cpus"])))
    (is (= 0 (counter ["starved" "alice" "mem"])))
    (is (= 0 (counter ["starved" "bob" "jobs"])))
    (is (= 0 (counter ["starved" "bob" "cpus"])))
    (is (= 0 (counter ["starved" "bob" "mem"])))
    (is (= 0 (counter ["starved" "sally" "jobs"])))
    (is (= 0 (counter ["starved" "sally" "cpus"])))
    (is (= 0 (counter ["starved" "sally" "mem"])))
    (is (= 1 (counter ["total" "users"])))
    (is (= 0 (counter ["starved" "users"])))
    (is (= 1 (counter ["hungry" "users"])))
    (is (= 0 (counter ["satisfied" "users"])))

    (run-job job3)
    (monitor/set-stats-counters! (db conn) stats-atom
                                 (util/get-pending-job-ents (db conn))
                                 (util/get-running-job-ents (db conn))
                                 "no-pool")
    (is (= 1 (counter ["running" "all" "jobs"])))
    (is (= 4 (counter ["running" "all" "cpus"])))
    (is (= 512 (counter ["running" "all" "mem"])))
    (is (= 1 (counter ["running" "sally" "jobs"])))
    (is (= 4 (counter ["running" "sally" "cpus"])))
    (is (= 512 (counter ["running" "sally" "mem"])))
    (is (= 0 (counter ["waiting" "all" "jobs"])))
    (is (= 0 (counter ["waiting" "all" "cpus"])))
    (is (= 0 (counter ["waiting" "all" "mem"])))
    (is (= 0 (counter ["waiting" "alice" "jobs"])))
    (is (= 0 (counter ["waiting" "alice" "cpus"])))
    (is (= 0 (counter ["waiting" "alice" "mem"])))
    (is (= 0 (counter ["waiting" "bob" "jobs"])))
    (is (= 0 (counter ["waiting" "bob" "cpus"])))
    (is (= 0 (counter ["waiting" "bob" "mem"])))
    (is (= 0 (counter ["waiting" "sally" "jobs"])))
    (is (= 0 (counter ["waiting" "sally" "cpus"])))
    (is (= 0 (counter ["waiting" "sally" "mem"])))
    (is (= 0 (counter ["starved" "all" "jobs"])))
    (is (= 0 (counter ["starved" "all" "cpus"])))
    (is (= 0 (counter ["starved" "all" "mem"])))
    (is (= 0 (counter ["starved" "alice" "jobs"])))
    (is (= 0 (counter ["starved" "alice" "cpus"])))
    (is (= 0 (counter ["starved" "alice" "mem"])))
    (is (= 0 (counter ["starved" "bob" "jobs"])))
    (is (= 0 (counter ["starved" "bob" "cpus"])))
    (is (= 0 (counter ["starved" "bob" "mem"])))
    (is (= 0 (counter ["starved" "sally" "jobs"])))
    (is (= 0 (counter ["starved" "sally" "cpus"])))
    (is (= 0 (counter ["starved" "sally" "mem"])))
    (is (= 1 (counter ["total" "users"])))
    (is (= 0 (counter ["starved" "users"])))
    (is (= 0 (counter ["hungry" "users"])))
    (is (= 1 (counter ["satisfied" "users"])))))

(deftest test-start-collecting-stats
  (setup :config {:metrics {:user-metrics-interval-seconds 1}})
  (let [period (atom nil)
        millis-between (fn [a b]
                         (-> a
                             ^Interval (time/interval b)
                             .toDuration
                             .getMillis))]
    (with-redefs [chime/chime-at (fn [p _ _] (reset! period p))]
      (monitor/start-collecting-stats)
      (is (= 1000 (millis-between (first @period) (second @period))))
      (is (= 1000 (millis-between (second @period) (nth @period 2))))
      (is (= 1000 (millis-between (nth @period 2) (nth @period 3)))))))
