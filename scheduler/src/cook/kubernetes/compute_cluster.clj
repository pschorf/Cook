(ns cook.kubernetes.compute-cluster
  (:require [cook.compute-cluster :as cc]
            [cook.datomic]
            [datomic.api :as d])
  (:import (io.kubernetes.client ApiClient)
           (io.kubernetes.client.util Config)))

(defrecord KubernetesComputeCluster [^ApiClient api-client entity-id]
  cc/ComputeCluster
  (launch-tasks [this offers task-metadata-seq]
    (throw (UnsupportedOperationException. "Cannot launch tasks")))

  (db-id [this]
    entity-id)

  (initialize-cluster [this pool->fenzo pool->offers-chan])

  (current-leader? [this]
    true)

  (get-mesos-driver-hack [this]
    (throw (UnsupportedOperationException. "KubernetesComputeCluster does not have a mesos driver"))))

(defn get-or-create-cluster-entity-id
  [conn compute-cluster-name]
  (let [query-result (d/q '[:find [?c]
                            :in $ ?cluster-name
                            :where
                            [?c :compute-cluster/type :compute-cluster.type/kubernetes]
                            [?c :compute-cluster/cluster-name ?cluster-name]]
                          (d/db conn) compute-cluster-name)]
    (if (seq query-result)
      (first query-result)
      (cc/write-compute-cluster conn {:compute-cluster/type :compute-cluster.type/kubernetes
                                      :compute-cluster/cluster-name compute-cluster-name}))))

(defn factory-fn
  [{:keys [compute-cluster-name ^String config-file]} context]
  (let [conn cook.datomic/conn
        cluster-entity-id (get-or-create-cluster-entity-id conn compute-cluster-name)
        api-client (Config/fromConfig config-file)
        compute-cluster (->KubernetesComputeCluster api-client cluster-entity-id)]
    (cc/register-compute-cluster! compute-cluster)
    compute-cluster))