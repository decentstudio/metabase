(ns metabase.driver.aws-dynamo-db
  "Apparently `database` contains a `details` key. This wasn't easy to figure out."
  (:require [metabase.driver :as driver]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as aws-creds]
            [cognitect.aws.region :as aws-region]
            [cognitect.anomalies :as anom]
            [clojure.spec.alpha :as s]
            [clojure.core.reducers :as r]))

(set! *warn-on-reflection* true)

(driver/register! :aws-dynamo-db)

(defn get-client 
  [{:keys [api region access-key-id secret-access-key] :as args}]
  (aws/client {:api api
               :region region
               :credentials-provider
               (aws-creds/basic-credentials-provider
                (select-keys args [:access-key-id :secret-access-key]))}))

(defn details->client [details]
  (get-client (assoc details :api :dynamodb)))

(defn can-connect?
  [client]
  (let [conn-attempt (aws/invoke client
                                 {:op :ListTables})]
    (if (s/valid? ::anom/anomaly conn-attempt)
      (throw (Exception. ^String (:message conn-attempt)))
      true)))

(defmethod driver/can-connect? :aws-dynamo-db
  [_ details]
  (can-connect? (details->client details)))

(defn list-tables! [client & {:keys [cursor]}]
  (aws/invoke client
              (merge {:op :ListTables}
                     (when cursor
                       {:request {:ExclusiveStartTableName cursor}}))))

(defn list-all-tables!
  "Return a lazy sequence of all table names, accounting for pagination."
  ([client] (list-all-tables! client :init))
  ([client cursor]
   (if cursor
     (let [result (list-tables! client :cursor (if (= :init cursor) nil cursor))]
       (lazy-seq (concat (:TableNames result)
                         (list-all-tables! client (:LastEvaluatedTableName result)))))
     nil)))

(defn ->DatabaseMetadataTable [^String s]
  {:name s
   :schema ""})

(defn describe-database!
  "Describe database as required by metabase."
  [client]
  {:tables (into #{} (r/map ->DatabaseMetadataTable (list-all-tables! (details->client details))))})

(defmethod driver/describe-database :aws-dynamo-db 
  [_ {:keys [details]}]
  (describe-database! (details->client details)))