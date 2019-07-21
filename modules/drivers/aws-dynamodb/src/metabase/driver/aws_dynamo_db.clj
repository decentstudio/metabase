(ns metabase.driver.aws-dynamo-db
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

(defn can-connect?
  [{:keys [region access-key-id secret-access-key] :as details}]
  (let [conn-attempt (aws/invoke (get-client (assoc details :api :dynamodb))
                                 {:op :ListTables})]
    (if (s/valid? ::anom/anomaly conn-attempt)
      (throw (Exception. ^String (:message conn-attempt)))
      true)))

(defmethod driver/can-connect? :aws-dynamo-db
  [_ details]
  (can-connect? details))

(defn details->client [details]
  (get-client (assoc details :api :dynamodb)))

(defn list-all-tables!
  "Lists all DynamoDB tables taking pagination into account."
  [details]
  (:TableNames (aws/invoke (details->client details)
                           {:op :ListTables})))

(defn ->DatabaseMetadataTable [^String s]
  {:name s
   :schema ""})

(defn describe-database!
  "Describe database as required by metabase."
  [details]
  {:tables (into #{} (r/map ->DatabaseMetadataTable (list-all-tables! details)))})

(defmethod driver/describe-database :aws-dynamo-db 
  [_ {:keys [details]}]
  (describe-database! details))