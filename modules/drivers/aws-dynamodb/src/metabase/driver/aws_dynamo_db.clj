(ns metabase.driver.aws-dynamo-db
  (:require [metabase.driver :as driver]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as aws-creds]
            [cognitect.aws.region :as aws-region]
            [cognitect.anomalies :as anom]
            [clojure.spec.alpha :as s]))

(driver/register! :aws-dynamo-db)

(defn get-client 
  [{:keys [api region access-key-id secret-access-key] :as args}]
  (aws/client {:api api
               :region region
               :credentials-provider
               (aws-creds/basic-credentials-provider
                (select-keys args [:access-key-id :secret-access-key]))}))

(defn can-connect?
  [{:keys [api region table-name access-key-id secret-access-key] :as details}]
  (let [conn-attempt (aws/invoke (get-client details)
                                 {:op :DescribeTable
                                  :request {:TableName table-name}})]
    (if (s/valid? ::anom/anomaly conn-attempt)
      (throw (Exception. (:message conn-attempt)))
      true)))

(defmethod driver/can-connect? :aws-dynamo-db
  [_ details]
  (can-connect? details))
