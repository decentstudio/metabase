(ns aws-dynamodb.dev
  (:require [cognitect.aws.client.api :as aws]))

(defn delete-table!
  [client table-name]
  (aws/invoke client
              {:op :DeleteTable
               :request {:TableName table-name}}))

(defn create-random-table!
  [client]
  (let [table-name (str (java.util.UUID/randomUUID))]
    (aws/invoke client
                {:op :CreateTable
                 :request {:AttributeDefinitions [{:AttributeName "id"
                                                   :AttributeType "S"}]
                           :KeySchema [{:AttributeName "id"
                                        :KeyType "HASH"}]
                           :TableName table-name
                           :BillingMode "PAY_PER_REQUEST"}})))

(defn create-lots-of-tables!
  [client n]
  (doall (repeatedly n #(create-random-table! client))))