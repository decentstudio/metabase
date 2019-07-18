(defproject metabase/aws-dynamodb-driver "0.0.1"
  :min-lein-version "2.5.0"

  :dependencies
  [[com.cognitect.aws/api "0.8.345"]
   [com.cognitect.aws/endpoints "1.1.11.590"]
   [com.cognitect.aws/dynamodb "726.2.484.0"]]

  :profiles
  {:provided
   {:dependencies
    [[metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "aws-dynamodb-driver.metabase-driver.jar"}})