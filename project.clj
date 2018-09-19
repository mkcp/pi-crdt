(defproject pi-crdt "0.1.0-SNAPSHOT"
  :description "Some CRDTs to be deployed a raspberry pi cluster"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.cli "0.4.0"]]
  :main pi-crdt.core/-main)
