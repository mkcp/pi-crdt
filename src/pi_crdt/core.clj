(ns pi-crdt.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io])
  (:import [java.net ServerSocket])
  (:gen-class))

(defonce g-counter (atom {:p []
                          :id :uninitialized
                          :n  :uninitialized}))

(defn n-initialize! [n]
  (swap! g-counter assoc-in [:n] n))

(defn id-initialize! [id]
  (swap! g-counter assoc-in [:id] id))

(defn p-initialize!
  "Each node is assigned its own slot in the array p"
  [n]
  (swap! g-counter assoc-in [:p] (vec (repeat n 0))))

(defn p-update!
  []
  (swap! g-counter update-in [:p (:id @g-counter)] inc))

(defn p-query
  []
  (let [{:keys [p]} @g-counter]
    (reduce + p)))

(defn p-merge!
  "Takes the max of every element in p"
  [y]
  (swap! g-counter mapv (max (:p @g-counter) y)))

(defn p-apply!
  [x]
  (swap! g-counter assoc-in [:p] x))

(defn p-compare
  [x y]
  (every? true? (map <= x y)))

(defn g-counter-initialize!
  [n id]
  (p-initialize!  n)
  (n-initialize!  n)
  (id-initialize! id))

(defn message-receive
  [msg]
  (let [p' (read-string msg)]
    (p-merge p')))

(defn message-send
  "Reads the current local value of p and serializes it"
  []
  (-> g-counter
      deref
      :p
      str))

(defn receive!
  "Read a line of text from the given socket"
  [socket]
  (.readLine (io/reader socket)))

(defn send!
  "Send the given string out over the given socket"
  [socket msg]
  (let [writer (io/writer socket)]
    (.write writer msg)
    (.flush writer)))

(defn serve!
  [port handler-in handler-out]
  (let [running (atom true)]
    (future
      (with-open [server-sock (ServerSocket. port)
                  sock (.accept server-sock)]
        (while @running
          (let [msg-in (-> sock receive! handler-in)
                msg-out (handler-out msg-in)]
            (send! sock msg-out)))))
    running))


(defn local-reader
  [update-rate]
  (let [running (atom true)]
    (future
      (while @running
        (Thread/sleep update-rate)
        (println @g-counter)))))

(def cli-options
  [["-i" "--id ID" "Node id"
    :parse-fn #(Integer/parseInt %)]
   ["-n" "--nodes NODE-COUNT" "Nodes"
     :parse-fn #(Integer/parseInt %)
     :default 5]
   ["-r" "--repl" "Run in interactive mode"]
   ["-p" "--port PORT" "Specify a port to listen on."
    :parse-fn #(Integer/parseInt %)
    :default 6969]
   ["-u" "--update-rate MS" "How long to sleep between updates, in milliseconds"
    :parse-fn #(Integer/parseInt %)
    :default 1000]
   ["-h" "--help"]])

(defn -main [& args]
  (let [o (parse-opts args cli-options)
        {:keys [id
                nodes
                repl
                port
                update-rate]} (:options o)
        port    (+ port id)

        ;; Initialize  node
        _   (g-counter-initialize! nodes id)

        ;; Start server
        server (serve! port message-receive message-send)
        _      (println "Listening on port" port)

        ;; Increment each time a user provides a new line
        _ (.start (Thread. (fn []
                             (loop []
                               (let [input (read-line)]
                                 (when input
                                   (p-update!))
                                 (recur))))))

        ;; Read out the node's state at the specified interval
        counter-reader (local-reader update-rate)
        _              (println "Printing local state every" (str update-rate "ms"))]

    ;; TODO Push the update to other nodes
    #_(dotimes [i nodes]
      (.start (Thread. (fn [] (serve port )))))))
