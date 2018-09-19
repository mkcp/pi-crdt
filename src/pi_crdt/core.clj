(ns pi-crdt.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io])
  (:import [java.net ServerSocket
                     Socket])
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
  (swap! g-counter #(update-in % [:p (:id %)] inc)))

(defn p-query
  []
  (let [{:keys [p]} @g-counter]
    (reduce + p)))

(defn p-merge!
  "Takes the max of every element in p"
  [y]
  (swap! g-counter update-in [:p] #(mapv max % y)))

(defn p-apply!
  [x]
  (swap! g-counter assoc-in [:p] x))

(defn p-compare
  [x y]
  (every? true? (map <= x y)))

(defn g-counter-initialize!
  [{:keys [nodes id]}]
  (p-initialize!  nodes)
  (n-initialize!  nodes)
  (id-initialize! id))

(defn message-receive
  "Takes a serialized value of p' from `msg` and merges it into local p"
  [{:keys [nodes id]} msg]
  (let [p  (get-in @g-counter [:p id])
        p' (read-string msg)]
    (when (and (= (count p') nodes)
               (<= (nth p' id) p))
      (p-merge! p'))))

(defn message-create
  "Reads and serializes the current local value of p "
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
  [{:keys [port id] :as opts} handler-in handler-out]
  (let [running (atom true)]
    (future
      (with-open [server-sock (ServerSocket. (+ port id))]
        (while @running
          (with-open [sock (.accept server-sock)]
            (let [msg-in (->> sock receive! (handler-in opts))
                  msg-out (handler-out)]
              (send! sock msg-out))))))
    running))

(defn exponential-backoff [time rate max f]
  (if (<= max time)
    (f)
    (try
      (f)
      (catch Throwable _
        (Thread/sleep time)
        (exponential-backoff (* time rate) rate max f)))))

(defn pusher
  "Push the update to other nodes"
  [{:keys [port update-rate]} i]
  (let [running (atom true)]
    (future
      (while @running
        (try
          (exponential-backoff
           100 1.1 10000
           #(with-open [sock (Socket. "0.0.0.0" (+ port i))]
              (send! sock (message-create))))
          (catch Throwable t
            (println t)))))
    running))

(defn pushers-start
  [{:keys [addresses nodes] :as opts}]
  (if-not (:addresses opts)
    (doall (map #(pusher opts %) (range nodes)))
    (doall (map #(pusher opts %) addresses))))

(defn local-updater
  "Fires `p-update` each time a user provides a new line"
  []
  (let [running (atom true)]
    (future
      (while running
        (let [input (read-line)]
          (when input
            (p-update!))))
      running)))

(defn local-reader
  "Read out the node's state at the specified interval"
  [{:keys [update-rate]}]
  (let [running (atom true)]
    (future
      (while @running
        (Thread/sleep update-rate)
        (println @g-counter)))))

(def cli-options
  [["-i" "--id ID" "Node id"
    :parse-fn #(Integer/parseInt %)]

   ["-n" "--nodes NODE_COUNT" "Nodes"
     :parse-fn #(Integer/parseInt %)
     :default 5]

   ["-p" "--port PORT" "Specify a port to listen on."
    :parse-fn #(Integer/parseInt %)
    :default 6969]

   ["-hosts" "--host-names [hostname,IP:PORT,...]" "Provide a comma delimited array of hostnames or IP:PORT"]

   ["-u" "--update-rate MS" "How long to sleep between updates, in milliseconds"
    :parse-fn #(Integer/parseInt %)
    :default 1000]

   ["-h" "--help"]])

(defn -main [& args]
  (let [opts (:options (parse-opts args cli-options))

        _ (println opts)

        ;; Initialize  node
        _              (g-counter-initialize! opts)

        server         (serve! opts message-receive message-create)
        _              (println "Listening on port" (+ (:id opts)
                                                       (:port opts)))

        pushers        (pushers-start opts)
        _              (println "Pushers started")

        console-input  (local-updater)
        _              (println "Press Enter to increment this node's value")

        counter-reader (local-reader opts)
        _              (println "Printing local state every" (str (:update-rate opts) "ms"))]
    (println "Counter started")))
