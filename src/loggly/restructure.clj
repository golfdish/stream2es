(ns loggly.restructure
  (:require [stream2es.main :as main]
            [stream2es.es :as es]
            [stream2es.log :as log])
  (:import [clojure.lang ExceptionInfo]
           [java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue
                                 TimeUnit]))

(defmacro in-thread [thread-name & forms]
  `(.start
     (Thread.
       (fn [] ~@forms)
       ~thread-name)))

(defn get-splitter-policy [opts]
  ; XXX
  (fn [cust-id]
    3))

(defn do-until-stop [source action]
  (loop []
    (let [x (source)]
      (when-not (= :stop x)
        (action x)
        (recur)))))

(defn start-index-worker-pool
  "takes a number of workers, a number of bulks to queue, a function
   to call on completion, and a function to index a single bulk.
   Returns a function that can be called with a
   list of documents or with :stop to signal done

   stolen with modifications from stream2es main"
  [{:keys [workers queue-size done-notifier do-index]}]
  (let [q (LinkedBlockingQueue. queue-size)
        latch (CountDownLatch. workers)
        disp (fn []
               (do-until-stop #(.take q) do-index)
               (log/debug "waiting for POSTs to finish")
               (.countDown latch))
        lifecycle (fn []
                    (.await latch)
                    (log/debug "done indexing")
                    (done-notifier))]
    ;; start index pool
    (dotimes [n workers]
      (.start (Thread. disp (str "indexer " (inc n)))))
    ;; notify when done
    (.start (Thread. lifecycle "index service"))
    ;; This becomes :indexer above!
    (fn [bulk]
      (if (= bulk :stop)
        (dotimes [n workers]
          (.put q :stop))
        (.put q bulk)))))

(defn start-indexer [signal-stop bulk-sink
                     {:keys [batch-size index-limit] :as opts}]
  (let [q (LinkedBlockingQueue.) ;XXX
        building-batch (atom [])
        batch-doc-count (atom 0)
        total-doc-count (atom 0)
        do-flush (fn []
                   (bulk-sink @building-batch)
                   (reset! batch-doc-count 0)
                   (reset! building-batch []))]
    (in-thread "indexer-thread-N" ;XXX
      (loop []
        (let [item (.take q)]
          (if (= item :stop)
            (do
              (do-flush)
              (bulk-sink :stop))
            (do
              (swap! building-batch conj item)
              (when (= (swap! batch-doc-count inc)
                       batch-size)
                (do-flush))
              (when (> (swap! total-doc-count inc)
                       index-limit)
                (signal-stop))
              (recur))))))
    (fn [item] (.put q item)))

  )

(defn start-indexers [index-names finish signal-stop index-fn-fact opts]
  (for [iname index-names]
    (start-indexer
      signal-stop
      (start-index-worker-pool ;XXX
        finish
        (index-fn-fact iname))
      opts)))

(defn get-target-index-names [opts]
  ; XXX
  ["foo" "bar" "baz"])

(defn get-cust [item] 
  ; XXX
  5)

(defn start-splitter [policy indexers continue? finish]
  (let [q (LinkedBlockingQueue.)
        flush-indexers (fn [] (doseq [indexer indexers]
                                (indexer :stop)))]
    (in-thread "splitter-thread"
      (loop []
        (let [item (.take q)]
          (case item
            :stop (do
                    (flush-indexers)
                    (finish :all))
            :new-index (let [ind-name (.take q)]
                         (if (continue?)
                           (recur)
                           (do
                             (flush-indexers)
                             (finish ind-name))))
            (let [cust (get-cust item)
                  indexer-id (policy cust)
                  indexer (nth indexers indexer-id)]
              (indexer item)
              (recur))))))
    (fn [item] (.put q item))))

(defn create-target-indexes [names opts]
  ; XXX
  )

(defmacro in-daemon [thread-name & forms]
  `(doto (Thread. 
           (fn [] ~@forms)
           ~thread-name)
     (.setDaemon true)
     .start
     ))

(defn get-item-stream [host index-names]
  ; XXX
  )

(defn do-index [target-url bulk]
  (when (and (sequential? bulk) (pos? (count bulk)))
    (let [first-id (-> bulk first :meta :index :_id)
          idxbulk (main/make-indexable-bulk bulk)
          idxbulkbytes (count (.getBytes idxbulk))
          url (format "%s/%s" target-url "_bulk") ]
      (es/error-capturing-bulk url bulk main/make-indexable-bulk))))

(defn make-url [hostname index-name]
  (format "http://%s:9200/%s" hostname index-name))

(defn main [{:keys [source-index-names target-count source-host
                    target-host shard-count]
             :as opts}]
  (let [target-index-names (get-target-index-names opts)
        indexer-done-latch (CountDownLatch. target-count)
        continue-flag      (atom true)
        index-fn-fact      (fn [iname]
                             (let [url (make-url target-host iname)]
                               (fn [bulk] (do-index url bulk))))
        indexers           (start-indexers
                             target-index-names
                             #(.countDown indexer-done-latch)
                             #(reset! continue-flag false)
                             index-fn-fact
                             opts)
        splitter-policy    (get-splitter-policy opts)
        done-reporter      (fn [up-to]
                             (.await indexer-done-latch)
                             (println "done indexing up to " up-to))
        splitter           (start-splitter
                             splitter-policy
                             indexers
                             (fn [] @continue-flag)
                             done-reporter)]
    (create-target-indexes target-index-names opts)
    (in-daemon "scan-thread"
      (doseq [item (get-item-stream source-host source-index-names)]
        (splitter item)))))
