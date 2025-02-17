(ns clojupyter.kernel.cljsrv
  "Interface to NREPL server mainly enabling Clojupyter to evaluate Clojure code entered into Jupyter
  code cells, but supporting code completion, access to documentation strings, and interrupting
  currently running evaluations."
  (:require
   [clojure.pprint          :as pp]
   [io.simplect.compose                 :refer [def-]]
   [nrepl.core              :as nrepl]
   [nrepl.server]
   ,,
   [clojupyter.messages     :as msgs]
   [clojupyter.kernel.nrepl-middleware  :as mw]
   [clojupyter.log          :as log]
   [clojupyter.util-actions     :as u!]
   [clojupyter.util :as u]
   [clojupyter.misc.kind]

   ))

;;; ------------------------------------------------------------------------------------------------------------------------
;;; NREPL-SERVER PROTOCOL
;;; ------------------------------------------------------------------------------------------------------------------------

(defprotocol nrepl-server-proto
  (nrepl-complete [cljsrv code])
  (nrepl-doc [cljsrv sym])
  (nrepl-eval [cljsrv code]
    "Evaluates `code` and returns a result map containing the generated NREPL messages under key
  `:nrepl-messages` and a stacktrace under key `:stacktrace` iff an error occurred during
  evaluation. If Clojure needs input from `stdin` the key `:need-input` is associated with `true` in
  which case `provide-input` must be used to provide the input obtained from the user. `nrepl-eval`
  cannot be called again until the needed input has been provided.")
  (nrepl-continue-eval [cljsrv msgseq]
    "Continues evaluating previously submitted `nrepl-eval` which has been suspended due to need for
    input.  `msgseq` must be a yet-to-be realized seq returned from `nrepl/message` from the
    continuing evaluation proceeds.  Return value is identical to that of `nrepl-eval`.")
  (nrepl-interrupt [cljsrv])
  (nrepl-provide-input [cljsrv input-string]
    "Sends `input-string` to Clojure to satisfy requested need from `stdin`.  Returns `input-string`.")
  (nrepl-server-addr [cljsrv])
  (nrepl-trace [cljsrv]))

;;; ------------------------------------------------------------------------------------------------------------------------
;;; COLJUPYTER NREPL HANDLER
;;; ------------------------------------------------------------------------------------------------------------------------

(defn- clojupyter-nrepl-handler
  []
  ;; dynamically load to allow cider-jack-in to work
  ;; see https://github.com/clojure-emacs/cider-nrepl/issues/447
  (require 'cider.nrepl)
  (apply nrepl.server/default-handler
         (map resolve
              (concat (var-get (ns-resolve 'cider.nrepl 'cider-middleware))
                      `[mw/mime-values]))))

;;; ------------------------------------------------------------------------------------------------------------------------
;;; MESSAGE PREDICATES
;;; ------------------------------------------------------------------------------------------------------------------------

(defn final-message?
  [m]
  (boolean (some #{"interrupted" "done" "error"} (:status m))))

(defn need-input-message?
  [m]
  (contains? (into #{} (:status m)) "need-input"))

(defn exception-message?
  [m]
  (boolean (and (map? m) (find m :ex))))

;;; ------------------------------------------------------------------------------------------------------------------------
;;; CLJSRV
;;; ------------------------------------------------------------------------------------------------------------------------

(defn- fmt
  [cljsrv]
  (str "#NREPL[tcp:/" (nrepl-server-addr cljsrv) "]"))

(defn messages-result
  "Build eval result by reducing the lazy sequence `msgs`.  Care must be taken to avoid reading past
  the final message from the NREPL server as the read will then block indefinitely.  Message reading
  stops either when we get to the final message of the repl (`:status` one of \"done\", \"error\" or
  \"interrupted\") or the NREPL server indicates it needs input from the user (`:status` equals
  \"need-input\")."
  [get-stacktrace-fn pending-input? msg-seq]
  (loop [ms msg-seq, msgs [], result {:need-input false}, iter 10]
    (if (seq ms)
      (let [msg (first ms)
            msgs' (conj msgs msg)]
        (cond
          (final-message? msg)
          ,, (assoc result :nrepl-messages msgs')
          (need-input-message? msg)
          ;; use `delay` to prevent accidental inspection or printing blocking on nrepl message stream:
          ,, (do
               (reset! pending-input? true)
               (assoc result :need-input true :delayed-msgseq (delay (next ms)) :nrepl-messages msgs))
          (exception-message? msg)
          ,, (recur (rest ms) msgs' (assoc result :trace-result (get-stacktrace-fn)) (dec iter))
          :else
          ,, (recur (rest ms) msgs' result (dec iter))))
      (throw (ex-info (str "messages-result - internal error: we should not get to end of nrepl stream without 'done' msg")
                      {:msgs msgs, :result result})))))


(defrecord CljSrv [nrepl-server_ nrepl-client_ nrepl-sockaddr_ pending-input?_]
  nrepl-server-proto

  (nrepl-complete
    [_ code]
    (->> (nrepl/message nrepl-client_ {:op "complete" :symbol code})
         nrepl/combine-responses
         :completions
         (mapv :candidate)))

  (nrepl-continue-eval
    [cljsrv msgseq]
    (assert (not @pending-input?_))
    (messages-result #(nrepl-trace cljsrv) pending-input?_ msgseq))

  (nrepl-doc
    [_ sym]
    (let [code (format "(clojure.core/with-out-str (clojure.repl/doc %s))" sym)]
      (apply str (-> nrepl-client_
                     (nrepl/message {:op "eval", :code code})
                     nrepl/response-values))))

  (nrepl-eval
    [cljsrv code]
    (->> {:id (u!/uuid), :op "eval", :code code :eval 'clojupyter.misc.kind/kind-eval}
         (nrepl/message nrepl-client_)
         (nrepl-continue-eval cljsrv)))

  (nrepl-provide-input
    [cljsrv input-string]
    (assert @pending-input?_)
    (nrepl/message nrepl-client_ {:op "stdin" :stdin (str input-string \newline)})
    (reset! pending-input?_ false)
    input-string)

  (nrepl-interrupt
    [_]
    (-> nrepl-client_
        (nrepl/message {:op :interrupt})
        nrepl/combine-responses
        doall))

  (nrepl-server-addr
    [_]
    nrepl-sockaddr_)

  (nrepl-trace
    [{:keys [nrepl-client_]}]
    (-> nrepl-client_
        (nrepl/message {:op :stacktrace})
        nrepl/combine-responses
        doall))

  Object
  (toString
    [this]
    (fmt this))

  java.io.Closeable
  (close
    [_]
    (nrepl.server/stop-server nrepl-server_)))

(defn cljsrv?
  [v]
  (instance? CljSrv v))

(u!/set-var-private! #'->CljSrv)

(u/define-simple-record-print CljSrv fmt)

(defn make-cljsrv ^CljSrv
  []
  (let [nrepl-server        (nrepl.server/start-server :handler (clojupyter-nrepl-handler))
        nrepl-transport     (nrepl/connect :port (:port nrepl-server))
        nrepl-base-client   (nrepl/client nrepl-transport Integer/MAX_VALUE)
        nrepl-client        (nrepl/client-session nrepl-base-client)
        nrepl-sockaddr      (.getLocalSocketAddress ^java.net.ServerSocket (:server-socket nrepl-server))
        cljsrv          (->CljSrv nrepl-server nrepl-client nrepl-sockaddr (atom false))]
    (println (str "Started NREPL server: tcp:/" nrepl-sockaddr "."))
    cljsrv))
