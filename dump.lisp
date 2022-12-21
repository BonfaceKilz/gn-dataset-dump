(defpackage :dump
  (:use :common-lisp  :lmdb)
  (:import-from :alexandria :once-only
		:plist-alist :with-gensyms)
  (:import-from :ironclad :with-octet-input-stream :with-octet-output-stream
   :with-digesting-stream :digest-length)
  (:import-from :cl-dbi :with-connection :prepare :execute :fetch-all :fetch)
  (:import-from :trivia :lambda-match :match)
  (:import-from :trivial-utf-8 :string-to-utf-8-bytes)
  (:import-from :lmdb :with-env :*env* :get-db :with-txn :put :g3t :uint64-to-octets))

(in-package :dump)


;; ENV SETTINGS

(defvar *connection-settings*
  (with-open-file
      (stream
       ;; TODO get this from the command line
       "~/projects/oqo-dump-genenetwork-database/fix-sql-queries/conn.scm")
    (read stream)))

(defvar *blob-hash-digest*
  :sha256)


;; Some helper functions
(defun assoc-ref (alist key &key (test #'equalp))
  "Given an association list ALIST, return the value associated with
KEY."
  (match (assoc key alist :test test)
    ((cons _ value) value)))

(defun plists->csv (plists)
  "Convert a list of PLISTS to a CSV string, with the keys of the PLISTS
being the first row."
  (let* ((keys (mapcar #'car (plist-alist
			      (car plists)))) ; get the keys from the first plist
	 (headers (format nil "~{~A~^,~}" keys))
	 (rows (mapcar (lambda (it)
			 (format nil "~{~A~^,~}"
				 (loop for (key value) on it
				       by #'cddr
				       collect value)))
		       plists)))
    (format nil "~A~%~{~A~%~}" headers rows)))

(defun fetch-results-from-sql (statement &optional params)
  (with-connection
      (conn :mysql
	    :database-name (assoc-ref *connection-settings* 'sql-database)
	    :host (assoc-ref *connection-settings* 'sql-host)
	    :port (assoc-ref *connection-settings* 'sql-port)
	    :username (assoc-ref *connection-settings* 'sql-username)
	    :password (assoc-ref *connection-settings* 'sql-password))
    (let* ((query (prepare conn statement))
	   (query (execute query params)))
      (fetch-all query))))

(defmacro with-sampledata-db ((db database-directory &key write) &body body)
  "Create a new LMDB database in DATABASE-DIRECTORY and execute BODY
with a transaction open on DB."
  (with-gensyms (env)
    (once-only (database-directory write)
      `(with-env (,env ,database-directory
		       :if-does-not-exist :create
		       :map-size (* 100 1024 1024))
	 (let ((,db (get-db nil :env ,env :value-encoding :utf-8)))
	   (with-txn (:env ,env :write ,write)
	     ,@body))))))


;; Hash functions and operations on bytevectors

(defun write-bytevector-with-length (bv stream)
  (write-sequence (uint64-to-octets (length bv)) stream)
  (write-sequence bv stream))

(defun hash-vector-length (hash-vector)
  "Return the number of hashes in HASH-VECTOR."
  (/ (length hash-vector)
     (digest-length *blob-hash-digest*)))

(defun bv-hash (bv &optional header)
  "Return the hash of a bytevector BV and optionally write a HEADER to
the hash stream"
  (with-digesting-stream (stream *blob-hash-digest*)
    ;; Write bytevector
    (write-bytevector-with-length bv stream)
    ;; Write header
    (lambda-match
      ((cons key value)
       (write-bytevector-with-length (string-to-utf-8-bytes key)
				     stream)
       (write-bytevector-with-length
	(etypecase value
	  (string (string-to-utf-8-bytes value))
	  ((unsigned-byte 64) (uint64-to-octets value))
	  ((vector (unsigned-byte 8)) value))
	stream)))))

(defun hash-vector-ref (hash-vector n)
  "Return the Nth hash in HASH-VECTOR."
  (let ((hash-length (digest-length *blob-hash-digest*)))
    (make-array hash-length
		:element-type '(unsigned-byte 8)
		:displaced-to hash-vector
		:displaced-index-offset (* n hash-length))))


;; Matrix Data Structures and associated helper functions

(defstruct sampledata matrix header)

(defun sampledata-db-get (db key)
  "Get bytevector with KEY from sampledata DB.  KEY may be a hash or a
string.  If it is a string, it is encoded into octets before querying
the database."
  (g3t db (if (stringp key)
	      (string-to-utf-8-bytes key)
	      key)))



(defun sampledata-db-put (db bv header)
  "Put BV - a bytevector - into DB.  Associate HEADER, representing the
name of the columns, with BV.  Return the hash."
  (let ((hash (bv-hash bv header)))
    (unless (sampledata-db-get db hash)
      (put db hash bv)
      ;; FIX THIS!
      (put db "header" header))))

(defun sampledata-db-matrix-put (db matrix)
  "Put sampledata MATRIX into DB and return the hash"
  (let ((matrix (sampledata-matrix matrix)))
    (match (array-dimensions matrix)
      ((list nrows ncols)
       (
	;; TODO
	)))))

(defun matrix-row (matrix n)
  "Return the Nth row of MATRIX."
  (let ((ncols (array-dimension matrix 1)))
    (make-array ncols
		:element-type (array-element-type matrix)
		:displaced-to matrix
		:displaced-index-offset (* n ncols))))


(defun matrix-column (matrix n)
  "Return the Nth column of MATRIX."
  (let ((column (make-array (array-dimension matrix 0))))
    (dotimes (i (length column))
      (setf (aref column i)
	    (aref matrix i n)))
    column))



;; Dumping an Retriewing Data Examples

(with-sampledata-db
    (db "lmdb-test2/" :write t)
  (put db "BXD105:10007" (plists->csv (fetch-results-from-sql
				       "SELECT * FROM
(SELECT DISTINCT st.Name as 'Name', ifnull(pd.value, 'x') as 'Value',
ifnull(ps.error, 'x') as 'SE', ifnull(ns.count, 'x') as 'Count', ps.StrainId as 'StrainId'
FROM PublishFreeze pf JOIN PublishXRef px ON px.InbredSetId = pf.InbredSetId
JOIN PublishData pd ON pd.Id = px.DataId JOIN Strain st ON pd.StrainId = st.Id
LEFT JOIN PublishSE ps ON ps.DataId = pd.Id AND ps.StrainId = pd.StrainId
LEFT JOIN NStrain ns ON ns.DataId = pd.Id AND ns.StrainId = pd.StrainId
WHERE px.PhenotypeId = ? ORDER BY st.Name) A
LEFT JOIN
(SELECT cxref.StrainId as StrainId, group_concat(ca.Name, '=', cxref.Value) as 'CaseAttributes'
FROM CaseAttributeXRefNew cxref LEFT JOIN CaseAttribute ca
ON ca.Id = cxref.CaseAttributeId
GROUP BY InbredSetId, cxref.StrainId) B ON A.StrainId = B.StrainId;"
				       (list 35)))))


(with-sampledata-db
    (db "lmdb-test2/" :write t)
  (print (g3t db "BXD105:10007")))
