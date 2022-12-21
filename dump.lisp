(defpackage :dump
  (:use :common-lisp  :lmdb)
  (:import-from :alexandria :plist-alist)
  (:import-from :cl-dbi :with-connection :prepare :execute :fetch-all)
  (:import-from :lmdb :with-env :*env* :get-db :with-txn :put :g3t))

(in-package :dump)


;; ENV SETTINGS

(defvar *connection-settings*
  (with-open-file
      (stream
       ;; TODO get this from the command line
       "~/projects/oqo-dump-genenetwork-database/fix-sql-queries/conn.scm")
    (read stream)))


;; Some helper functions
(defun assoc-ref (key alist)
  "Given an association list ALIST, return the value associated with
KEY."
  (cdr (assoc key alist)))

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
	    :database-name (assoc-ref 'sql-database *connection-settings*)
	    :host (assoc-ref 'sql-host *connection-settings*)
	    :port (assoc-ref 'sql-port *connection-settings*)
	    :username (assoc-ref 'sql-username *connection-settings*)
	    :password (assoc-ref 'sql-password *connection-settings*))
    (let* ((query (prepare conn statement))
	   (query (execute query params)))
      (fetch-all query))))

(defun store-sample-data-in-lmdb (db-name name sample-data)
  (with-env
      (*env* (assoc-ref 'lmdb-path *connection-settings*)
		  :if-does-not-exist :create)
    (let ((db (get-db db-name :value-encoding :utf-8)))
      (with-txn (:write t)
	(put db name sample-data)))))


;; Dumping Data using rdf

;; Simple Example
(let ((result (fetch-results-from-sql
	       "SELECT * FROM
(SELECT DISTINCT st.Name as 'Name', ifnull(pd.value, 'x') as 'Value',
ifnull(ps.error, 'x') as 'SE', ifnull(ns.count, 'x') as 'Count', ps.StrainId as 'StrainId'
FROM PublishFreeze pf JOIN PublishXRef px ON px.InbredSetId = pf.InbredSetId
JOIN PublishData pd ON pd.Id = px.DataId JOIN Strain st ON pd.StrainId = st.Id
LEFT JOIN PublishSE ps ON ps.DataId = pd.Id AND ps.StrainId = pd.StrainId
LEFT JOIN NStrain ns ON ns.DataId = pd.Id AND ns.StrainId = pd.StrainId
WHERE px.PhenotypeId = ? ORDER BY st.Name) A
LEFT JOIN
(SELECT cxref.StrainId as StrainId, group_concat(ca.Name, '=', cxref.Value) as \"CaseAttributes\"
FROM CaseAttributeXRefNew cxref LEFT JOIN CaseAttribute ca
ON ca.Id = cxref.CaseAttributeId
GROUP BY InbredSetId, cxref.StrainId) B ON A.StrainId = B.StrainId;"
	       ;; TODO: Fetch this from RDF
	       (list 35))))
  (if result
      (store-sample-data-in-lmdb "sample-data-collection"
				 "BXD101" (plists->csv result))))
