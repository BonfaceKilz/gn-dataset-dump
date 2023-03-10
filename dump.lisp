(defpackage :dump
  (:use :common-lisp :cl-json)
  (:import-from :alexandria :once-only :iota
   :plist-alist :with-gensyms)
  (:import-from :listopia :all :any :split-at)
  (:import-from :ironclad :with-octet-input-stream :with-octet-output-stream
   :with-digesting-stream :digest-length)
  (:import-from :str
   :concat :contains? :join :s-rest :split :starts-with?
   :trim-right :words)
  (:import-from :arrows :-> :->>)
  (:import-from :trivia :lambda-match :match)
  (:import-from :trivial-utf-8 :string-to-utf-8-bytes :write-utf-8-bytes :utf-8-bytes-to-string)
  (:import-from :lmdb :with-env :*env* :get-db :with-txn :put :g3t :uint64-to-octets
		:with-cursor :cursor-first :do-cursor :cursor-del :octets-to-uint64
   :db-statistics)
  (:export
   :main))

(in-package :dump)


;; ENV SETTINGS
(defvar *blob-hash-digest*
  :sha256)


;; Some helper functions
(defun assoc-ref (alist key &key (test #'equalp))
  "Given an association list ALIST, return the value associated with
KEY."
  (match (assoc key alist :test test)
    ((cons _ value) value)))

(defmacro with-sampledata-db ((db database-directory &key write) &body body)
  "Create a new LMDB database in DATABASE-DIRECTORY and execute BODY
with a transaction open on DB."
  (with-gensyms (env)
    (once-only (database-directory write)
      `(with-env (,env ,database-directory
		       :if-does-not-exist :create
		       :map-size (* 100 1024 1024))
	 (let ((,db (get-db nil :env ,env)))
	   (with-txn (:env ,env :write ,write)
	     ,@body))))))


;; Hash functions and operations on bytevectors
(defun metadata-key (hash key)
  "Return the database key to retrieve metadata KEY associated with blob
of HASH."
  (concatenate '(vector (unsigned-byte 8))
	       hash
	       (string-to-utf-8-bytes (concat ":" key))))

(defun write-bytevector-with-length (bv stream)
  "Write length of BV followed by BV itself to STREAM. The length is
written as a little endian 64-bit unsigned integer."
  (write-sequence (-> bv
		      length
		      uint64-to-octets)
		  stream)
  ;; Accomodate strings and floats by encoding to json
  (write-sequence (-> bv
		      json:encode-json-to-string
		      string-to-utf-8-bytes)
		  stream))


(defun hash-vector-length (hash-vector)
  "Return the number of hashes in HASH-VECTOR."
  (/ (length hash-vector)
     (digest-length *blob-hash-digest*)))

(defun bv-hash (bv &optional metadata)
  "Return the hash of a bytevector BV and optionally write a HEADER to
the hash stream"
  (with-digesting-stream (stream *blob-hash-digest*)
    ;; Write bytevector
    (write-bytevector-with-length bv stream)
    ;; Write metadata
    (mapc (lambda-match
	    ((cons key value)
	     (write-bytevector-with-length (string-to-utf-8-bytes key)
					   stream)
	     (write-bytevector-with-length
	      (etypecase value
		(string (string-to-utf-8-bytes value))
		((unsigned-byte 64) (uint64-to-octets value))
		((vector (unsigned-byte 8)) value))
	      stream)))
	  metadata)))

(defun hash-vector-ref (hash-vector n)
  "Return the Nth hash in HASH-VECTOR."
  (let ((hash-length (digest-length *blob-hash-digest*)))
    (make-array hash-length
		:element-type '(unsigned-byte 8)
		:displaced-to hash-vector
		:displaced-index-offset (* n hash-length))))


;; Matrix Data Structures and associated helper functions

(defstruct sampledata matrix metadata)

(defstruct sampledata-db-matrix
  db hash nrows ncols row-pointers column-pointers array transpose)

(defun array-to-list (array)
  "Convert ARRAY into a LIST."
  (let* ((dimensions (array-dimensions array))
         (depth (1- (length dimensions)))
         (indices (make-list (1+ depth) :initial-element 0)))
    (labels ((recurse (n)
               (loop for j below (nth n dimensions)
                     do (setf (nth n indices) j)
                     collect (if (= n depth)
                                 (apply #'aref array indices)
				 (recurse (1+ n))))))
      (recurse 0))))

(defun list-dimensions (list depth)
  "Return the array dimensions of a LIST given the LIST's DEPTH."
  (loop repeat depth
        collect (length list)
        do (setf list (car list))))

(defun list-to-array (list depth)
  "Convert a LIST into an ARRAY given the lists DEPTH."
  (make-array (list-dimensions list depth)
              :initial-contents list))

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


;; Working with sample data matrixes

(defun sampledata-db-get (db key)
  "Get sampledata with KEY from DB.  KEY may be a hash or a string.  If
it is a string, it is encoded into octets before querying the
database."
  (g3t db (if (stringp key)
	      (string-to-utf-8-bytes key)
	      key)))

(defun sampledata-db-put (db data &optional metadata)
  "Put DATA into DB.  Associate HEADER, representing the name of the
columns, with DATA.  Return the hash."
  (let ((hash (bv-hash data metadata)))
    (unless (sampledata-db-get db hash)
      (put db hash data)
      (mapc (lambda-match
	      ((cons key value)
	       (put db (metadata-key hash key) value)))
	    metadata))
    hash))

(defun sampledata-db-metadata-get (db hash key)
  "Get metadata associated with KEY, HASH from sampledata DB."
  (sampledata-db-get db (metadata-key hash key)))

(defun sampledata-db-current-matrix-hash (db)
  "Return the hash of the current matrix in the sampledata matrix DB."
  (hash-vector-ref (sampledata-db-get db "versions")
		   0))

(defun sampledata-db-matrix (db hash)
  "Return the matrix identified by HASH from sampledata matrix DB."
  (let ((nrows (octets-to-uint64 (sampledata-db-metadata-get db hash "nrows")))
	(ncols (octets-to-uint64 (sampledata-db-metadata-get db hash "ncols")))
	(hash-length (digest-length *blob-hash-digest*)))
    (make-sampledata-db-matrix
     :db db
     :hash hash
     :nrows nrows
     :ncols ncols
     :row-pointers (make-array (* nrows hash-length)
			       :element-type '(unsigned-byte 8)
			       :displaced-to (sampledata-db-get db hash))
     :column-pointers (make-array (* ncols hash-length)
				  :element-type '(unsigned-byte 8)
				  :displaced-to (sampledata-db-get db hash)
				  :displaced-index-offset (* nrows hash-length)))))

(defun sampledata-db-matrix-put (db db-matrix)
  "Put sampledata MATRIX into DB and return the hash"
  (let* ((hash-length (digest-length *blob-hash-digest*))
	 (matrix (sampledata-matrix db-matrix))
	 (metadata (->> (sampledata-metadata db-matrix)
			json:encode-json-alist-to-string)))
    (match (array-dimensions matrix)
      ((list nrows ncols)
       (let* ((data
		(with-octet-output-stream (stream)
		  (dotimes (i nrows)
		    (write-sequence
		     (sampledata-db-put
		      db (->> i
			      (matrix-row matrix)
			      json:encode-json-to-string
			      string-to-utf-8-bytes))
		     stream))
		  (dotimes (j ncols)
		    (write-sequence
		     (sampledata-db-put
		      db (->> j
			      (matrix-column matrix)
			      json:encode-json-to-string
			      string-to-utf-8-bytes))
		     stream))))
	      (row-pointers (make-array (* nrows hash-length)
					:element-type '(unsigned-byte 8)
					:displaced-to data))
	      (column-pointers (make-array (* ncols hash-length)
					   :element-type '(unsigned-byte 8)
					   :displaced-to data
					   :displaced-index-offset (* nrows hash-length))))
	 (sampledata-db-put
	  db
	  data
	  `(("nrows" . ,nrows)
	    ("ncols" . ,ncols)
	    ("metadata" . ,metadata)
	    ("row-pointers" . ,row-pointers)
	    ("column-pointers" . ,column-pointers))))))))

(defun (setf sampledata-db-current-matrix-hash) (hash db)
  "Set HASH as the current matrix in the sampledata matrix DB."
  ;; Prepend hash into versions array.
  (put db (string-to-utf-8-bytes "versions")
       (concatenate '(vector (unsigned-byte 8))
		    hash
		    (sampledata-db-get db "versions")))
  ;; Write a read-optimized copy of the current matrix into the database
  (let ((matrix (sampledata-db-matrix db hash)))
    (put db
	 (string-to-utf-8-bytes "current")
	 (sampledata-db-put
	  db
	  (with-octet-output-stream (stream)
	    (dotimes (i (sampledata-db-matrix-nrows matrix))
	      (write-sequence
	       (->> i
		    (sampledata-db-matrix-row-ref matrix)
		    json:encode-json-to-string
		    string-to-utf-8-bytes)
	       stream))
	    (dotimes (j (sampledata-db-matrix-ncols matrix))
	      (write-sequence
	       (->> j
		    (sampledata-db-matrix-column-ref matrix)
		    json:encode-json-to-string
		    string-to-utf-8-bytes)
	       stream)))
	  `(("matrix" . ,hash))))))

(defun sampledata-db-current-matrix (db)
  "Return the latest version of the matrix in DB."
  (let* ((current-matrix-hash (sampledata-db-current-matrix-hash db))
	 (nrows (octets-to-uint64 (sampledata-db-metadata-get db current-matrix-hash "nrows")))
	 (ncols (octets-to-uint64 (sampledata-db-metadata-get db current-matrix-hash "ncols"))))
    (make-sampledata-db-matrix
     :db db
     :nrows nrows
     :ncols ncols
     :array
     (make-array
      (list nrows ncols)
      :initial-contents (loop for i from 0 to (- nrows 1)
			      collect (sampledata-db-matrix-row-ref
				       (sampledata-db-matrix db current-matrix-hash) i)))
     :transpose
     (make-array
      (list ncols nrows)
      :initial-contents (loop for i from 0 to (- ncols 1)
			      collect (sampledata-db-matrix-column-ref
				       (sampledata-db-matrix db current-matrix-hash) i))))))

(defun sampledata-db-all-matrices (db)
  "Return a list of all matrices in DB, newest first."
  (let ((all-matrix-hashes (sampledata-db-get db "versions")))
    (mapcar (lambda (i)
	      (sampledata-db-matrix db (hash-vector-ref all-matrix-hashes i)))
	    (iota (hash-vector-length all-matrix-hashes)))))

(defun sampledata-db-current-matrix-ref (matrix)
  "Return MATRIX as a 2-D array."
  (let ((array (sampledata-db-matrix-array matrix)))
    (if array
	array
	(let* ((nrows (sampledata-db-matrix-nrows matrix))
	       (ncols (sampledata-db-matrix-ncols matrix))
	       (array (make-array (list nrows ncols)
				  :element-type '(unsigned-byte 8))))
	  (dotimes (i nrows)
	    (let ((row (sampledata-db-matrix-row-ref matrix i)))
	      (dotimes (j ncols)
		(setf (aref array i j)
		      (aref row j)))))
	  array))))

(defun sampledata-db-matrix-row-ref (matrix i)
  "Return the Ith row of sampledata db MATRIX."
  (let ((db (sampledata-db-matrix-db matrix))
	(array (sampledata-db-matrix-array matrix)))
    (coerce (if array
		(matrix-row array i)
		(json:decode-json-from-string
		 (utf-8-bytes-to-string
		  (sampledata-db-get
		   db
		   (hash-vector-ref (sampledata-db-matrix-row-pointers matrix) i)))))
	    'vector)))

(defun sampledata-db-matrix-column-ref (matrix j)
  "Return the Jth row of sampledata db MATRIX."
  (let ((db (sampledata-db-matrix-db matrix))
	(transpose (sampledata-db-matrix-array matrix)))
    (coerce (if transpose
		(matrix-row transpose j)
		(json:decode-json-from-string
		 (utf-8-bytes-to-string
		  (sampledata-db-get
		   db
		   (hash-vector-ref (sampledata-db-matrix-column-pointers matrix) j)))))
	    'vector)))

(defun json-file->sampledata (file)
  "Convert FILE to SAMPLEDATA."
  (let* ((json-data (json:decode-json-from-source
		     (pathname file)))
	 (headers (assoc-ref json-data :headers))
	 (matrix (assoc-ref json-data :data))
	 (nrows (length matrix))
	 (ncols (length (first matrix))))
    (make-sampledata
     :matrix
     (make-array (list nrows ncols)
		 :initial-contents matrix)
     :metadata `(("header" . ,headers)))))

(defun collect-garbage (db)
  "Delete all keys in DB that are not associated with a live hash."
  (with-cursor (cursor db)
    (cursor-first cursor)
    (do-cursor (key value cursor)
      (unless (live-key-p db key)
        (cursor-del cursor)))))

(defun find-index (function n)
  "Return the index between 0 and n-1 (both inclusive) for which
FUNCTION returns non-nil. If no such index exists, return
nil. FUNCTION is invoked as (FUNCTION INDEX). The order of invocation
of FUNCTION is unspecified."
  (unless (zerop n)
    (if (funcall function (1- n))
        (1- n)
        (find-index function (1- n)))))

(defun for-each-indexed (function list &optional (start 0))
  "Apply FUNCTION successively on every element of LIST.  FUNCTION is
invoked as (FUNCTION INDEX ELEMENT) where ELEMENT is an element of
LIST and INDEX is its index.  START is the index to use for the first
element."
  (match list
    ((list* head tail)
     (funcall function start head)
     (for-each-indexed function tail (1+ start)))))
 
(defun hash-in-hash-vector-p (hash hash-vector)
  "Return non-nil if HASH is in HASH-VECTOR. Else, return nil."
  (find-index (lambda (i)
                (equalp (hash-vector-ref hash-vector i)
                        hash))
              (hash-vector-length hash-vector)))

(defun live-key-p (db key)
  "Return non-nil if KEY is live. Else, return nil."
  (or (equalp key (string-to-utf-8-bytes "current"))
      (equalp key (string-to-utf-8-bytes "versions"))
      (equalp key (sampledata-db-get db "current"))
      (let ((versions-hash-vector (sampledata-db-get db "versions"))
	    (key-hash-prefix (make-array (digest-length *blob-hash-digest*)
					 :element-type '(unsigned-byte 8)
					 :displaced-to key)))
	(or (hash-in-hash-vector-p key-hash-prefix versions-hash-vector)
	    (find-index (lambda (i)
			  (hash-in-hash-vector-p
			   key-hash-prefix
			   (sampledata-db-get db (hash-vector-ref versions-hash-vector i))))
			(hash-vector-length versions-hash-vector))))))

(defun import-into-sampledata-db (data db-path)
  "Import MATRIX which is a sampledata-matrix object into
DB-PATH."
  (with-sampledata-db (db db-path :write t)
    (let* ((hash (sampledata-db-matrix-put db data))
	   (db-matrix (sampledata-db-matrix db hash)))
      ;; Read written data back and verify.
      (unless (and (all (lambda (i)
			  (equalp (matrix-row (sampledata-matrix data) i)
				  (sampledata-db-matrix-row-ref db-matrix i)))
			(iota (sampledata-db-matrix-nrows db-matrix)))
		   (all (lambda (i)
			  (equalp (matrix-column (sampledata-matrix data) i)
				  (sampledata-db-matrix-column-ref db-matrix i)))
			(iota (sampledata-db-matrix-ncols db-matrix))))
	;; Roll back database updates.
	(collect-garbage db)
	;; Exit with error message.
	(format *error-output*
		"Rereading and verifying sampledata matrix written to \"~a\" failed.
This is a bug. Please report it.
"
		db-path)
	(uiop:quit 1))
      ;; Set the current matrix.
      (setf (sampledata-db-current-matrix-hash db)
	    hash))))

(defun print-sampledata-db-info (database-directory)
  (with-sampledata-db (db database-directory)
    (format t
	    "Path: ~a~%Versions: ~a~%Keys: ~a~%~%"
	    database-directory
	    (length (sampledata-db-all-matrices db))
	    (getf (db-statistics db)
		  :entries))
    (for-each-indexed (lambda (i matrix)
			(format t "Version ~a
Dimensions: ~a x ~a~%"
				(1+ i)
				(sampledata-db-matrix-nrows matrix)
				(sampledata-db-matrix-ncols matrix)))
		      (sampledata-db-all-matrices db))))



(defun main ()
  (match (uiop:command-line-arguments)
    ((list "import" json-data dump-dir)
     (fad:walk-directory
      json-data
      (lambda (el)
	(let* ((dataset-name
		 (-> el pathname-directory last car))
	       (db-path
		 (make-pathname
		  :defaults dump-dir
		  :directory (append (pathname-directory dump-dir)
				     (list dataset-name (pathname-name el))))))
	  (format t "Dumping: ~a~%" db-path)
	  (import-into-sampledata-db
	   (json-file->sampledata (namestring el))
	   db-path)))))
    ((list "info" sampledata-database)
     (print-sampledata-db-info
      (fad:pathname-as-directory sampledata-database)))
    (_ (format t "Usage:

Import JSON-DATA into DUMP-DIR:
    dump import JSON-DATA/ DUMP-DIR/

Print info about SAMPLEDATA-DATABASE:
    dump info SAMPLEDATA-DATABASE
")
       (uiop:quit 1))))
