(in-package cl-user)

(asdf:defsystem #:dump
  :description "GeneNetwork matrix database tool"
  :version "0.1.0"
  :author "The GeneNetwork team"
  :licence "GNU General Public License version 3 or later"
  :depends-on (:alexandria
	       :arrows
	       :cl-conspack
	       :cl-dbi :trivia
	       :cl-fad
	       :cl-json
	       :ironclad
	       :listopia
	       :lmdb
	       :str
	       :trivial-utf-8)
  :components ((:file "dump"))
  :build-operation "program-op"
  :build-pathname "dump"
  :entry-point "dump:main")

