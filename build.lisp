(require :asdf)

;; Load genenetwork.asd from the current directory.
(asdf:load-asd (merge-pathnames "dump.asd"
                                (directory-namestring *load-truename*)))

(asdf:make :dump)
