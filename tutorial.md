MongoDB Haskell Mini Tutorial
-----------------------------

  __Author:__ Brian Gianforcaro (b.gianfo@gmail.com)

  __Updated:__ 2/28/2010

This is a mini tutorial to get you up and going with the basics
of the Haskell mongoDB drivers. It is modeled after the python tutorial
pymongo available here: http://api.mongodb.org/python/1.4%2B/tutorial.html

You will need the mongoDB bindings installed as well as mongo itself installed.

>$ = command line prompt
> = ghci repl prompt


Installing Haskell Bindings
---------------------------

From Source:

> $ git clone git://github.com/srp/mongoDB.git

> $ cd mongoDB

> $ runhaskell Setup.hs configure

> $ runhaskell Setup.hs build

> $ runhaskell Setup.hs install

From Hackage using cabal:

> $ cabal install mongoDB


Getting Ready
-------------

Start a MongoDB instance for us to play with:

> $ mongod

Start up a haskell repl:

> $ ghci

Now We'll need to bring in the MongoDB/BSON bindings:

> import Database.MongoDB

> import Database.MongoDB.BSON

Making A Connection
-------------------
Open up a connection to your DB instance, using the standard port:

> con <- connect "127.0.0.1" []

or for a non-standard port

> import Network
> con <- connectOnPort "127.0.0.1" (Network.PortNumber 666) []

By default mongoDB will try to find the master and connect to it and
will throw an exception if a master can not be found to connect
to. You can force mongoDB to connect to the slave by adding SlaveOK as
a connection option, eg:

> con <- connect "127.0.0.1" [SlaveOK]

Getting the Databases
------------------

> dbs <- databaseNames con
> let testdb = head dbs


Getting the Collections
-----------------------

> collections <- collectionNames con testdb
> let testcol = head collections

Documents
---------

BSON representation in Haskell

Inserting a Document
-------------------

> insert con testcol (toBsonDoc [("author", toBson "Mike"), ("text", toBson "My first Blog post!"), ("tags", toBson ["mongodb", "python","pymongo"])])


Getting a single document with findOne
-------------------------------------

> findOne con curcol (toBsonDoc [("author", toBson "Mike")])

Querying for More Than One Document
------------------------------------

> cursor <- find con curcol (toBsonDoc [("author", toBson "Mike")])

> allDocs cursor

 See nextDoc to modify cursor incrementally one at a time. 

 * Note: allDocs automatically closes the cursor when done, through nextDoc.


Counting
--------

  We can count how many documents are in an entire collection:

> num <- count con testcol

  Or we can query for how many documents match a query:

> num <- countMatching con  testcol (toBsonDoc [("author", toBson "Mike")])

Range Queries
-------------

  No non native sorting yet.

Indexing
--------

 WIP - coming soon.

 Something like...
> index <- createIndex con testcol [("author", Ascending)] True
