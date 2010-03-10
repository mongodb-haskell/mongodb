MongoDB Haskell Mini Tutorial
-----------------------------

  __Author:__ Brian Gianforcaro (b.gianfo@gmail.com)

  __Updated:__ 2/28/2010

This is a mini tutorial to get you up and going with the basics
of the Haskell mongoDB drivers. It is modeled after the
[pymongo tutorial](http://api.mongodb.org/python/1.4%2B/tutorial.html).

You will need the mongoDB bindings installed as well as mongo itself installed.

    $ = command line prompt
    > = ghci repl prompt


Installing Haskell Bindings
---------------------------

From Source:

    $ git clone git://github.com/srp/mongoDB.git
    $ cd mongoDB
    $ runhaskell Setup.hs configure
    $ runhaskell Setup.hs build
    $ runhaskell Setup.hs install

From Hackage using cabal:

    $ cabal install mongoDB

Getting Ready
-------------

Start a MongoDB instance for us to play with:

    $ mongod

Start up a haskell repl:

    $ ghci

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

Databases, Collections and FullCollections
------------------------------------------

As many database servers, MongoDB has databases--separate namespaces
under which collections reside. Most of the APIs for this driver
request the *FullCollection* which is simply the *Database* and the
*Collection* concatenated with a period.

For instance 'myweb_prod.users' is the the *FullCollection* name for
the *Collection 'users' in the database 'myweb_prod'.

Databases and collections do not need to be created, just start using
them and MongoDB will automatically create them for you.

In the below examples we'll be using the following *FullCollection*:

    > import Data.ByteString.Lazy.UTF8
    > let testcol = (fromString "test.haskell")

You can obtain a list of databases available on a connection:

    > dbs <- databaseNames con

You can obtain a list of collections available on a database:

    > collections <- collectionNames con (fromString "test")

Documents
---------

Data in MongoDB is represented (and stored) using JSON-style
documents. In mongoDB we use the *BsonDoc* type to represent these
documents. At the moment a *BsonDoc* is simply a tuple list of the
type '[(ByteString, BsonValue)]'. Here's a BsonDoc which could represent
a blog post:

    > import Data.Time.Clock.POSIX
    > now <- getPOSIXTime
    > :{
      let post = [(fromString "author", BsonString $ fromString "Mike"),
                  (fromString "text",
                   BsonString $ fromString "My first blog post!"),
                  (fromString "tags",
                   BsonArray [BsonString $ fromString "mongodb",
                              BsonString $ fromString "python",
                              BsonString $ fromString "pymongo"]),
                  (fromString "date", BsonDate now)]
      :}

With all the type wrappers and string conversion, it's hard to see
what's actually going on. Fortunately the BSON library provides
conversion functions *toBson* and *fromBson* for converting native
between the wrapped BSON types and many native Haskell types. The
functions *toBsonDoc* and *fromBsonDoc* help convert from tuple lists
with plain *String* keys, or *Data.Map*.

Here's the same BSON data structure using these conversion functions:

    > :{
      let post = toBsonDoc [("author", toBson "Mike"),
                            ("text", toBson "My first blog post!"),
                            ("tags", toBson ["mongoDB", "Haskell"]),
                            ("date", BsonDate now)]
      :}

Inserting a Document
-------------------

    > insert con testcol post


Getting a single document with findOne
-------------------------------------

    > findOne con testcol (toBsonDoc [("author", toBson "Mike")])

Querying for More Than One Document
------------------------------------

    > cursor <- find con testcol (toBsonDoc [("author", toBson "Mike")])
    > allDocs cursor

You can combine these into one line:

    > docs <- allDocs =<< find con testcol (toBsonDoc [("author", toBson "Mike")])

See nextDoc to modify cursor incrementally one at a time.

 * Note: allDocs automatically closes the cursor when done, through nextDoc.


Counting
--------

We can count how many documents are in an entire collection:

    > num <- count con testcol

Or we can query for how many documents match a query:

    > num <- countMatching con testcol (toBsonDoc [("author", toBson "Mike")])

Range Queries
-------------

No non native sorting yet.

Indexing
--------

WIP - coming soon.

Something like...

    > index <- createIndex con testcol [("author", Ascending)] True
