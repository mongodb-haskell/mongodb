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
    > let postsCol = (fromString "test.posts")

You can obtain a list of databases available on a connection:

    > dbs <- databaseNames con

You can obtain a list of collections available on a database:

    > cols <- collectionNames con (fromString "test")
    > map toString cols
    ["test.system.indexes"]

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

To insert a document into a collection we can use the *insert* function:

    > insert con postsCol post
    BsonObjectId 23400392795601893065744187392

When a document is inserted a special key, *_id*, is automatically
added if the document doesn't already contain an *_id* key. The value
of *_id* must be unique across the collection. *insert* returns the
value of *_id* for the inserted document. For more information, see
the [documentation on _id](http://www.mongodb.org/display/DOCS/Object+IDs).

After inserting the first document, the posts collection has actually
been created on the server. We can verify this by listing all of the
collections in our database:

    > cols <- collectionNames con (fromString "test")
    > map toString cols
    [u'postsCol', u'system.indexes']

* Note The system.indexes collection is a special internal collection
that was created automatically.

Getting a single document with findOne
-------------------------------------

The most basic type of query that can be performed in MongoDB is
*findOne*. This method returns a single document matching a query (or
*Nothing* if there are no matches). It is useful when you know there is
only one matching document, or are only interested in the first
match. Here we use *findOne* to get the first document from the posts
collection:

    > findOne con postsCol []
    Just [(Chunk "_id" Empty,BsonObjectId (Chunk "K\151\153S9\CAN\138e\203X\182'" Empty)),(Chunk "author" Empty,BsonString (Chunk "Mike" Empty)),(Chunk "text" Empty,BsonString (Chunk "My first blog post!" Empty)),(Chunk "tags" Empty,BsonArray [BsonString (Chunk "mongoDB" Empty),BsonString (Chunk "Haskell" Empty)]),(Chunk "date" Empty,BsonDate 1268226361.753s)]

The result is a dictionary matching the one that we inserted
previously.

* Note: The returned document contains an *_id*, which was automatically
added on insert.

*findOne* also supports querying on specific elements that the
resulting document must match. To limit our results to a document with
author "Mike" we do:

    > findOne con postsCol $ toBsonDoc [("author", toBson "Mike")]
    Just [(Chunk "_id" Empty,BsonObjectId (Chunk "K\151\153S9\CAN\138e\203X\182'" Empty)),(Chunk "author" Empty,BsonString (Chunk "Mike" Empty)),(Chunk "text" Empty,BsonString (Chunk "My first blog post!" Empty)),(Chunk "tags" Empty,BsonArray [BsonString (Chunk "mongoDB" Empty),BsonString (Chunk "Haskell" Empty)]),(Chunk "date" Empty,BsonDate 1268226361.753s)]

If we try with a different author, like "Eliot", we'll get no result:

    > findOne con postsCol $ toBsonDoc [("author", toBson "Eliot")]
    Nothing

Bulk Inserts
------------

In order to make querying a little more interesting, let's insert a
few more documents. In addition to inserting a single document, we can
also perform bulk insert operations, by using the *insertMany* api
which accepts a list of documents to be inserted. This will insert
each document in the iterable, sending only a single command to the
server:

    > now <- getPOSIXTime
    > :{
      let new_postsCol = [toBsonDoc [("author", toBson "Mike"),
                                     ("text", toBson "Another post!"),
                                     ("tags", toBson ["bulk", "insert"]),
                                     ("date",  toBson now)],
                          toBsonDoc [("author", toBson "Eliot"),
                                     ("title", toBson "MongoDB is fun"),
                                     ("text", toBson "and pretty easy too!"),
                                     ("date", toBson now)]]
      :}
    > insertMany con postsCol new_posts
    [BsonObjectId 23400393883959793414607732737,BsonObjectId 23400398126710930368559579137]

* Note that *new_posts !! 1* has a different shape than the other
posts - there is no "tags" field and we've added a new field,
"title". This is what we mean when we say that MongoDB is schema-free.

Querying for More Than One Document
------------------------------------

To get more than a single document as the result of a query we use the
*find* method. *find* returns a cursor instance, which allows us to
iterate over all matching documents. There are several ways in which
we can iterate: we can call *nextDoc* to get documents one at a time
or we can get a lazy list of all the results by applying the cursor
to *allDocs*:

    > cursor <- find con postsCol $ toBsonDoc [("author", toBson "Mike")]
    > allDocs cursor

Of course you can use bind (*>>=*) to combine these into one line:

    > docs <- find con postsCol (toBsonDoc [("author", toBson "Mike")]) >>= allDocs

* Note: *nextDoc* automatically closes the cursor when the last
document has been read out of it. Similarly, *allDocs* automatically
closes the cursor when you've consumed to the end of the resulting
list.

Counting
--------

We can count how many documents are in an entire collection:

    > num <- count con postsCol

Or we can query for how many documents match a query:

    > num <- countMatching con postsCol (toBsonDoc [("author", toBson "Mike")])

Range Queries
-------------

No non native sorting yet.

Indexing
--------

WIP - coming soon.

Something like...

    > index <- createIndex con testcol [("author", Ascending)] True
