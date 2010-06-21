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

Now we'll need to bring in the MongoDB/BSON bindings and set
OverloadedStrings so literal strings are converted to UTF-8 automatically.

    > import Database.MongoDB
    > :set -XOverloadedStrings

Making A Connection
-------------------
Open up a connection to your DB instance, using the standard port:

    > Right con <- connect $ server "127.0.0.1"

or for a non-standard port

    > Right con <- connect $ server "127.0.0.1" (PortNumber 666)

*connect* returns Left IOError if connection fails. We are assuming above
it won't fail. If it does you will get a pattern match error.

Task and Db monad
-------------------

The current connection is held in a Connected monad, and the current database
is held in a Reader monad on top of that. To run a connected monad, supply
it and a connection to *runConn*. To access a database within a connected
monad, call *useDb*.

Since we are working in ghci, which requires us to start from the
IO monad every time, we'll define a convenient *run* function that takes a
db-action and executes it against our "test" database on the server we
just connected to:

    > let run act = runConn (useDb "test" act) con

*run* (*runConn*) will return either Left Failure or Right result. Failure
means the connection failed (eg. network problem) or the server failed
(eg. disk full).

Databases and Collections
-----------------------------

A MongoDB can store multiple databases -- separate namespaces
under which collections reside.

You can obtain the list of databases available on a connection:

    > runConn allDatabases con

You can also use the *run* function we just created:

    > run allDatabases

The "test" database is ignored in this case because *allDatabases*
is not a query on a specific database but on the server as a whole.

Databases and collections do not need to be created, just start using
them and MongoDB will automatically create them for you.

In the below examples we'll be using the database "test" (captured in *run*
above) and the colllection "posts":

You can obtain a list of collections available in the "test" database:

    > run allCollections

Documents
---------

Data in MongoDB is represented (and stored) using JSON-style
documents. In mongoDB we use the BSON *Document* type to represent
these documents. A document is simply a list of *Field*s, where each field is
a named value. A value is a basic type like Bool, Int, Float, String, Time;
a special BSON value like Binary, Javascript, ObjectId; a (embedded)
Document; or a list of values. Here's an example document which could
represent a blog post:

    > import Data.Time
    > now <- getCurrentTime
    > :{
      let post = ["author" =: "Mike",
                  "text" =: "My first blog post!",
                  "tags" =: ["mongoDB", "Haskell"],
                  "date" =: now]
      :}

Inserting a Document
-------------------

To insert a document into a collection we can use the *insert* function:

    > run $ insert "posts" post
    Right (Oid 4c16d355 c80c560858000000)

When a document is inserted a special field, *_id*, is automatically
added if the document doesn't already contain that field. The value
of *_id* must be unique across the collection. *insert* returns the
value of *_id* for the inserted document. For more information, see
the [documentation on _id](http://www.mongodb.org/display/DOCS/Object+IDs).

After inserting the first document, the posts collection has actually
been created on the server. We can verify this by listing all of the
collections in our database:

    > run allCollections

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

    > run $ findOne (select [] "posts")
    Right (Just [ _id: Oid 4c16d355 c80c560858000000, author: "Mike", text: "My first blog post!", tags: ["mongoDB","Haskell"], date: 2010-06-15 01:09:28.364 UTC])

The result is a document matching the one that we inserted previously.

* Note: The returned document contains an *_id*, which was automatically
added on insert.

*findOne* also supports querying on specific elements that the
resulting document must match. To limit our results to a document with
author "Mike" we do:

    > run $ findOne (select ["author" =: "Mike"] "posts")
    Right (Just [ _id: Oid 4c16d355 c80c560858000000, author: "Mike", text: "My first blog post!", tags: ["mongoDB","Haskell"], date: 2010-06-15 01:09:28.364 UTC])

If we try with a different author, like "Eliot", we'll get no result:

    > run $ findOne (select ["author" =: "Eliot"] "posts")
    Right Nothing

Bulk Inserts
------------

In order to make querying a little more interesting, let's insert a
few more documents. In addition to inserting a single document, we can
also perform bulk insert operations, by using the *insertMany* function
which accepts a list of documents to be inserted. It send only a single
command to the server:

    > now <- getCurrentTime
    > :{
      let post1 = ["author" =: "Mike",
                   "text" =: "Another post!",
                   "tags" =: ["bulk", "insert"],
                   "date" =: now]
      :}
    > :{
      let post2 = ["author" =: "Eliot",
                   "title" =: "MongoDB is fun",
                   "text" =: "and pretty easy too!",
                   "date" =: now]
      :}
    > run $ insertMany "posts" [post1, post2]
    Right [Oid 4c16d67e c80c560858000001,Oid 4c16d67e c80c560858000002]

* Note that post2 has a different shape than the other posts - there
is no "tags" field and we've added a new field, "title". This is what we
mean when we say that MongoDB is schema-free.

Querying for More Than One Document
------------------------------------

To get more than a single document as the result of a query we use the
*find* method. *find* returns a cursor instance, which allows us to
iterate over all matching documents. There are several ways in which
we can iterate: we can call *next* to get documents one at a time
or we can get all the results by applying the cursor to *rest*:

    > Right cursor <- run $ find (select ["author" =: "Mike"] "posts")
    > run $ rest cursor

Of course you can use bind (*>>=*) to combine these into one line:

    > run $ find (select ["author" =: "Mike"] "posts") >>= rest

* Note: *next* automatically closes the cursor when the last
document has been read out of it. Similarly, *rest* automatically
closes the cursor after returning all the results.

Counting
--------

We can count how many documents are in an entire collection:

    > run $ count (select [] "posts")

Or count how many documents match a query:

    > run $ count (select ["author" =: "Mike"] "posts")

Range Queries
-------------

To do

Indexing
--------

To do
