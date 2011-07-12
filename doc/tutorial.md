This tutorial takes you through inserting, updating, and querying documents.

### Getting Ready

Start a local MongoDB server in a separate terminal window:

	$ mkdir mongoFiles
	$ mongod --dbpath mongoFiles

Start a haskell session:

	$ ghci
	> :set prompt "> "

Import the MongoDB driver library, and set OverloadedStrings so literal strings are converted to UTF-8 automatically.

	> :set -XOverloadedStrings
	> import Database.MongoDB
	> import Data.CompactString ()  -- only needed when using ghci

### Connecting

Establish a connection to your local Mongo server on the standard port (27017):

	> pipe <- runIOE $ connect $ host "127.0.0.1"

A host with non-standard port would look like `Host "127.0.0.1" (PortNumber 27001)`.

`connect h` has type `ErrorT IOError IO Pipe`. `runIOE` brings this type back down to `IO Pipe` and throws the IOError in IO if present. One design principle of this driver was to make DB errors explicit, hence the need for `runIOE`.

A `Pipe` is a thread-safe, pipelined (a' la [HTTP pipelining](http://en.wikipedia.org/wiki/HTTP_pipelining)) TCP connection to a MongoDB server. Multiple threads can use the pipe at the same time. The pipelining feature is used by cursors and not exposed to the user.

### Action monad

A DB read or write operation is called a DB `Action`. A DB Action is a monad so you can sequence them together. To run an Action supply it to the `access` function with the Pipe to use, the `AccessMode` for read/write operations, and the `Database` to access. For example, to list all collections in the "test" database:

	> access pipe master "test" allCollections

`access` return either Left `Failure` or Right result. Failure means there was a connection failure, or a read/write failure like cursor expired or duplicate key insert.

`master` is an `AccessMode`. Access mode indicates how reads and writes will be performed. Its three modes are: `ReadStaleOk`, `UnconfirmedWrites`, and `ConfirmWrites GetLastErrorParams`. `master` is just short hand for `ConfirmWrites []`. The first mode may be used against a slave or a master server, the last two must be used against a master server.

Since we are working in ghci, which requires us to start from the IO monad every time, we'll define a convenient *run* function that takes an action and executes it against our "test" database on the server we just connected to, with master access mode:

	> let run act = access pipe master "test" act

### Databases and Collections

To see all the databases available on the server:

	> run allDatabases

The "test" database in context is ignored in this case because `allDatabases` is not a query on a specific database but on the server as a whole.

Databases and collections do not need to be created, just start using them and MongoDB will automatically create them for you. In the examples below we'll be using the "test" database (captured in *run* above) and the "posts" colllection.

### Documents

Data in MongoDB is represented (and stored) using JSON-style documents, called BSON documents. A `Document` is simply a list of `Field`s, where each field is a label-value pair. A `Value` is a basic type like Bool, Int, Float, String, Time; a special BSON value like Binary, Javascript, ObjectId; a (embedded) Document; or a list of Values. Here's an example document which could represent a blog post:

	> let post = ["author" =: "Mike", "text" =: "My first blog post!", "tags" =: ["mongoDB", "Haskell"]]

### Inserting One

To insert a document into a collection we can use the `insert` function.

	> run $ insert "posts" post

When a document is inserted and it does not contain an *_id* field then it is added with a globally unique value of type `ObjectId`. The *_id* value can be any type but must be unique across the collection. `insert` returns the *_id* value of the inserted document.

After inserting the first document, the "posts" collection has actually been created on the server. We can verify this by listing all of the collections in our database again:

	> run allCollections

Note, the "system.indexes" collection is a special internal collection that was created automatically.

### Reading One

The most basic type of query that can be performed in MongoDB is `findOne`. This function returns a single document matching the selection, or `Nothing` if there are no matches. It is useful when you know there is only one matching document, or are only interested in the first match. Here we use `findOne` to get the first document from the posts
collection:

	> run $ findOne $ select [] "posts"

The result is a document matching the one that we inserted previously. Note, the returned document contains the _id field, which was automatically added on insert.

`findOne` also supports querying on specific elements that the resulting document must match. For example, to limit our results to a document with author "Mike" we do:

	> run $ findOne $ select ["author" =: "Mike"] "posts"

If we try with a different author, like "Eliot", we'll get no result:

	> run $ findOne $ select ["author" =: "Eliot"] "posts"

`fetch` is the same as `findOne` except it fails if no document matches.

### Inserting Many

In order to make querying a little more interesting, let's insert a few more documents. In addition to inserting a single document, we can also perform bulk insert operations, by using the `insertMany` function which accepts a list of documents to be inserted. It sends only a single write operation to the server.

	> let post1 = ["author" =: "Mike", "text" =: "Another post!", "tags" =: ["bulk", "insert"]]
	> let post2 = ["author" =: "Eliot", "title" =: "MongoDB is fun", "text" =: "and pretty easy too!"]
	> run $ insertMany "posts" [post1, post2]

Note that *post2* has a different shape than the other posts; it has no "tags" field and a new "title" field. Documents in the same collection can have different schemas.

### Reading Many

To retrieve more than a single document we use the `find` function. `find` returns a `Cursor`, which allows us to
iterate over the matching documents. There are few ways in which we can iterate: `next` gets the documents one at a time, and `rest` gets all (remaining) documents in the query result.

	> run $ find (select ["author" =: "Mike"] "posts") >>= rest

`next` automatically closes the cursor when the last document has been read out of it, similarly for `rest`. Otherwise, you should close a cursor if you don't exhaust it via `closeCursor`.

### Counting

You can count how many documents are in an entire collection:

	> run $ count $ select [] "posts"

Or count how many documents match a query:

	> run $ count $ select ["title" =: ["$exists" =: True]] "posts"

### Sorting

`sort` takes the fields to sort by and whether ascending (1) or descending (-1)

	> run $ find (select [] "posts") {sort = ["author" =: 1, "text" =: 1]} >>= rest

If you don't sort, documents are returned in *natural* order, which is the order found on disk. Natural order is not particularly useful because, although the order is often close to insertion order, it is not guaranteed.

### Projecting

`project` returns partial documents containing only the fields you include (1). However, *_id* is always included unless you exclude it (0).

	> run $ find (select [] "posts") {project = ["author" =: 1, "_id" =: 0]} >>= rest

### Updating

`save` updates an existing document

	> run $ fetch (select ["author" =: "Eliot"] "posts") >>= save "posts" . merge ["tags" =: ["hello"]]

or inserts a new document if its *_id* is new or missing

	> run $ save "posts" ["author" =: "Tony", "text" =: "hello world"]

`modify` updates every document matching selection using supplied modifier. For example:

	> run $ modify (select [] "posts") ["$push" =: ["tags" =: "new"]]

### Deleting

`delete` deletes all documents matching selection. `deleteOne` deletes one document matching selection (the first one in *natural* order), if any

	> run $ delete $ select ["author" =: "Homer"] "posts"  -- none deleted in this case

### Documentation

Documentation on the Mongo query language (i.e. the selector document, modifier document, etc.) can be found at the [MongoDB Developer Zone](http://www.mongodb.org/display/DOCS/Developer+Zone).

Haddock generated documentation of this Haskell driver can be found on [Hackage](http://hackage.haskell.org/package/mongoDB).

