Map/Reduce Example
------------------

This is an example of how to use the mapReduce function to perform
map/reduce style aggregation on your data.

This document has been shamelessly ported from the similar
[pymongo Map/Reduce Example](http://api.mongodb.org/python/1.4%2B/examples/map_reduce.html).

Setup
-----

To start, we’ll insert some example data which we can perform
map/reduce queries on:

> $ ghci -package mongoDB
> GHCi, version 6.12.1: http://www.haskell.org/ghc/  :? for help
> ...
> Prelude> :set prompt "> "
> > import Database.MongoDB
> > import Database.MongoDB.BSON
> > import Data.ByteString.Lazy.UTF8
> > c <- connect "localhost" []
> > let col = (fromString "test.mr1")
> > :{
> insertMany c col [
>       (toBsonDoc [("x", BsonInt32 1),
>                   ("tags", BsonArray [toBson "dog",
>                                       toBson "cat"])]),
>       (toBsonDoc [("x", BsonInt32 2),
>                   ("tags", BsonArray [toBson "cat"])]),
>       (toBsonDoc [("x", BsonInt32 3),
>                   ("tags", BsonArray [toBson "mouse",
>                                       toBson "cat",
>                                       toBson "doc"])]),
>       (toBsonDoc [("x", BsonInt32 4),
>                   ("tags", BsonArray [])])
> ]
> :}

Basic Map/Reduce
----------------

Now we'll define our map and reduce functions. In this case we're
performing the same operation as in the MongoDB Map/Reduce
documentation - counting the number of occurrences for each tag in the
tags array, across the entire collection.

Our map function just emits a single (key, 1) pair for each tag in the
array:

> > :{
> let mapFn = "
> function() {\n
>   this.tags.forEach(function(z) {\n
>     emit(z, 1);\n
>   });\n
> }"
> :}

The reduce function sums over all of the emitted values for a given
key:

> > :{
> let reduceFn = "
> function (key, values) {\n
>   var total = 0;\n
>   for (var i = 0; i < values.length; i++) {\n
>     total += values[i];\n
>   }\n
>   return total;\n
> }"
> :}

Note: We can’t just return values.length as the reduce function might
be called iteratively on the results of other reduce steps.

Finally, we call map_reduce() and iterate over the result collection:

> > mapReduce c col (fromString mapFn) (fromString reduceFn) [] >>= allDocs
> [[(Chunk "_id" Empty,BsonString (Chunk "cat" Empty)),(Chunk "value" Empty,BsonDouble 6.0)],[(Chunk "_id" Empty,BsonString (Chunk "doc" Empty)),(Chunk "value" Empty,BsonDouble 1.0)],[(Chunk "_id" Empty,BsonString (Chunk "dog" Empty)),(Chunk "value" Empty,BsonDouble 3.0)],[(Chunk "_id" Empty,BsonString (Chunk "mouse" Empty)),(Chunk "value" Empty,BsonDouble 2.0)]]

Advanced Map/Reduce
-------------------

MongoDB returns additional information in the map/reduce results. To
obtain them, use *runMapReduce*:

> > res <- runMapReduce c col (fromString mapFn) (fromString reduceFn) []
> > res
> [(Chunk "result" Empty,BsonString (Chunk "tmp.mr.mapreduce_1268105512_18" Empty)),(Chunk "timeMillis" Empty,BsonInt32 90),(Chunk "counts" Empty,BsonDoc [(Chunk "input" Empty,BsonInt64 8),(Chunk "emit" Empty,BsonInt64 12),(Chunk "output" Empty,BsonInt64 4)]),(Chunk "ok" Empty,BsonDouble 1.0)]

You can then obtain the results using *mapReduceResults*:

> > mapReduceResults c (fromString "test") res >>= allDocs
> [[(Chunk "_id" Empty,BsonString (Chunk "cat" Empty)),(Chunk "value" Empty,BsonDouble 6.0)],[(Chunk "_id" Empty,BsonString (Chunk "doc" Empty)),(Chunk "value" Empty,BsonDouble 1.0)],[(Chunk "_id" Empty,BsonString (Chunk "dog" Empty)),(Chunk "value" Empty,BsonDouble 3.0)],[(Chunk "_id" Empty,BsonString (Chunk "mouse" Empty)),(Chunk "value" Empty,BsonDouble 2.0)]]
