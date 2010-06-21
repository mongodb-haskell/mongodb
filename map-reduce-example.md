Map/Reduce Example
------------------

This is an example of how to use the mapReduce function to perform
map/reduce style aggregation on your data.

This document has been shamelessly ported from the similar
[pymongo Map/Reduce Example](http://api.mongodb.org/python/1.4%2B/examples/map_reduce.html).

Setup
-----

To start, we'll insert some example data which we can perform
map/reduce queries on:

    $ ghci -package mongoDB
    GHCi, version 6.12.1: http://www.haskell.org/ghc/  :? for help
    ...
    Prelude> :set prompt "> "
    > :set -XOverloadedStrings
    > import Database.MongoDB
    > Right conn <- connect (server "localhost")
    > let run act = runConn (useDb "test" act) con
    > :{
    run $ insertMany "mr1" [
          ["x" =: 1, "tags" =: ["dog", "cat"]],
          ["x" =: 2, "tags" =: ["cat"]],
          ["x" =: 3, "tags" =: ["mouse", "cat", "dog"]],
          ["x" =: 4, "tags" =: ([] :: [String])]
    ]
    :}

Basic Map/Reduce
----------------

Now we'll define our map and reduce functions. In this case we're
performing the same operation as in the MongoDB Map/Reduce
documentation - counting the number of occurrences for each tag in the
tags array, across the entire collection.

Our map function just emits a single (key, 1) pair for each tag in the
array:

    > :{
    let mapFn = Javascript [] "
    function() {\n
      this.tags.forEach(function(z) {\n
        emit(z, 1);\n
      });\n
    }"
    :}

The reduce function sums over all of the emitted values for a given
key:

    > :{
    let reduceFn = Javascript [] "
    function (key, values) {\n
      var total = 0;\n
      for (var i = 0; i < values.length; i++) {\n
        total += values[i];\n
      }\n
      return total;\n
    }"
    :}

Note: We can't just return values.length as the reduce function might
be called iteratively on the results of other reduce steps.

Finally, we run mapReduce and iterate over the result collection:

    > runDb "test" $ runMR (mapReduce "mr1" mapFn reduceFn) >>= rest
    Right [[ _id: "cat", value: 3.0],[ _id: "dog", value: 2.0],[ _id: "mouse", value: 1.0]]

Advanced Map/Reduce
-------------------

MongoDB returns additional statistics in the map/reduce results. To
obtain them, use *runMR'* instead:

    > runDb "test" $ runMR' (mapReduce "mr1" mapFn reduceFn)
    Right [ result: "tmp.mr.mapreduce_1276482643_7", timeMillis: 379, counts: [ input: 4, emit: 6, output: 3], ok: 1.0]

You can then obtain the results from here by quering the result collection yourself. *runMR* (above) does this for you but discards the statistics.
