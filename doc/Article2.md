# Considering Haskell
### by Tony Hannan, July 2011

There are already plenty of languages to choose from for your Mongo application, so why consider [Haskell](http://www.haskell.org/)? The reason is there are several unique features in Haskell that help reduce bugs and increase programmer productivity (and serenity). In this article, I will introduce some of these features and describe how they could benefit your application. I will also discuss one troublesome feature of Haskell that you should be aware of.

Before we get into features, let me address probably the most common reason why many people do not consider Haskell: because Haskell is not popular (no one they know uses it). My rebuttal to this is: even though Haskell is not popular, it is popular *enough*. The Haskell community and ecosystem (libraries and tools) has reached a critical mass such that you can easily find previous success stories (see [Haskell in industry](http://haskell.org/haskellwiki/Haskell_in_industry)), find Haskell programmers to hire (just send an email to the [Haskell mailing list](http://www.haskell.org/haskellwiki/Mailing_Lists)), and find libraries for most anything you would expect to find in a popular language (see [Hackage](http://hackage.haskell.org/packages/archive/pkg-list.html) for the list of over 3000 libraries). Let's define a *mainstream* language as one with this critical mass. I claim that you should consider all mainstream languages (I would say there are about 10 of them). Assuming you agree, let's now judge Haskell on its features alone.

Haskell's most important and distinguishing feature is its [*declared effects*](http://en.wikipedia.org/wiki/Effect_system). No other mainstream language has it. Declared effects means the type of a procedure includes the type of side effects it may have (in addition to the type of value it returns). In other words, the type declares what the procedure *does*, not just its inputs and output. For example, a procedure with type `Document -> DbAction ObjectId` declares that the procedure takes a Document, performs a DbAction, and returns an ObjectId. If you looked at DbAction's type definition it would say that a DbAction reads/writes to the database and may throw a DbException.

This extension of type to include effect as well as input and output is significant. It has many software engineering benefits. It is easier to read code because you don't have to look at a procedure's implementation to know what effect it may have. It helps structure your code; you can't inadvertently call a side-effecting procedure from a procedure declared not to have that effect. But most importantly it makes side-effects explicit. Side effects increase software complexity significantly, especially in a multi-threaded application. Declared effects forces you to manage side effects and keep them under control.

A procedure that has no side effects and is not affected by others' effects is called a [(pure) function](http://en.wikipedia.org/wiki/Pure_function). For example, we know that a function with type `Document -> ObjectId` has no side effects and that the resulting ObjectId is derived solely from the input Document. Implementing a significant portion of your logic using pure functions reduces complexity and increases reusability significantly. Pure functions are easy to understand and reuse because there is no shared state to mess up or new state to set up.

Another nice feature of Haskell is the elimination of null pointer exceptions. A possibly null value is declared as such, and its non-null value cannot be extracted without first checking for null. The compiler checks this so it is impossible to get a null pointer exception at runtime.

[Higher-order functions](http://en.wikipedia.org/wiki/Higher-order_function) are functions that take other functions as input or output. They allow you to parameterize operations in addition to data, which makes it easier to abstract and reuse code. For example, in

	sum list = case list of [] -> 0; head:tail -> head + sum tail
	prod list = case list of [] -> 1; head:tail -> head * prod tail

we can abstract out the common code while parameterizing the specific code

	reduce op id list = case list of [] -> id; head:tail -> head `op` reduce id op tail
	sum = reduce (+) 0
	prod = reduce (*) 1
	count = reduce (\ item cnt -> cnt + 1) 0

Notice that one of the new parameters is an operation (function). Without higher-order functions it is harder to create these higher-level abstractions, causing you to always program at a lower level. For example, `for` loops are rare in Haskell because of the availability of higher-level, higher-order functions like `map`, `filter`, and `reduce`.

[Type inference](http://en.wikipedia.org/wiki/Type_inference) means you don't have to supply the types of variables, the compiler will infer them from their use. Type inference makes code succint, and thus easier to read and write, while still maintaining static type safety. For example, the code above will compile and infer that the type of `reduce` is `(a -> b -> b) -> b -> [a] -> b`, which can be read as "For any types A and B, this function takes three inputs, a function with inputs A and B and output B, a B, and a list of As, and outputs a B". A and B can be the same type but in the general case they are not (lowercase types in Haskell are type variables while uppercase types are actual types).

Haskell has excellent concurrency and parallelism to take advantage of multiple cores and GPUs. First, Haskell threads are very lightweight, allowing you to fork millions of short-lived threads without undue overhead. Internally, Haskell threads are scheduled onto a small set of operating system threads (usually one per CPU core). Each OS thread use asynchronous IO (via epoll/kqueue) so it can run other Haskell threads when a Haskell thread blocks on IO. So Haskell threads gives you the simple semantics of blocking IO with the low overhead of asynchronous IO. Second, Haskell supports [software transactional memory](http://www.haskell.org/haskellwiki/Software_transactional_memory), which allows you to atomically update a set of shared variables, which is simpler and more composable than using mutexes/locks. Third, you can parallelize pure code (while maintaining determinism) using simple [`par` annotations](http://hackage.haskell.org/package/parallel). Finally, [nested data parallelism](http://www.haskell.org/haskellwiki/GHC/Data_Parallel_Haskell) gives you automatic parallel processing of arrays whose elements can be any type of value including arrays, which are also processed in parallel. When compared to traditional [flat data parallelism](http://en.wikipedia.org/wiki/Data_parallelism), nesting greatly increases the class of algorithms and data structures that you can easily parallelize such as divide-and-conquer algorithms and trees.

Haskell expressions are [lazily evaluated](http://en.wikipedia.org/wiki/Lazy_evaluation), meaning outer expressions are evaluated *before* inner expressions and an expression is never evaluated if its value is never needed. For example, in

	cond bool a b = case bool of True -> a; False -> b
	x = cond True 1 (2 `div` 0)

`x` evaluates to 1 instead of failing on divide-by-zero because the last argument is not evaluated in this case. Lazy evaluation facilitates user-defined control structures, like `cond` above. It also enables infinite data structures, like `ones = 1 : ones` (infinite list of 1's) or `[1,2..]` (infinite sequence), because later elements are only created on demand. Finally, lazy evaluation facilitates modularity because you can separate functionality knowing that an inner operation will only proceed when its outer operation demands it. Intermediate data structures (eg. lists) won't be built in full then later consumed, instead each element will be built on demand and immediately consumed thus using constant space. This behavior is similar to piping programs together in Unix. To get similar behavior in an eager language, you could use an imperative generator for intermediate operations, but this is more complex, not pervasive, and not pure.

However, a drawback of lazy evaluation (and thus Haskell) is hard-to-predict space usage. For example, we know the above `reduce` function requires (stack) space linear to the size of the list, but what about its [tail recursive](http://en.wikipedia.org/wiki/Tail_recursion) version, which uses constant stack space.

	reduce op id list = red id list  where red acc list = case list of [] -> acc; head:tail -> red (head `op` acc) tail

Stepping through the execution on a small list shows an unevaluated expression building up consuming space linear to the size of list.

	reduce (+) 0 [1,2,3]
	=> red 0 [1,2,3]
	=> red (1 + 0) [2,3]
	=> red (2 + (1 + 0)) [3]
	=> red (3 + (2 + (1 + 0))) []
	=> 3 + (2 + (1 + 0))
	=> 3 + (2 + 1)
	=> 3 + 3
	=> 6

This is not good and unexpected. How can you detect these problems without analyzing every expression for unevaluated expression build up? The answer is: [profile](http://www.haskell.org/ghc/docs/7.0-latest/html/users_guide/profiling.html) your application if it is not performing as expected. It will tell you how much time and space different functions are consuming. Once you find the troublesome code, the solution is easy: Force the evaluation of the unevaluated expression by marking it as *strict* (*eager*). This is done by prefixing its variable with `!` and adding language extension [*BangPatterns*](http://www.haskell.org/ghc/docs/7.0-latest/html/users_guide/bang-patterns.html).

	{-# LANGUAGE BangPatterns #-}
	reduce op id list = red id list  where red !acc list = case list of [] -> acc; head:tail -> red (head `op` acc) tail

	reduce (+) 0 [1,2,3]
	=> red 0 [1,2,3]
	=> red (1 + 0) [2,3]
	=> red 1 [2,3]
	=> red (2 + 1) [3]
	=> red 3 [3]
	=> red (3 + 3) []
	=> red 6 []
	=> 6

Unfortunately, profiling is not trivial, it takes some time and analysis. But it is a task usually needed anyway for production-quality applications. Over time you may develop enough expertise to recognize problematic unevaluated expressions as you write them, but for now I would get accustom to profiling.

In conclusion, Haskell is a modern mainstream programming language designed to help you write high quality software easily. The [MongoDB driver for Haskell](http://hackage.haskell.org/package/mongoDB) is ready if you decide to build a Mongo application in Haskell.
