# Consider Haskell
### by Tony Hannan, July 2011

There are already plenty of languages to choose from for your Mongo application, so why consider [Haskell](http://www.haskell.org/)? The reason is there are several unique features in Haskell that help reduce bugs and increase programmer productivity (and serenity). In this article, I will introduce some of these features and describe how they could benefit your application. I will also discuss one troublesome feature of Haskell that you should watch out for.

Before we get into features, let me address probably the most common reason why many people do not consider Haskell: because Haskell is not popular (no one they know uses it). My rebuttal to this is: even though Haskell is not popular, it is popular *enough*. The Haskell community and ecosystem (libraries and tools) has reached a critical mass such that you can easily find previous success stories (see [Haskell in industry](http://haskell.org/haskellwiki/Haskell_in_industry)), find Haskell programmers to hire (just send an email to the [Haskell mailing list](http://www.haskell.org/haskellwiki/Mailing_Lists)), and find libraries for most anything you would expect to find in a popular language (see [Hackage](http://hackage.haskell.org/packages/archive/pkg-list.html) for the list of over 3000 libraries). For this article, let's define a *mainstream* language as one with this critical mass. I claim that you should consider all mainstream languages (I would say there are about 10 of them). Assuming you agree, let's now judge Haskell on its features alone.

Haskell's most important and distinguishing feature is [declared effects](http://en.wikipedia.org/wiki/Effect_system). No other mainstream language has it. Declared effects means the type of a procedure includes the type of side effects it may have (in addition to the type of value it returns). In other words, the type declares what the procedure *does*, not just its inputs and output. For example, a procedure with type `Document -> DbAction ObjectId` declares that the procedure takes a Document, performs a DbAction, and returns an ObjectId. If you looked at the DbAction type definition it would say that a DbAction reads/writes to the database and may throw a DbException.

This extension of type to include effect as well as input and output is significant. It has many software engineering benefits. It is easier to read code because you don't have to look at a procedure's implementation to know what effect it may have. It helps structure your code; you can't inadvertently call a side-effecting procedure from a procedure declared not to have that effect. But most importantly it makes side-effects explicit. Side effects increase software complexity significantly, especially in a multi-threaded application. Declared effects forces you to manage side effects and keep them under control.

A procedure that has no side effects and is not affected by others' effects is called a [(pure) function](http://en.wikipedia.org/wiki/Pure_function). For example, we know that a function with type `Document -> ObjectId` has no side effects and that the resulting ObjectId is derived solely from the input Document. Implementing a significant portion of your logic using pure functions reduces complexity and increases reusability significantly. Pure functions are easy to understand and reuse because there is no shared state to mess up or new state to set up.

Another nice feature of Haskell is the elimination of null pointer exceptions. A possibly null value is declared as such, and its non-null value cannot be extracted without first checking for null. The compiler enforces this so it is impossible to get a null pointer exception at runtime.

A [higher-order function](http://en.wikipedia.org/wiki/Higher-order_function) is a function that take other functions as input or output. They allow you to parameterize operations in addition to data, which makes it easier to abstract and reuse code. For example, in

	sum list = case list of [] -> 0; head:tail -> head + sum tail
	prod list = case list of [] -> 1; head:tail -> head * prod tail

we can abstract out the common code while parameterizing the specific code

	reduce id op list = case list of [] -> id; head:tail -> head `op` reduce id op tail
	sum = reduce 0 (+)
	prod = reduce 1 (*)

Notice that one of the new parameters is an operation (function). `reduce` is a new higher level operation for looping over a list that we can now use. Higher-order functions facilitate higher level coding.

[Type inference](http://en.wikipedia.org/wiki/Type_inference) means you don't have supply types of variables, the compiler will infer them from their use. Type inference makes code succint, and thus easier to read and write, while still maintaining static type safety. For example, the code above will compile and infer that the type of `reduce` is `b -> (a -> b -> b) -> [a] -> b`, which can be read as "For any types A and B, this function takes three inputs, a B, a function with inputs A and B and output B, and a list of As, and outputs a B". A and B can be the same type but in the general case they are not. Syntax note: lowercase types in Haskell are type variables while uppercase types are actual types.

Haskell threads are very lightweight, allowing you to fork millions of short-lived threads without undue overhead. Internally, Haskell schedules its threads onto a small set of operating system threads (usually one per CPU core). Each OS thread use asynchronous IO (via epoll/kqueue) so it can run other Haskell threads when a Haskell thread blocks on IO. So Haskell threads gives you the simple semantics of blocking IO with the low overhead of asynchronous IO.

Haskell expressions are [lazily evaluated](http://en.wikipedia.org/wiki/Lazy_evaluation), meaning outer expressions are evaluated *before* inner expressions and an expression is never evaluated if its value is never needed. For example, ``let f x = 42; n = 1 `div` 0; in f n`` evaluates to 42 instead of failing on divide by 0 because the value of `n` is never used. Lazy evaluation facilitates modularity because you can keep operations separate knowing that the inner operation will only proceed when the outer operation demands it. For example,

	fibs = fibonacci 0 1 where fibonacci a b = a : fibonacci b (a + b)
