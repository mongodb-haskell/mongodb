Type class may use result type as descriminator.
Null pointer exceptions caught at compile time.


Intro to Haskell

There are already plenty of languages to write your Mongo application in, so why would you consider [Haskell](http://www.haskell.org/)? The reason is there are several unique features in Haskell that help reduce bugs and increase programmer productivity (and serenity). In this article, I will introduce some of these features and describe how they could benefit your Mongo application. I will also discuss one disadvantage of Haskell that you should watch out for.

But first let me address probably the most common reason why Haskell is not considered: because no one you know is using it, i.e. it is not popular. My rebuttal to this is: even though Haskell is not popular, it is popular *enough*. The community and ecosystem (libraries and tools) has reached a critical mass such that you can easily find previous success stories (see [Haskell in industry](http://haskell.org/haskellwiki/Haskell_in_industry)), find Haskell programmers to hire (just send an email to the [Haskell mailing list](http://www.haskell.org/haskellwiki/Mailing_Lists)) and find libraries for most anything you would expect to find in a popular language (see [Hackage](http://hackage.haskell.org/packages/archive/pkg-list.html) for the list of libraries). So I think it is incorrect to reject Haskell for this reason. Assuming you agree that Haskell is a *mainstream* language by this definition of critical mass (or popular enough), we can now judge Haskell on its features alone.

I think the most distinguishing feature of Haskell is [declared effects](http://en.wikipedia.org/wiki/Effect_system). No other mainstream language has it. Declared effects means the type of a procedure includes the type of side effect it may have (in addition to the type of value it will return). This means that just by looking at a procedure's type you know what effect it may have, without having to remember or look at its code. Plus the compiler checks that the code does not violate its declared effect, which helps a lot with maintenance.

Another nice feature of Haskell is the elimination of null pointer exceptions. A possibly null value is declared as such, and you cannot get its non-null value without first checking for null. The compiler enforces this so it is impossible to get a null pointer exception at runtime.

Higher-order functions and currying makes it easy to code at a higher level. For example, you rarely need a for loop, because you use higher level operations instead like, map, filter, and fold.

[Type inference](http://en.wikipedia.org/wiki/Type_inference) makes code succint and thus easier to read and write.



