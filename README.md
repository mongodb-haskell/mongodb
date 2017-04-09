This is the Haskell MongoDB driver (client). [MongoDB](http://www.mongodb.org) is a free, scalable, fast, document database management system. This driver lets you connect to a MongoDB server, and update and query its data. It also lets you do adminstrative tasks, like create an index or look at performance statistics.

[![Join the chat at https://gitter.im/mongodb-haskell/mongodb](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/mongodb-haskell/mongodb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/mongodb-haskell/mongodb.svg?branch=master)](https://travis-ci.org/mongodb-haskell/mongodb)

### Documentation

* [Quick example](https://github.com/mongodb-haskell/mongodb/blob/master/doc/Example.hs)
* [Tutorial](https://github.com/mongodb-haskell/mongodb/blob/master/doc/tutorial.md)
* [Driver API](http://hackage.haskell.org/package/mongoDB)
* [MapReduce example](http://github.com/mongodb-haskell/mongodb/blob/master/doc/map-reduce-example.md)
* [Driver design](https://github.com/mongodb-haskell/mongodb/blob/master/doc/Article1.md)
* [MongoDB DBMS](http://www.mongodb.org)

### Dev Environment

It's important for this library to be tested with various versions of mongodb
server and with different ghc versions. In order to achieve this we use docker
containers and docker-compose. This repository contains two files: docker-compose.yml
and reattach.sh.

Docker compose file describes two containers.

One container is for running mongodb server. If you want a different version of
mongodb server you need to change the tag of mongo image in the
docker-compose.yml. In order to start your mongodb server you need to run:

```
docker-compose up -d mongodb
```

In order to stop your containers without loosing the data inside of it:

```
docker-compose stop mongodb
```

Restart:

```
docker-compose start mongodb
```

If you want to remove the mongodb container and start from scratch then:

```
docker-compose stop mongodb
docker-compose rm mongodb
docker-compose up -d mongodb
```

The other container is for compiling your code. By specifying the tag of the image
you can change the version of ghc you will be using. If you never started this
container then you need:

```
docker-compose run mongodb-haskell
```

It will start the container and mount your working directory to
`/opt/mongodb-haskell` If you exit the bash cli then the conainer will stop.
In order to reattach to an old stopped container you need to run script
`reattach.sh`.  If you run `docker-compose run` again it will create another
container and all work made by cabal will be lost. `reattach.sh` is a
workaround for docker-compose's inability to pick up containers that exited.

When you are done with testing you need to run:
```
docker-compose stop mongodb
```

Next time you will need to do:
```
docker-compose start mongodb
reattach.sh
```
It will start your stopped container with mongodb server and pick up the stopped
container with haskell.
