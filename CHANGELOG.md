# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Package Versioning Policy](https://wiki.haskell.org/Package_versioning_policy).

## [2.7.1.2] - 2022-10-26

### Added
- Support of OP_MSG protocol
- Clarifications in the documentation
- Allow optional TLS parameters

## [2.7.1.1] - 2021-06-14

### Fixed
- Sample code

### Added
- Clarification to the tutorial

## [2.7.1.0] - 2020-08-17

### Added
- Function findCommand

## [2.7.0.1] - 2020-04-07

### Fixed
- Error reporting for deletion of big messages
- Formatting of docs

## [2.7.0.0] - 2020-02-08

### Fixed
- Upgraded bson to compile with GHC 8.8

## [2.6.0.1] - 2020-02-01

### Fixed
- Parsing hostname with underscores in readHostPortM.

## [2.6.0.0] - 2020-01-03

### Added
- MonadFail. It's a standard for newer versions of Haskell,
- Open replica sets over tls.

### Fixed
- Support for unix domain socket connection,
- Stubborn listener threads.

## [2.5.0.0] - 2019-06-14

### Fixed
Compatibility with network 3.0 package

## [2.4.0.1] - 2019-03-03

### Fixed
Doc for modify method

## [2.4.0.0] - 2018-05-03

### Fixed
- GHC 8.4 compatibility. isEmptyChan is not available in base 4.11 anymore.

## [2.3.0.5] - 2018-03-15

### Fixed
- Resource leak in SCRAM authentication

## [2.3.0.4] - 2018-02-11

### Fixed
- Benchmark's build

## [2.3.0.3] - 2018-02-10

### Fixed
- aggregate requires cursor in mongo 3.6

## [2.3.0.2] - 2018-01-28

### Fixed
- Uploading files with GridFS

## [2.3.0.1] - 2017-12-28

### Removed
- Log output that littered stdout in modify many commands.

## [2.3.0] - 2017-05-31

### Changed
- Description of access function
- Lift MonadBaseControl restriction
- Update and delete results are squashed into one WriteResult type
- Functions insertMany, updateMany, deleteMany are rewritten to properly report
  various errors

## [2.2.0] - 2017-04-08

### Added
- GridFS implementation

### Fixed
- Write functions hang when the connection is lost.

## [2.1.1] - 2016-08-13

### Changed
- Interfaces of update and delete functions. They don't require MonadBaseControl
anymore.

## [2.1.0] - 2016-06-21

### Added
- TLS implementation. So far it is an experimental feature.
- Insert using command syntax with mongo server >= 2.6
- UpdateMany and UpdateAll commands. They use bulk operations from mongo
  version 2.6 and above. With versions below 2.6 it sends many updates.
- DeleteAll and DeleteMany functions use bulk operations with mongo server
  >= 2.6. If mongo server version is below 2.6 then it sends many individual
  deletes.

### Changed
- All messages will be strictly evaluated before sending them to mongodb server.
No more closed handles because of bad arguments.
- Update command is reimplemented in terms of UpdateMany.
- delete and deleteOne functions are now implemented using bulk delete
  functions.

### Removed
- System.IO.Pipeline module

### Fixed
- allCollections request for mongo versions above 3.0

## [2.0.10] - 2015-12-22

### Fixed
- SCRAM-SHA-1 authentication for mongolab

## [2.0.9] - 2015-11-07

### Added
- SCRAM-SHA-1 authentication for mongo 3.0

## [2.0.8] - 2015-10-03

### Fixed
- next function was getting only one batch when the request was unlimited,
  as a result you were receiving only 101 docs (default mongo batch size)

## [2.0.7] - 2015-09-04

### Fixed
- Slow requests to the database server.

## [2.0.6] - 2015-08-02

### Added
- Time To Live index

### Fixed
- Bug, the driver could not list more 97899 documents.
