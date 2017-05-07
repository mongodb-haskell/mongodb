# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Package Versioning Policy](https://wiki.haskell.org/Package_versioning_policy).

## [2.3.0] - unreleased

### Changed
- Description of access function

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
