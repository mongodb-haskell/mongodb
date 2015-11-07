# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Package Versioning Policy](https://wiki.haskell.org/Package_versioning_policy).

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
