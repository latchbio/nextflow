# Latch SDK Changelog

## 1.0.3 - 2024-06-12

### Refactor

* Cleanup unnecessary logs

## 1.0.2 - 2024-06-12

### Fixed

* Use a single `ForkJoinPool` for all LatchPaths to avoid OOM when uploading hundreds of files

## 1.0.1 - 2024-06-10

### Fixed

* Fix race condition in LatchPath `upload` causing file corruption

## 1.0.0 - 2024-06-08

### Added

* Add versioning

