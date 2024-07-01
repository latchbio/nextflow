# Latch SDK Changelog

## 1.1.3 - 2024-07-01

### Added
* Add tag to task_info

## 1.1.2 - 2024-06-28

### Fixed

* CopyMoveHelper.java will now attempt to copy files to Latch directly (instead of 
  opening a stream and creating an auxiliary file to upload from)

## 1.1.1 - 2024-06-25

### Fixed

* delete tmp file after latch file upload
* fix memory leak caused by HttpClient creation

## 1.1.0 - 2024-06-25

### Added

* ability to download directories via LatchPath

## 1.0.11 - 2024-06-22

### Fixed

* bump log column length from 100 to 150

## 1.0.10 - 2024-06-20

### Fixed

* error handling when getting current workspace
* close LatchFileSystem thread executor after workflow onComplete

## 1.0.9 - 2024-06-19

### Added

* Add support for retry from failed task

## 1.0.8 - 2024-06-18

### Added

* HttpRetryClient -- retries on 429 and 500s
* Check task completion on "NodeTerminationError"
* decreasing polling interval from 15s to 5s

## 1.0.7 - 2024-06-17

### Added

* Add retry on "context deadline exceeded" error for k8s executor

## 1.0.6 - 2024-06-15

### Fixed

* Set line width to 100 so that logs don't get cut off

## 1.0.5 - 2024-06-15

### Fixed

* Fix releasing nextflow workflows to different workspace

## 1.0.4 - 2024-06-13

### Fixed

* Use ExecutorService instead of `ForkJoinPool` because ForkJoinPool doesn't cleanup threads -- causes memory to be leaked
* Move memory allocation logic into thread to avoid OOM when uploading large files

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

