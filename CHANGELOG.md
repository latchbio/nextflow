# Latch Nextflow Changelog

## 2.2.1

## Fixed
* Move logs file upload to nextflow runtime because process containers may not have all
  dependencies required to run `latch cp`

## 2.2.0

## Fixed
* Allow task hash to be `null` when processes are skipped

## 2.1.9

## Fixed
* Fix for downloading empty Latch paths

## 2.1.8

## Fixed
* Update .command polling script to use integer sleep times (decimal not always supported by host)

## 2.1.7

## Fixed
* Read exit code from container instead of .exitcode file
* Disable publishDir overwrite when publishing files for a cached task

## 2.1.6

## Fixed
* NullPointerException when relaunching cached tasks
* Race condition when concurrently creating multiple LatchFileSystem objects

## 2.1.5 - 2024-10-03

### Added
* Chunk writes when downloading files to optimize write performance

## 2.1.4 - 2024-10-02

### Added
* Report task hash to Latch backend
* Add retry logging for HttpRetryClient
* If latch logging directory specified, upload .command.* files to LData

## 2.1.3 - 2024-10-01

### Changed
* Add retires on Latch file part download
* Improved logging for dispatcher failures

## 2.1.2 - 2024-09-28

### Changed
* Print polling loop every 10 attempts and remove an echo

## 2.1.1 - 2024-09-24

### Changed
* Add explicit timeouts to all HTTP operations to prevent hanging executions

## 2.1.0 - 2024-09-14

### Changed
* Add explicit fsync of generated files in runtime and task steps
* Add polling for files in case the changes in the shared file system are not seen immediately after task start/finish

## 2.0.3 - 2024-09-09

### Changed
* Always sync written task files to disk. OFS only guarantees that files will be seen by other clients if the file is fsynced to the disk.

## 2.0.1 - 2024-08-31

### Fixed
* Fix status for native nextflow processes

## 2.0.0 - 2024-08-23

### Changed
* Use v2 Nextflow scheduler

## 1.1.7 - 2024-08-09

### Added
* Parallel downloads for latch paths

## 1.1.6 - 2024-07-26

### Added
* Ansi color codes to warning and error messages

## 1.1.5 - 2024-07-17

### Added
* Cleanup logging for DispatcherClient

## 1.1.4 - 2024-07-11

### Added
* storeDir support for Latch Paths

## 1.1.3 - 2024-07-01

### Added
* Add tag to task_info
* Demote K8s unschedulable warning to debug

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

