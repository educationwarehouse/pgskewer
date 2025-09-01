# Changelog

<!--next-version-placeholder-->

## v0.1.0 (2025-09-01)

### Feature

* Pass information about the current pipeline to substeps ([`1e97a94`](https://github.com/educationwarehouse/pgskewer/commit/1e97a94e2d0204c6ebb654fb7a70ab1ea6551f31))
* Rename table to `pgqueuer_result`; more tests+docs ([`a1f9037`](https://github.com/educationwarehouse/pgskewer/commit/a1f9037dfb36a5115a628a99902f75321990f072))
* Basic cli to enqueue new jobs ([`d0bee5b`](https://github.com/educationwarehouse/pgskewer/commit/d0bee5bcf9383a6b82d0b08c9e9a668718021ea7))
* Support 'crashable' functions that don't halt the pipeline; ensure entrypoints are `async` ([`e2163df`](https://github.com/educationwarehouse/pgskewer/commit/e2163dfe760ddd64fd3061e96c8d87c692bea9ef))
* Add optional `migrations` using `edwh-migrate` to keep in sync with pgqueuer (+pgskewer) changes easily ([`24c7f9f`](https://github.com/educationwarehouse/pgskewer/commit/24c7f9f4cd39b5dc791dc67df8458dc966f85247))
* Added `.result(job_id, timeout: optional[int])` to poll for job results ([`99f8aa8`](https://github.com/educationwarehouse/pgskewer/commit/99f8aa81a0b6bc9dd98aae973f60cffad586c415))
* Initial `pgskewer` functionality ([`628c44c`](https://github.com/educationwarehouse/pgskewer/commit/628c44cf43d78bc5c7c54a066e1a31275c8b6a66))

### Fix

* **migrate:** Use the same data retention logic as other `pgqueuer` tables, to prevent referencing ids that don't exist anymore ([`7128d3e`](https://github.com/educationwarehouse/pgskewer/commit/7128d3e841ffc8832e6db1582d54eacc318b4aec))
* **enqueue:** Support passing your own 'unique_key' ([`88c9f1e`](https://github.com/educationwarehouse/pgskewer/commit/88c9f1edf0da2da1ca71c47734f0049e034dee49))
* Ensure subtasks have a dedupe key ([`ff6b851`](https://github.com/educationwarehouse/pgskewer/commit/ff6b851aafac5355bdc9f190ad9df54dcdf36a69))
* Move `edwh-uuid7` to core dependencies ([`65e911f`](https://github.com/educationwarehouse/pgskewer/commit/65e911ff42a53274b729881c9a7ebbb0fd14f951))
* Use uuid7 dedupe key as unique identifier that persists in the _result table even when the job id has been reused ([`7a13c9e`](https://github.com/educationwarehouse/pgskewer/commit/7a13c9e838b6e0f6b083115bd0dfa85ed1aa77e6))
* Support tuples in addition to lists for grouping steps ([`bf39048`](https://github.com/educationwarehouse/pgskewer/commit/bf39048b26217da8e5497c652213af0f6e72953f))
* Silence pydal syntax warning during import ([`a9e6f6c`](https://github.com/educationwarehouse/pgskewer/commit/a9e6f6cf1c8d08ff2e0811fc0971eb6d693367d3))
* Support Python 3.12 (which introduces the new typing syntax) ([`3a1882b`](https://github.com/educationwarehouse/pgskewer/commit/3a1882bdf62b9c5851d57cc426a9518a8d52a699))

### Documentation

* Included initial README and added todo's ([`387f57b`](https://github.com/educationwarehouse/pgskewer/commit/387f57b18836a0b08dafe6bef9e366fa1e8345f8))
