# Changelog

All notable changes to this project will be documented in this file. See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [3.0.0](https://github.com/blackopsrepl/rdedupe/compare/v2.0.0...v3.0.0) (2026-11-13)


### ⚠ BREAKING CHANGES

* the DataFrame library API and --delete unlinking are removed; see docs/migration.md.

### Features

* exact, bounded, recoverable deduplication engine (rdedupe 3.0) ([81efe19](https://github.com/blackopsrepl/rdedupe/commit/81efe19304810d4a62fd10f36941dda56c62ef49)), closes [#1](https://github.com/blackopsrepl/rdedupe/issues/1)
* extent resume and cross-filesystem refusal, fault-injected restore/purge/resume, journal write failure, creation-time identity, more mutation cases ([9610acf](https://github.com/blackopsrepl/rdedupe/commit/9610acffc8b4772745ba73c4875745bb99ee9721))
* name hard-link aliases in every output; xxh3 sampled fingerprints with BLAKE3 refinement at any size; real write-failure tests; dependency rationale; skill platform and exclusion wording ([5e5f35b](https://github.com/blackopsrepl/rdedupe/commit/5e5f35be6c23bf524408bc4d281c5cd4231b9849))
* queue budget, inventory metric, over-budget and cancellation tests, tested docs examples, cache interruption, extent crash injection ([6c5c396](https://github.com/blackopsrepl/rdedupe/commit/6c5c39610724199757993faff4cde7ea6901b955))
* scan-owned buffer pool lent to every stage ([bd430ce](https://github.com/blackopsrepl/rdedupe/commit/bd430ce582a6b7d34d98c674295040751bd381a5))


### Bug Fixes

* **audit:** error categories, NUL policy parity, prompt cancellation, scratch cleanup, metadata differences, fd-relative quarantine, capability probes, apply --resume ([698d62e](https://github.com/blackopsrepl/rdedupe/commit/698d62e4ec4a653bf24c7dccd005d1bf4e14c860))
* charge comparison failures to the representative, escape every human message, pipefail in CI and make ([ead41ea](https://github.com/blackopsrepl/rdedupe/commit/ead41ead25e2b3c1c5480f97f0699cb1fb68c7e8))
* close every finding of the independent review ([e134eb7](https://github.com/blackopsrepl/rdedupe/commit/e134eb716ddb4fb9dc8376ec5d3249d8239497d5))
* explicit quarantine capability refusal, F_FULLFSYNC directories on macOS, metadata-difference test; docs: storage, metadata, state machine, power loss, cache limits, queue ([6fe1bcd](https://github.com/blackopsrepl/rdedupe/commit/6fe1bcd673450032acbc86fde64802af4c2775ff))
* portable no-replace rename, musl ioctl type, inode-reuse-proof recovery ([ad76896](https://github.com/blackopsrepl/rdedupe/commit/ad7689665349a9d0261cfb8564b55d985f564f16))


### Performance Improvements

* in-memory classes for one-page buckets, lean scratch database, 4 readers ([8fcd6fb](https://github.com/blackopsrepl/rdedupe/commit/8fcd6fbb65960882d0144d9e4f98e344ea6be082))
* lockstep one-to-many comparison and on-demand refinement ([2504548](https://github.com/blackopsrepl/rdedupe/commit/250454899d6aa3c28c43647a03542e8575bfd7bf))
* printable-ASCII fast path for terminal escaping; escaping tests ([c6049a4](https://github.com/blackopsrepl/rdedupe/commit/c6049a4f79b33dd68d1faf07220050f4ea96e165))
* remove fixed scratch fsyncs, cache prepared statements, size small compare buffers ([09aa573](https://github.com/blackopsrepl/rdedupe/commit/09aa5738ea0ca974b866fd7df0ff85445f1329e1))
* wake buffer-pool waiters only when blocked; one listxattr call for files without attributes ([bb571da](https://github.com/blackopsrepl/rdedupe/commit/bb571da0a4401047b8dc92ba483835c53d8567e2))


### Build System

* no early-exit readers in pipelines under pipefail (dist-check SIGPIPE race) ([6664fcf](https://github.com/blackopsrepl/rdedupe/commit/6664fcf39be23b9a4ad22a5be3c1c590205686cd))


### Documentation

* benchmarks, measured README claims, benchmark-compare target ([3d0f4b9](https://github.com/blackopsrepl/rdedupe/commit/3d0f4b92b7c988b4232b742c4b5f43efa969531e))
* final benchmarks at 5e5f35b with measured hash choice and xxh3 attribution ([b7de5ae](https://github.com/blackopsrepl/rdedupe/commit/b7de5ae0ace24cbe01d93c47e168cdc8dc193197))
* final benchmarks at bb571da, cache repeat-scan measured separately, regression attributed ([6adca30](https://github.com/blackopsrepl/rdedupe/commit/6adca30be56a368c3c0a7508fe53af1b119c3cef))
* instruction counts after the final audit fixes ([22b0def](https://github.com/blackopsrepl/rdedupe/commit/22b0def1d6e66dd672c2f7ff95c5ab5cc8d3636b))
* mascot, user-facing README with screenshots from real runs; build: Makefile in the house style ([6be48f5](https://github.com/blackopsrepl/rdedupe/commit/6be48f5fcd41082d7cb8f73ed9aa33d2e70a1bb6))
* record the agent evaluation of the final archive (22b0def) ([205257b](https://github.com/blackopsrepl/rdedupe/commit/205257b534dfc17dde1ac90fdf90a2c887ce16ce))
* record the agent evaluation of the final archive (b7de5ae) ([3b8050d](https://github.com/blackopsrepl/rdedupe/commit/3b8050d80a7de115b94808fdbdab99d3d98b8da2))
* **skill:** complete recipes, resume recovery, error categories; tests: workflow postconditions and failure scenarios ([c55cd9a](https://github.com/blackopsrepl/rdedupe/commit/c55cd9a1dc4da24995f17069bf339f82ea03f7a0))
* **skill:** proceed with authorized quarantine unless there is evidence of concurrent writers ([ca18776](https://github.com/blackopsrepl/rdedupe/commit/ca18776e77b65de61a7384c419a41aff9218b03b))
