# PostgreSQL 17 Source Tree Overview (src/)

This document summarizes what lives under `src/` and its nested subdirectories, using a hierarchical table for readability. Empty cells inherit the directory from the previous non-empty level.

Updated: 2025-09-16

## Drill-down quick links

- [backend/storage/ipc](#level-6-drill-down-backendstorageipc)
- [backend/storage/lmgr](#level-6-drill-down-backendstoragelmgr)
- [backend/storage/buffer](#level-6-drill-down-backendstoragebuffer)
- [backend/storage/smgr](#level-6-drill-down-backendstoragesmgr)
- [backend/storage/page](#level-6-drill-down-backendstoragepage)
- [backend/storage/file](#level-6-drill-down-backendstoragefile)
- [backend/storage/aio](#level-6-drill-down-backendstorageaio)
- [backend/storage/freespace](#level-6-drill-down-backendstoragefreespace)
- [backend/storage/sync](#level-6-drill-down-backendstoragesync)
- [backend/access/transam](#level-6-drill-down-backendaccesstransam)
- [backend/access/heap](#level-6-drill-down-backendaccessheap)
- [backend/optimizer/path](#level-6-drill-down-backendoptimizerpath)
- [backend/executor](#level-6-drill-down-backendexecutor)
- [backend/nodes](#level-6-drill-down-backendnodes)
- [backend/parser](#level-6-drill-down-backendparser)
- [backend/foreign](#level-6-drill-down-backendforeign)
- [backend/tsearch](#level-6-drill-down-backendtsearch)

## Directory map

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Purpose / What’s here |
|---|---|---|---|---|---|
| src |  |  |  |  | Top-level source tree for PostgreSQL server, client tools, common libraries, headers, and build scripts. |
|  | backend |  |  |  | Core database server backend (postmaster, executor, storage, catalog, etc.). Builds the `postgres` server. |
|  |  | access |  |  | Access methods and lower-level storage APIs; MVCC visibility and tuple access layers. |
|  |  |  | [brin](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/README) |  | Block Range Index access method and support code. |
|  |  |  | common |  | Tuple/rel utilities shared by access methods. |
|  |  |  | [gin](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/README) |  | Generalized Inverted Index implementation. |
|  |  |  | [gist](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/README) |  | Generalized Search Tree access method. |
|  |  |  | [hash](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/README) |  | Hash index access method. |
|  |  |  | [heap](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/README.HOT) |  | Heap storage (table AM), HOT/pruning/visibility. |
|  |  |  | index |  | Index AM glue and generic index APIs. |
|  |  |  | [nbtree](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/README) |  | B-Tree access method implementation. |
|  |  |  | [rmgrdesc](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/README) |  | Resource manager (WAL) record describers. |
|  |  |  | sequence |  | SEQUENCE relation operations. |
|  |  |  | [spgist](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/README) |  | Space-partitioned GiST access method. |
|  |  |  | table |  | Table access method layer. |
|  |  |  | tablesample |  | TABLESAMPLE methods (e.g., bernoulli, system). |
|  |  |  | [transam](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/README) |  | Transaction/logging subsystems (xlog, clog, multixact, …). |
|  |  | archive |  |  | Server-side WAL archiving integration points. |
|  |  | backup |  |  | Base backup server code (including compression/manifest/walsummary). |
|  |  | bootstrap |  |  | Bootstrap code for initial catalog creation. |
|  |  | catalog |  |  | System catalogs and helpers (DDL, dependencies, namespace). |
|  |  | commands |  |  | SQL command implementations (ALTER, CREATE, COPY, VACUUM, …). |
|  |  | [executor](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/README) |  |  | Query executor (plan node runners, tuple machinery). |
|  |  | foreign |  |  | Foreign Data Wrapper core and callbacks. |
|  |  | [jit](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/README) |  |  | JIT integration for expressions/queries. |
|  |  |  | llvm |  | LLVM-based JIT backend. |
|  |  | [lib](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/README) |  |  | Server-only support libraries (containers, algos, etc.). |
|  |  | [libpq](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/README.SSL) |  |  | Backend-side libpq protocol support. |
|  |  | main |  |  | Backend entry point and main loop. |
|  |  | [nodes](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/README) |  |  | Parse/plan/exec tree nodes and support. |
|  |  | [optimizer](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/README) |  |  | Planner/optimizer (paths, plans, GEQO, prep, utils). |
|  |  |  | geqo |  | Genetic query optimizer components. |
|  |  |  | path |  | Path finding and join path construction. |
|  |  |  | [plan](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/README) |  | Plan creation from paths. |
|  |  |  | prep |  | Planner preprocessing (subqueries, equivalence classes). |
|  |  |  | util |  | Optimizer utilities and costing helpers. |
|  |  | [parser](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/README) |  |  | SQL grammar, parser, and lexer. |
|  |  | partitioning |  |  | Declarative partitioning analysis and pruning. |
|  |  | po |  |  | Backend translations. |
|  |  | port |  |  | Backend portability (SysV/Win32 shmem/semaphores, atomics). |
|  |  |  | tas |  | Test-and-set primitives (platform-specific). |
|  |  |  | win32 |  | Windows-specific backend port code. |
|  |  | postmaster |  |  | Postmaster and process control (bgworkers, archiver, writer). |
|  |  | [regex](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/README) |  |  | Regular expression engine (Henry Spencer). |
|  |  | [replication](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/README) |  |  | Streaming/logical replication, senders/receivers. |
|  |  |  | libpqwalreceiver |  | WAL receiver library using libpq. |
|  |  |  | logical |  | Logical replication (apply worker, reorder buffer, snapshot build). |
|  |  |  | pgoutput |  | Output plugin for logical replication. |
|  |  | rewrite |  |  | Query rewrite system and RLS support. |
|  |  | [snowball](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/README) |  |  | Snowball-based stemming for full-text search. |
|  |  |  | libstemmer |  | Embedded Snowball libstemmer sources. |
|  |  |  | stopwords |  | Stopword lists for TSearch. |
|  |  | [statistics](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/README) |  |  | Extended statistics and MCV lists. |
|  |  | storage |  |  | Storage managers, buffers, WAL I/O, IPC, locks. |
|  |  |  | aio |  | Asynchronous I/O helpers. |
|  |  |  | [buffer](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/README) |  | Shared/local buffer manager. |
|  |  |  | file |  | File descriptors, temporary files, copydir. |
|  |  |  | [freespace](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/README) |  | Free space map (FSM). |
|  |  |  | ipc |  | Inter-process communication (shmem, DSM, latches, sinval). |
|  |  |  | large_object |  | Large object storage API. |
|  |  |  | [lmgr](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/README) |  | Lock manager (locks, LWLocks, deadlock, predicates). |
|  |  |  | [page](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/README) |  | Page layout, checksums, item pointers. |
|  |  |  | [smgr](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/README) |  | Storage manager abstraction (md, etc.). |
|  |  |  | sync |  | File sync scheduling. |
|  |  | tcop |  |  | Statement processing and protocol messaging. |
|  |  | tsearch |  |  | Full-text search core (dicts, parsers, ts*). |
|  |  |  | dicts |  | Sample dictionaries for tsearch. |
|  |  | utils |  |  | Backend utilities (GUC, error, caches, memory, encodings). |
|  |  |  | activity |  | Activity statistics helpers. |
|  |  |  | adt |  | Built-in data type functions. |
|  |  |  | cache |  | System cache and relcache. |
|  |  |  | error |  | Error reporting and SQLSTATE mappings. |
|  |  |  | [fmgr](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/README) |  | Function manager (fmgr) glue. |
|  |  |  | hash |  | Hashing utilities. |
|  |  |  | init |  | Backend initialization. |
|  |  |  | [mb](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/README) |  | Multi-byte encodings support. |
|  |  |  |  | Unicode | Unicode mapping data and tables. |
|  |  |  |  | conversion_procs | Encoding conversion procedures. |
|  |  |  | [misc](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/README) |  | Miscellaneous helpers. |
|  |  |  | [mmgr](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/README) |  | Memory context manager. |
|  |  |  | [resowner](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/README) |  | Resource owner tracking. |
|  |  |  | sort |  | External sort/tuplesort. |
|  |  |  | time |  | Time/date utilities. |
|  | bin |  |  |  | Client and maintenance utilities; each subdir builds one program unless noted. |
|  |  | initdb |  |  | Initializes a new data directory (cluster). |
|  |  |  | po |  | Translations for initdb. |
|  |  | [pg_amcheck](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/README) |  |  | Integrity checks for access methods and relations. |
|  |  |  | po |  | Translations for pg_amcheck. |
|  |  | pg_archivecleanup |  |  | Cleans up old WAL files from an archive. |
|  |  |  | po |  | Translations for pg_archivecleanup. |
|  |  | pg_basebackup |  |  | Base backup/receivewal/recvlogical client utilities. |
|  |  |  | po |  | Translations for pg_basebackup suite. |
|  |  | pg_checksums |  |  | Enable/disable/verify data file checksums. |
|  |  |  | po |  | Translations for pg_checksums. |
|  |  | pg_combinebackup |  |  | Combine incremental backups into a full backup. |
|  |  |  | po |  | Translations for pg_combinebackup. |
|  |  | pg_config |  |  | Print build-time configuration of PostgreSQL. |
|  |  |  | po |  | Translations for pg_config. |
|  |  | pg_controldata |  |  | Show control file contents. |
|  |  |  | po |  | Translations for pg_controldata. |
|  |  | pg_ctl |  |  | Control postmaster: start/stop/restart/status. |
|  |  |  | po |  | Translations for pg_ctl. |
|  |  | pg_dump |  |  | Logical backup (`pg_dump`, `pg_dumpall`, `pg_restore`). |
|  |  |  | po |  | Translations for pg_dump tools. |
|  |  | pg_resetwal |  |  | Reset write-ahead log (emergency). |
|  |  |  | po |  | Translations for pg_resetwal. |
|  |  | pg_rewind |  |  | Rewind a diverged cluster to a common timeline. |
|  |  |  | po |  | Translations for pg_rewind. |
|  |  | pg_test_fsync |  |  | Benchmark fsync performance. |
|  |  |  | po |  | Translations for pg_test_fsync. |
|  |  | pg_test_timing |  |  | Measure timing overhead/precision. |
|  |  |  | po |  | Translations for pg_test_timing. |
|  |  | pg_upgrade |  |  | In-place major version upgrades via file reuse. |
|  |  |  | po |  | Translations for pg_upgrade. |
|  |  | pg_verifybackup |  |  | Verify backup manifest and integrity. |
|  |  |  | po |  | Translations for pg_verifybackup. |
|  |  | pg_waldump |  |  | Dump WAL records for inspection. |
|  |  |  | po |  | Translations for pg_waldump. |
|  |  | pg_walsummary |  |  | Generate summaries of WAL activity. |
|  |  |  | po |  | Translations for pg_walsummary. |
|  |  | pgbench |  |  | Benchmarking/load generator. |
|  |  | [pgevent](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgevent/README) |  |  | Windows event log support library. |
|  |  | psql |  |  | Interactive SQL shell. |
|  |  |  | po |  | Translations for psql. |
|  |  | scripts |  |  | Client helper scripts (createdb, vacuumdb, etc.). |
|  |  |  | po |  | Translations for client scripts. |
|  | common |  |  |  | Shared C code used by both client and server (string, compression, crypto, file utils). |
|  |  | [unicode](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode/README) |  |  | Unicode tables and normalization/case handling data. |
|  | fe_utils |  |  |  | Frontend utilities shared across client programs (connection, printing, options). |
|  | include |  |  |  | Public and internal C headers. Mirrors many subtrees. |
|  |  | access |  |  | Access method headers. |
|  |  | archive |  |  | WAL archiving headers. |
|  |  | backup |  |  | Base backup/WAL summary headers. |
|  |  | bootstrap |  |  | Bootstrap-related headers. |
|  |  | catalog |  |  | System catalog headers. |
|  |  | commands |  |  | Command implementation headers. |
|  |  | common |  |  | Shared frontend/backend headers. |
|  |  | datatype |  |  | Data type headers. |
|  |  | executor |  |  | Executor headers. |
|  |  | fe_utils |  |  | Frontend utilities headers. |
|  |  | foreign |  |  | FDW headers. |
|  |  | jit |  |  | JIT headers. |
|  |  | lib |  |  | Backend library headers. |
|  |  | libpq |  |  | libpq protocol/client headers. |
|  |  | mb |  |  | Multi-byte/encoding headers. |
|  |  | nodes |  |  | Node structure headers. |
|  |  | optimizer |  |  | Planner/optimizer headers. |
|  |  | parser |  |  | Parser headers. |
|  |  | partitioning |  |  | Partitioning headers. |
|  |  | pch |  |  | Precompiled header configuration. |
|  |  | port |  |  | Portability (frontend) headers. |
|  |  | portability |  |  | Additional portability headers. |
|  |  | postmaster |  |  | Postmaster headers. |
|  |  | regex |  |  | Regex engine headers. |
|  |  | replication |  |  | Replication headers. |
|  |  | rewrite |  |  | Rewrite system headers. |
|  |  | snowball |  |  | Snowball headers. |
|  |  | statistics |  |  | Extended statistics headers. |
|  |  | storage |  |  | Storage and buffer/WAL headers. |
|  |  | tcop |  |  | Statement processing headers. |
|  |  | tsearch |  |  | Full-text search headers. |
|  |  | utils |  |  | Backend utility headers. |
|  | interfaces |  |  |  | Client APIs and language bindings. |
|  |  | [ecpg](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/README.dynSQL) |  |  | Embedded C preprocessor and libraries. |
|  |  |  | compatlib |  | Compatibility support library. |
|  |  |  | ecpglib |  | ECPG runtime client library. |
|  |  |  | include |  | ECPG headers. |
|  |  |  | pgtypeslib |  | ECPG type library. |
|  |  |  | preproc |  | ECPG preprocessor. |
|  |  |  | test |  | Tests for ECPG. |
|  |  | [libpq](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/README) |  |  | C client library (libpq). |
|  |  |  | po |  | libpq translations. |
|  |  |  | test |  | libpq client tests. |
|  | makefiles |  |  |  | Makefile fragments used throughout the build. |
|  | pl |  |  |  | Procedural languages and glue code. |
|  |  | [plperl](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/README) |  |  | PL/Perl language handler. |
|  |  |  | expected |  | Expected outputs for tests. |
|  |  |  | po |  | Translations for PL/Perl. |
|  |  |  | sql |  | SQL test scripts. |
|  |  | plpgsql |  |  | PL/pgSQL language handler. |
|  |  |  | src |  | PL/pgSQL sources. |
|  |  | plpython |  |  | PL/Python language handler. |
|  |  |  | expected |  | Expected outputs for tests. |
|  |  |  | po |  | Translations for PL/Python. |
|  |  |  | sql |  | SQL test scripts. |
|  |  | tcl |  |  | PL/Tcl language handler. |
|  |  |  | expected |  | Expected outputs for tests. |
|  |  |  | po |  | Translations for PL/Tcl. |
|  |  |  | sql |  | SQL test scripts. |
|  | [port](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/README) |  |  |  | Portability shims and platform-specific support code (frontend). |
|  | template |  |  |  | Build templates and skeleton files (per-OS toolchains). |
|  |  | cygwin |  |  | Cygwin-specific template files. |
|  |  | darwin |  |  | macOS-specific template files. |
|  |  | freebsd |  |  | FreeBSD-specific template files. |
|  |  | linux |  |  | Linux-specific template files. |
|  |  | netbsd |  |  | NetBSD-specific template files. |
|  |  | openbsd |  |  | OpenBSD-specific template files. |
|  |  | solaris |  |  | Solaris-specific template files. |
|  |  | win32 |  |  | Windows-specific template files. |
|  | [test](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/README) |  |  |  | Test suites and harnesses (TAP/regression/ISOLATION, etc.). |
|  |  | [authentication](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/authentication/README) |  |  | Auth-related integration tests. |
|  |  | examples |  |  | Example extension/tests. |
|  |  | [icu](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/icu/README) |  |  | ICU-related tests. |
|  |  | [isolation](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/README) |  |  | Serializable/locking/isolation tests. |
|  |  | [kerberos](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/kerberos/README) |  |  | Kerberos integration tests. |
|  |  | [ldap](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/ldap/README) |  |  | LDAP integration tests. |
|  |  | [locale](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/locale/README) |  |  | Locale/collation tests. |
|  |  | [mb](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/mb/README) |  |  | Multi-byte/encoding tests. |
|  |  | [modules](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/README) |  |  | Tests for contrib-like modules. |
|  |  | [perl](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/perl/README) |  |  | Perl-related tests. |
|  |  | [recovery](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/recovery/README) |  |  | Crash/replication/recovery tests. |
|  |  | [regress](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/README) |  |  | Core SQL regression tests. |
|  |  | [ssl](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/ssl/README) |  |  | SSL/TLS tests. |
|  |  | [subscription](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/subscription/README) |  |  | Logical replication subscription tests. |
|  | [timezone](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/README) |  |  |  | Time zone data and conversion code. |
|  |  | data |  |  | Time zone source data. |
|  |  | [tznames](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/tznames/README) |  |  | Time zone name tables. |
|  | tools |  |  |  | Developer tools, scripts, and generators used during build/dev. |
|  |  | [ci](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/ci/README) |  |  | CI scripts and helpers. |
|  |  | editors |  |  | Editor integration/configuration helpers. |
|  |  | [ifaddrs](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/ifaddrs/README) |  |  | Ifaddrs compatibility sources. |
|  |  | perlcheck |  |  | Perl lint/check scripts. |
|  |  | [pg_bsd_indent](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/README) |  |  | BSD indent tool and rules. |
|  |  | [pginclude](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pginclude/README) |  |  | Include/header checking scripts. |
|  |  | [pgindent](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pgindent/README) |  |  | Code formatter configuration and scripts. |
|  | [tutorial](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/README) |  |  |  | Example programs and tutorial code. |

### Level-6 drill-down: backend/storage/ipc

The main table above focuses on directories. This drill-down adds a Level-6 view for `src/backend/storage/ipc` by listing major modules (files/components) as Level-6 entries to show finer-grained responsibilities.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | storage | ipc |  | barrier | Lightweight barriers for synchronization primitives. |
|  |  |  |  |  | dsm | Dynamic shared memory (DSM) management APIs. |
|  |  |  |  |  | dsm_impl | Platform-specific DSM implementation details. |
|  |  |  |  |  | dsm_registry | Registry/tracking of DSM segments. |
|  |  |  |  |  | ipc | IPC initialization and helpers. |
|  |  |  |  |  | ipci | Postmaster-time shared memory initialization. |
|  |  |  |  |  | latch | Latch primitives for wakeups and waits. |
|  |  |  |  |  | pmsignal | Signals between postmaster and backends. |
|  |  |  |  |  | procarray | Process array tracking and visibility snapshots. |
|  |  |  |  |  | procsignal | Backend-to-backend signaling helpers. |
|  |  |  |  |  | shm_mq | Shared-memory message queues. |
|  |  |  |  |  | shm_toc | Shared-memory table-of-contents structure. |
|  |  |  |  |  | shmem | Core shared memory allocation and management. |
|  |  |  |  |  | signalfuncs | SQL-callable signal helpers. |
|  |  |  |  |  | sinval | Shared-invalidation messaging (catalog relcache). |
|  |  |  |  |  | sinvaladt | Shared-invalidation SLRU backing store. |
|  |  |  |  |  | standby | Standby-side conflict resolution and recovery hooks. |

### Level-6 drill-down: backend/storage/lmgr

A finer-grained look at `src/backend/storage/lmgr` modules (files) to show locking primitives and related process coordination components.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | storage | lmgr |  | condition_variable | Condition variable primitives used with latches/LWLocks. |
|  |  |  |  |  | deadlock | Deadlock detection, wait graph analysis, and reporting. |
|  |  |  |  |  | lmgr | Lock manager glue and shared definitions. |
|  |  |  |  |  | lock | Heavyweight lock manager (relation/transaction locks). |
|  |  |  |  |  | lwlock | Lightweight locks for shared memory structures. |
|  |  |  |  |  | predicate | SSI predicate locking system (serializable isolation). |
|  |  |  |  |  | proc | Per-backend lock state and wait queues. |
|  |  |  |  |  | s_lock | Spinlock primitive wrappers. |
|  |  |  |  |  | spin | Spinlock fallback and debugging support. |

### Level-6 drill-down: backend/storage/buffer

Modules within `src/backend/storage/buffer` that implement the shared buffer cache.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | storage | buffer |  | buf_init | Buffer manager initialization paths. |
|  |  |  |  |  | buf_table | Mapping from rel/block to buffer IDs (shared hash). |
|  |  |  |  |  | bufmgr | Core buffer manager operations (pin, read, flush, evict). |
|  |  |  |  |  | freelist | Free list management for buffer replacement. |
|  |  |  |  |  | localbuf | Local buffers (per-backend, e.g., for temp relations). |

### Level-6 drill-down: backend/storage/smgr

Storage manager abstraction modules in `src/backend/storage/smgr`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | storage | smgr |  | bulk_write | Batched/bulk write helpers for relation files. |
|  |  |  |  |  | md | The md (magnetic disk) storage manager implementation. |
|  |  |  |  |  | smgr | Storage manager dispatch and API glue. |

### Level-6 drill-down: backend/access/transam

Core transaction and WAL subsystems in `src/backend/access/transam`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | access | transam |  | clog | Transaction commit status (CLOG/PG_XACT). |
|  |  |  |  |  | commit_ts | Commit timestamp tracking. |
|  |  |  |  |  | generic_xlog | Generic WAL record facility. |
|  |  |  |  |  | multixact | MultiXact (shared row locks) subsystem. |
|  |  |  |  |  | parallel | Parallel worker coordination helpers. |
|  |  |  |  |  | rmgr | Resource managers registry. |
|  |  |  |  |  | slru | Simple LRU (SLRU) shared storage. |
|  |  |  |  |  | subtrans | Subtransaction tracking. |
|  |  |  |  |  | timeline | Timeline management and history. |
|  |  |  |  |  | transam | Transaction system glue and globals. |
|  |  |  |  |  | twophase | Two-phase commit core. |
|  |  |  |  |  | twophase_rmgr | Resource managers for two-phase. |
|  |  |  |  |  | varsup | OID/transaction ID allocation support. |
|  |  |  |  |  | xact | Transaction state machine. |
|  |  |  |  |  | xlog | WAL writer, flush, and control. |
|  |  |  |  |  | xlogarchive | WAL archiving helpers. |
|  |  |  |  |  | xlogbackup | WAL base backup helpers. |
|  |  |  |  |  | xlogfuncs | SQL-callable WAL/LSN functions. |
|  |  |  |  |  | xloginsert | WAL insertion/path. |
|  |  |  |  |  | xlogprefetcher | Prefetching for WAL replay. |
|  |  |  |  |  | xlogreader | WAL record reader. |
|  |  |  |  |  | xlogrecovery | WAL recovery logic. |
|  |  |  |  |  | xlogstats | Statistics and analysis of WAL. |
|  |  |  |  |  | xlogutils | Utility helpers for WAL operations. |

### Level-6 drill-down: backend/access/heap

Heap table access method modules in `src/backend/access/heap`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | access | heap |  | heapam | Heap AM core (scan, insert, update, delete). |
|  |  |  |  |  | heapam_handler | Handler for heap table access method. |
|  |  |  |  |  | heapam_visibility | MVCC visibility for heap tuples. |
|  |  |  |  |  | heaptoast | TOAST support for large varlena attrs. |
|  |  |  |  |  | hio | Heap page I/O helpers. |
|  |  |  |  |  | pruneheap | Page pruning and HOT chain cleanup. |
|  |  |  |  |  | rewriteheap | Rewrite operations (CLUSTER/VACUUM FULL). |
|  |  |  |  |  | vacuumlazy | Lazy vacuum implementation. |
|  |  |  |  |  | visibilitymap | Visibility Map (VM) maintenance. |

### Level-6 drill-down: backend/optimizer/path

Pathfinding and costing modules in `src/backend/optimizer/path`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | optimizer | path |  | allpaths | Entry points to path generation for queries. |
|  |  |  |  |  | clausesel | Clause selectivity estimation. |
|  |  |  |  |  | costsize | Costing of scans/joins/sorts and sizes. |
|  |  |  |  |  | equivclass | Equivalence classes for pathkeys/join planning. |
|  |  |  |  |  | indxpath | Index path generation. |
|  |  |  |  |  | joinpath | Join path generation. |
|  |  |  |  |  | joinrels | Join relation building. |
|  |  |  |  |  | pathkeys | Path key ordering and comparisons. |
|  |  |  |  |  | tidpath | TID scan path generation. |

### Level-6 drill-down: backend/storage/page

Page layout and verification modules in `src/backend/storage/page`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | storage | page |  | bufpage | Buffer/page layout helpers and item operations. |
|  |  |  |  |  | checksum | Data page checksum computation/verification. |
|  |  |  |  |  | itemptr | ItemPointer helpers (ctid, block/offset). |

### Level-6 drill-down: backend/storage/file

File and temporary storage modules in `src/backend/storage/file`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | storage | file |  | buffile | Buffered read/write files with spilling. |
|  |  |  |  |  | copydir | Directory copy utilities (relocation/backup). |
|  |  |  |  |  | fd | Virtual file descriptor management and VFD cache. |
|  |  |  |  |  | fileset | Shared temporary filesets across processes. |
|  |  |  |  |  | reinit | Reinitialize temporary files on startup. |
|  |  |  |  |  | sharedfileset | Shared temp fileset manager. |

### Level-6 drill-down: backend/executor

Executor modules in `src/backend/executor` (plan node runners and tuple machinery).

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | executor |  |  | execAmi | Access methods interface and scanning helpers. |
|  |  |  |  |  | execAsync | Asynchronous execution support. |
|  |  |  |  |  | execCurrent | CURRENT OF and related cursor semantics. |
|  |  |  |  |  | execExpr | Expression evaluation engine. |
|  |  |  |  |  | execExprInterp | Interpreted expression executor. |
|  |  |  |  |  | execGrouping | Grouping sets and hash aggregate helpers. |
|  |  |  |  |  | execIndexing | Index insert/update/delete from executor. |
|  |  |  |  |  | execJunk | Junk columns handling. |
|  |  |  |  |  | execMain | Top-level executor control flow. |
|  |  |  |  |  | execParallel | Parallel query execution utilities. |
|  |  |  |  |  | execPartition | Partitioned table execution helpers. |
|  |  |  |  |  | execProcnode | Per-node execution dispatch. |
|  |  |  |  |  | execReplication | Logical replication execution hooks. |
|  |  |  |  |  | execSRF | Set-returning functions execution. |
|  |  |  |  |  | execScan | Common scan framework. |
|  |  |  |  |  | execTuples | Tuple slots and tuple table. |
|  |  |  |  |  | execUtils | Utility helpers used by executor. |
|  |  |  |  |  | functions | SPI-callable functions and support. |
|  |  |  |  |  | instrument | Execution instrumentation and EXPLAIN support. |
|  |  |  |  |  | nodeAgg | Aggregation plan node. |
|  |  |  |  |  | nodeAppend | Append plan node. |
|  |  |  |  |  | nodeBitmapAnd | Bitmap AND node. |
|  |  |  |  |  | nodeBitmapHeapscan | Bitmap Heap Scan node. |
|  |  |  |  |  | nodeBitmapIndexscan | Bitmap Index Scan node. |
|  |  |  |  |  | nodeBitmapOr | Bitmap OR node. |
|  |  |  |  |  | nodeCtescan | CTE Scan node. |
|  |  |  |  |  | nodeCustom | Custom scan node. |
|  |  |  |  |  | nodeForeignscan | Foreign Scan node. |
|  |  |  |  |  | nodeFunctionscan | Function Scan node. |
|  |  |  |  |  | nodeGather | Gather node. |
|  |  |  |  |  | nodeGatherMerge | Gather Merge node. |
|  |  |  |  |  | nodeGroup | Group node. |
|  |  |  |  |  | nodeHash | Hash node. |
|  |  |  |  |  | nodeHashjoin | Hash Join node. |
|  |  |  |  |  | nodeIncrementalSort | Incremental Sort node. |
|  |  |  |  |  | nodeIndexonlyscan | Index Only Scan node. |
|  |  |  |  |  | nodeIndexscan | Index Scan node. |
|  |  |  |  |  | nodeLimit | Limit node. |
|  |  |  |  |  | nodeLockRows | LockRows node. |
|  |  |  |  |  | nodeMaterial | Materialize node. |
|  |  |  |  |  | nodeMemoize | Memoize node. |
|  |  |  |  |  | nodeMergeAppend | Merge Append node. |
|  |  |  |  |  | nodeMergejoin | Merge Join node. |
|  |  |  |  |  | nodeModifyTable | ModifyTable (INSERT/UPDATE/DELETE) node. |
|  |  |  |  |  | nodeNamedtuplestorescan | NamedTuplestoreScan node. |
|  |  |  |  |  | nodeNestloop | Nested Loop node. |
|  |  |  |  |  | nodeProjectSet | ProjectSet node. |
|  |  |  |  |  | nodeRecursiveunion | Recursive Union node. |
|  |  |  |  |  | nodeResult | Result node. |
|  |  |  |  |  | nodeSamplescan | Sample Scan node. |
|  |  |  |  |  | nodeSeqscan | Seq Scan node. |
|  |  |  |  |  | nodeSetOp | SetOp node. |
|  |  |  |  |  | nodeSort | Sort node. |
|  |  |  |  |  | nodeSubplan | SubPlan node. |
|  |  |  |  |  | nodeSubqueryscan | Subquery Scan node. |
|  |  |  |  |  | nodeTableFuncscan | TableFunc Scan node. |
|  |  |  |  |  | nodeTidrangescan | TidRange Scan node. |
|  |  |  |  |  | nodeTidscan | TID Scan node. |
|  |  |  |  |  | nodeUnique | Unique node. |
|  |  |  |  |  | nodeValuesscan | Values Scan node. |
|  |  |  |  |  | nodeWindowAgg | WindowAgg node. |
|  |  |  |  |  | nodeWorktablescan | WorkTable Scan node. |
|  |  |  |  |  | spi | Server Programming Interface helpers. |
|  |  |  |  |  | tqueue | Tuple queue interprocess transport. |
|  |  |  |  |  | tstoreReceiver | Tuple store receiver. |

### Level-6 drill-down: backend/nodes

Node support modules in `src/backend/nodes` (parse/plan/exec node structures and utilities).

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | nodes |  |  | bitmapset | Bitmapset operations. |
|  |  |  |  |  | copyfuncs | Deep-copy implementations for nodes. |
|  |  |  |  |  | equalfuncs | Structural equality checks for nodes. |
|  |  |  |  |  | extensible | Extensible node support. |
|  |  |  |  |  | list | Single/doubly-linked list utilities. |
|  |  |  |  |  | makefuncs | Node construction helpers. |
|  |  |  |  |  | multibitmapset | Multi-bitmapset ops. |
|  |  |  |  |  | nodeFuncs | Node tree walkers and utilities. |
|  |  |  |  |  | outfuncs | Node-to-text serialization. |
|  |  |  |  |  | params | Param node support. |
|  |  |  |  |  | print | Node pretty-printer for debugging. |
|  |  |  |  |  | queryjumblefuncs | Query jumble for normalized fingerprinting. |
|  |  |  |  |  | read | Lexer for node text representation. |
|  |  |  |  |  | readfuncs | Parsing of node text representation. |
|  |  |  |  |  | tidbitmap | TID bitmap implementation. |
|  |  |  |  |  | value | Node value constructors (String, Integer, etc.). |

### Level-6 drill-down: backend/storage/aio

Asynchronous I/O modules in `src/backend/storage/aio`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | storage | aio |  | read_stream | Helpers for reading WAL/relations with async I/O. |

### Level-6 drill-down: backend/storage/freespace

Free Space Map (FSM) modules in `src/backend/storage/freespace`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | storage | freespace |  | freespace | FSM lookup and maintenance. |
|  |  |  |  |  | fsmpage | FSM page layout and operations. |
|  |  |  |  |  | indexfsm | Index FSM helpers. |

### Level-6 drill-down: backend/parser

SQL grammar and parsing modules in `src/backend/parser`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | parser |  |  | analyze | Parse analysis and query rewriting entry points. |
|  |  |  |  |  | parse_agg | Aggregate function parsing. |
|  |  |  |  |  | parse_clause | FROM/WHERE/GROUP/ORDER clause parsing. |
|  |  |  |  |  | parse_coerce | Type coercion and casting. |
|  |  |  |  |  | parse_collate | COLLATE clause processing. |
|  |  |  |  |  | parse_cte | Common Table Expressions. |
|  |  |  |  |  | parse_enr | Enr (ENR) relations in parsing. |
|  |  |  |  |  | parse_expr | Expression parsing. |
|  |  |  |  |  | parse_func | Function call parsing. |
|  |  |  |  |  | parse_jsontable | JSON_TABLE support. |
|  |  |  |  |  | parse_merge | MERGE command parsing. |
|  |  |  |  |  | parse_node | Node creation during parsing. |
|  |  |  |  |  | parse_oper | Operator lookup and parsing. |
|  |  |  |  |  | parse_param | Param nodes and $n parameters. |
|  |  |  |  |  | parse_relation | RangeVar/relations in parsing. |
|  |  |  |  |  | parse_target | Target list parsing. |
|  |  |  |  |  | parse_type | Type name parsing and resolution. |
|  |  |  |  |  | parse_utilcmd | Utility command parsing helpers. |
|  |  |  |  |  | parser | Parser driver. |
|  |  |  |  |  | scansup | Scanner helpers. |

### Level-6 drill-down: backend/storage/sync

File synchronization scheduling in `src/backend/storage/sync`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | storage | sync |  | sync | File sync scheduling (checkpoint/flush strategy). |

### Level-6 drill-down: backend/foreign

Foreign Data Wrapper core in `src/backend/foreign`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | foreign |  |  | foreign | FDW core APIs and hooks. |

### Level-6 drill-down: backend/tsearch

Full-text search core in `src/backend/tsearch`.

| Level-1 | Level-2 | Level-3 | Level-4 | Level-5 | Level-6 (module) | Purpose / What’s here |
|---|---|---|---|---|---|---|
| src | backend | tsearch |  |  | dict | Dictionary API core. |
|  |  |  |  |  | dict_ispell | Ispell dictionary. |
|  |  |  |  |  | dict_simple | Simple dictionary. |
|  |  |  |  |  | dict_synonym | Synonym dictionary. |
|  |  |  |  |  | dict_thesaurus | Thesaurus dictionary. |
|  |  |  |  |  | regis | Regex index support. |
|  |  |  |  |  | spell | Spelling support. |
|  |  |  |  |  | to_tsany | TS ANY conversions. |
|  |  |  |  |  | ts_locale | Locale support for tsearch. |
|  |  |  |  |  | ts_parse | Text parser core. |
|  |  |  |  |  | ts_selfuncs | Planner selectivity for tsearch ops. |
|  |  |  |  |  | ts_typanalyze | Stats collection for tsearch types. |
|  |  |  |  |  | ts_utils | TSearch utilities. |
|  |  |  |  |  | wparser | Word parser core. |
|  |  |  |  |  | wparser_def | Default word parser. |

## Notes

- The table focuses on directories rather than individual files. File-level details can be explored in each directory as needed.
- Some directories contain many nested subdirectories that mirror server components; this document highlights the most relevant layers for orientation.
