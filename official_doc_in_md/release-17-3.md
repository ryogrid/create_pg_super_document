E.3. Release 17.3  
---  
[Prev](release-17-4.md "E.2. Release 17.4") | [Up](release.md "Appendix E. Release Notes")| Appendix E. Release Notes| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](release-17-2.md "E.4. Release 17.2")  
  
* * *

## E.3. Release 17.3 #

[E.3.1. Migration to Version 17.3](release-17-3.md#RELEASE-17-3-MIGRATION)
[E.3.2. Changes](release-17-3.md#RELEASE-17-3-CHANGES)

**Release date:** 2025-02-13

This release contains a variety of fixes from 17.2. For information about new features in major release 17, see [Section E.6](release-17.md "E.6. Release 17"). 

### E.3.1. Migration to Version 17.3 #

A dump/restore is not required for those running 17.X. 

However, if you are upgrading from a version earlier than 17.1, see [Section E.5](release-17-1.md "E.5. Release 17.1"). 

### E.3.2. Changes #

  * Harden `PQescapeString` and allied functions against invalidly-encoded input strings (Andres Freund, Noah Misch) [§](https://postgr.es/c/43a77239d) [§](https://postgr.es/c/7d43ca6fe) [§](https://postgr.es/c/61ad93cdd) [§](https://postgr.es/c/02d4d87ac) [§](https://postgr.es/c/05abb0f83) [§](https://postgr.es/c/85c1fcc65)

Data-quoting functions supplied by libpq now fully check the encoding validity of their input. If invalid characters are detected, they report an error if possible. For the ones that lack an error return convention, the output string is adjusted to ensure that the server will report invalid encoding and no intervening processing will be fooled by bytes that might happen to match single quote, backslash, etc. 

The purpose of this change is to guard against SQL-injection attacks that are possible if one of these functions is used to quote crafted input. There is no hazard when the resulting string is sent directly to a PostgreSQL server (which would check its encoding anyway), but there is a risk when it is passed through psql or other client-side code. Historically such code has not carefully vetted encoding, and in many cases it's not clear what it should do if it did detect such a problem. 

This fix is effective only if the data-quoting function, the server, and any intermediate processing agree on the character encoding that's being used. Applications that insert untrusted input into SQL commands should take special care to ensure that that's true. 

Applications and drivers that quote untrusted input without using these libpq functions may be at risk of similar problems. They should first confirm the data is valid in the encoding expected by the server. 

The PostgreSQL Project thanks Stephen Fewer for reporting this problem. (CVE-2025-1094) 

  * Restore auto-truncation of database and user names appearing in connection requests (Nathan Bossart) [§](https://postgr.es/c/d09fbf645)

This reverts a v17 change that proved to cause trouble for some users. Over-length names should be truncated in an encoding-aware fashion, but for now just return to the former behavior of blind truncation at `NAMEDATALEN-1` bytes. 

  * Exclude parallel workers from connection privilege checks and limits (Tom Lane) [§](https://postgr.es/c/15b4c46c3)

Do not check `datallowconn`, `rolcanlogin`, and `ACL_CONNECT` privileges when starting a parallel worker, instead assuming that it's enough for the leader process to have passed similar checks originally. This avoids, for example, unexpected failures of parallelized queries when the leader is running as a role that lacks login privilege. In the same vein, enforce `ReservedConnections`, `datconnlimit`, and `rolconnlimit` limits only against regular backends, and count only regular backends while checking if the limits were already reached. Those limits are meant to prevent excessive consumption of process slots for regular backends --- but parallel workers and other special processes have their own pools of process slots with their own limit checks. 

  * Drop “Lock” suffix from LWLock wait event names (Bertrand Drouvot) [§](https://postgr.es/c/5ffbbcfa1)

Refactoring unintentionally caused the `pg_stat_activity` view to show lock-related wait event names with a “Lock” suffix, which among other things broke joining it to `pg_wait_events`. 

  * Fix possible failure to return all matching tuples for a btree index scan with a ScalarArrayOp (`= ANY`) condition (Peter Geoghegan) [§](https://postgr.es/c/9e85b20da)

  * Fix possible re-use of stale results in window aggregates (David Rowley) [§](https://postgr.es/c/9d5ce4f1a)

A window aggregate with a “run condition” optimization and a pass-by-reference result type might incorrectly return the result from the previous partition instead of performing a fresh calculation. 

  * Keep `TransactionXmin` in sync with `MyProc->xmin` (Heikki Linnakangas) [§](https://postgr.es/c/7cfdb4d1e)

This oversight could permit a process to try to access data that had already been vacuumed away. One known consequence is transient “could not access status of transaction” errors. 

  * Fix race condition that could cause failure to add a newly-inserted catalog entry to a catalog cache list (Heikki Linnakangas) [§](https://postgr.es/c/96e61b279)

This could result, for example, in failure to use a newly-created function within an existing session. 

  * Prevent possible catalog corruption when a system catalog is vacuumed concurrently with an update (Noah Misch) [§](https://postgr.es/c/1587f7b9f) [§](https://postgr.es/c/f4af4515b)

  * Fix data corruption when relation truncation fails (Thomas Munro) [§](https://postgr.es/c/0350b876b) [§](https://postgr.es/c/66aaabe7a) [§](https://postgr.es/c/45aef9f6b)

The filesystem calls needed to perform relation truncation could fail, leaving inconsistent state on disk (for example, effectively reviving deleted data). We can't really prevent that, but we can recover by dint of making such failures into PANICs, so that consistency is restored by replaying from WAL up to just before the attempted truncation. This isn't a hugely desirable behavior, but such failures are rare enough that it seems an acceptable solution. 

  * Prevent checkpoints from starting during relation truncation (Robert Haas) [§](https://postgr.es/c/d4ffbf47b)

This avoids a race condition wherein the modified file might not get fsync'd before completing the checkpoint, creating a risk of data corruption if the operating system crashes soon after. 

  * Avoid possibly losing an update of `pg_database`.`datfrozenxid` when `VACUUM` runs concurrently with a `REASSIGN OWNED` that changes that database's owner (Kirill Reshke) [§](https://postgr.es/c/fa6131377)

  * Fix incorrect `tg_updatedcols` values passed to `AFTER UPDATE` triggers (Tom Lane) [§](https://postgr.es/c/2b72fed2d)

In some cases the `tg_updatedcols` bitmap could describe the set of columns updated by an earlier command in the same transaction, fooling the trigger into doing the wrong thing. 

Also, prevent memory bloat caused by making too many copies of the `tg_updatedcols` bitmap. 

  * Fix detach of a partition that has its own foreign-key constraint referencing a partitioned table (Amul Sul) [§](https://postgr.es/c/2f30847d1)

In common cases, foreign keys are defined on a partitioned table's top level; but if instead one is defined on a partition and references a partitioned table, and the referencing partition is detached, the relevant `pg_constraint` entries were updated incorrectly. This led to errors like “could not find ON INSERT check triggers of foreign key constraint”. 

  * Fix `pg_get_constraintdef`'s support for `NOT NULL` constraints on domains (Álvaro Herrera) [§](https://postgr.es/c/6e793582b)

  * Fix mis-processing of `to_timestamp`'s `FF _`n`_` format codes (Tom Lane) [§](https://postgr.es/c/765f76d8c)

An integer format code immediately preceding `FF _`n`_` would consume all available digits, leaving none for `FF _`n`_`. 

  * When deparsing a `PASSING` clause in a SQL/JSON query function, ensure that variable names are double-quoted when necessary (Dean Rasheed) [§](https://postgr.es/c/d037cc2af)

  * When deparsing an `XMLTABLE()` expression, ensure that XML namespace names are double-quoted when necessary (Dean Rasheed) [§](https://postgr.es/c/61b12135f)

  * Include the `ldapscheme` option in `pg_hba_file_rules()` output (Laurenz Albe) [§](https://postgr.es/c/8ed9bf0a3) [§](https://postgr.es/c/dc24c9ad5)

  * Fix planning of pre-sorted `UNION` operations for cases where the input column datatypes don't all match (David Rowley) [§](https://postgr.es/c/5db9367e5)

This error could lead to sorting data with the wrong sort operator, with consequences ranging from no visible problem to core dumps. 

  * Don't merge `UNION` operations if their column collations aren't consistent (Tom Lane) [§](https://postgr.es/c/c1ebef3c1)

Previously we ignored collations when deciding if it's safe to merge `UNION` steps into a single N-way `UNION` operation. This was arguably valid before the introduction of nondeterministic collations, but it's not anymore, since the collation in use can affect the definition of uniqueness. 

  * Prevent “wrong varnullingrels” planner errors after pulling up a subquery that's underneath an outer join (Tom Lane) [§](https://postgr.es/c/72822a99d) [§](https://postgr.es/c/78883cd90)

  * Ignore nulling-relation marker bits when looking up statistics (Richard Guo) [§](https://postgr.es/c/297b280ab)

This oversight could lead to failure to use relevant statistics about expressions, or to “corrupt MVNDistinct entry” errors. 

  * Fix missed expression processing for partition pruning steps (Tom Lane) [§](https://postgr.es/c/0671a71e0)

This oversight could lead to “unrecognized node type” errors, and perhaps other problems, in queries accessing partitioned tables. 

  * Give the slotsync worker process its own process slot (Tom Lane, Hou Zhijie) [§](https://postgr.es/c/14141bbbc)

This was overlooked in the addition of the slotsync worker, with the result that its process slot effectively came out of the pool meant for regular backend processes. This could result in failure to launch the worker, or to subsequent failures of connection requests that should have succeeded according to the configured settings, if the number of regular backend processes approached `max_connections`. 

  * Allow dshash tables to grow past 1GB (Matthias van de Meent) [§](https://postgr.es/c/18452b70a)

This avoids errors like “invalid DSA memory alloc request size”. The case can occur for example in transactions that process several million tables. 

  * Avoid possible integer overflow in `bringetbitmap()` (James Hunter, Evgeniy Gorbanyov) [§](https://postgr.es/c/e027ee990)

Since the result is only used for statistical purposes, the effects of this error were mostly cosmetic. 

  * Correct miscalculation of SLRU bank numbers (Yura Sokolov) [§](https://postgr.es/c/ffd9b8134)

This error led to using a smaller number of banks than intended, causing more contention but no functional misbehavior. 

  * Ensure that an already-set process latch doesn't prevent the postmaster from noticing socket events (Thomas Munro) [§](https://postgr.es/c/44f400fbc)

An extremely heavy workload of backends launching workers and workers exiting could prevent the postmaster from responding to incoming client connections in a timely fashion. 

  * Prevent streaming standby servers from looping infinitely when reading a WAL record that crosses pages (Kyotaro Horiguchi, Alexander Kukushkin) [§](https://postgr.es/c/e6767c0ed)

This would happen when the record's continuation is on a page that needs to be read from a different WAL source. 

  * Fix unintended promotion of FATAL errors to PANIC during early process startup (Noah Misch) [§](https://postgr.es/c/4bd9de3f4)

This fixes some unlikely cases that would result in “PANIC: proc_exit() called in child process”. 

  * Fix cases where an operator family member operator or support procedure could become a dangling reference (Tom Lane) [§](https://postgr.es/c/ec7b89cc5) [§](https://postgr.es/c/5b44a317a)

In some cases a data type could be dropped while references to its OID still remain in `pg_amop` or `pg_amproc`. While that caused no immediate issues, an attempt to drop the owning operator family would fail, and pg_dump would produce bogus output when dumping the operator family. This fix causes creation and modification of operator families/classes to add needed dependency entries so that dropping a data type will also drop any dependent operator family elements. That does not help vulnerable pre-existing operator families, though, so a band-aid has also been added to `DROP OPERATOR FAMILY` to prevent failure when dropping a family that has dangling members. 

  * Fix multiple memory leaks in logical decoding output (Vignesh C, Masahiko Sawada, Boyu Yang) [§](https://postgr.es/c/836435424) [§](https://postgr.es/c/bbe68c13a) [§](https://postgr.es/c/afe9b0d9f)

  * Fix small memory leak when updating the `application_name` or `cluster_name` settings (Tofig Aliev) [§](https://postgr.es/c/9add1bbfa)

  * Avoid crash when a background process tries to check a new value of `synchronized_standby_slots` (Álvaro Herrera) [§](https://postgr.es/c/9abdc1841)

  * Avoid integer overflow while testing `wal_skip_threshold` condition (Tom Lane) [§](https://postgr.es/c/1e25cdb21)

A transaction that created a very large relation could mistakenly decide to ensure durability by copying the relation into WAL instead of fsync'ing it, thereby negating the point of `wal_skip_threshold`. (This only matters when `wal_level` is set to `minimal`, else a WAL copy is required anyway.) 

  * Fix unsafe order of operations during cache lookups (Noah Misch) [§](https://postgr.es/c/718af10da)

The only known consequence was a usually-harmless “you don't own a lock of type ExclusiveLock” warning during `GRANT TABLESPACE`. 

  * Avoid potential use-after-free in parallel vacuum (Vallimaharajan G, John Naylor) [§](https://postgr.es/c/83ce20d67)

This bug seems to have no consequences in standard builds, but it's theoretically a hazard. 

  * Fix possible “failed to resolve name” failures when using JIT on older ARM platforms (Thomas Munro) [§](https://postgr.es/c/8a9a51518)

This could occur as a consequence of inconsistency about the default setting of `-moutline-atomics` between gcc and clang. At least Debian and Ubuntu are known to ship gcc and clang compilers that target armv8-a but differ on the use of outline atomics by default. 

  * Fix assertion failure in `WITH RECURSIVE ... UNION` queries (David Rowley) [§](https://postgr.es/c/7b8d45d27)

  * Avoid assertion failure in rule deparsing if a set operation leaf query contains set operations (Man Zeng, Tom Lane) [§](https://postgr.es/c/fea81aee8)

  * Avoid edge-case assertion failure in parallel query startup (Tom Lane) [§](https://postgr.es/c/556f7b7bc)

  * Fix assertion failure at shutdown when writing out the statistics file (Michael Paquier) [§](https://postgr.es/c/dc5f90541)

  * Avoid valgrind complaints about string hashing code (John Naylor) [§](https://postgr.es/c/6555fe197)

  * In `NULLIF()`, avoid passing a read-write expanded object pointer to the data type's equality function (Tom Lane) [§](https://postgr.es/c/97be02ad0)

The equality function could modify or delete the object if it's given a read-write pointer, which would be bad if we decide to return it as the `NULLIF()` result. There is probably no problem with any built-in equality function, but it's easy to demonstrate a failure with one coded in PL/pgSQL. 

  * Ensure that expression preprocessing is applied to a default null value in `INSERT` (Tom Lane) [§](https://postgr.es/c/6e41e9e5e)

If the target column is of a domain type, the planner must insert a coerce-to-domain step not just a null constant, and this expression missed going through some required processing steps. There is no known consequence with domains based on core data types, but in theory an error could occur with domains based on extension types. 

  * Avoid data loss when starting a bulk write on a relation fork that already contains data (Matthias van de Meent) [§](https://postgr.es/c/969583553)

Any pre-existing data was overwritten with zeroes. This is not an issue for core PostgreSQL, which never does that. Some extensions would like to, however. 

  * Avoid crash if a server process tried to iterate over a shared radix tree that it didn't create (Masahiko Sawada) [§](https://postgr.es/c/9af2b3435)

There is no code in core PostgreSQL that does this, but an extension might wish to. 

  * Repair memory leaks in PL/Python (Mat Arye, Tom Lane) [§](https://postgr.es/c/e98df02df)

Repeated use of `PLyPlan.execute` or `plpy.cursor` resulted in memory leakage for the duration of the calling PL/Python function. 

  * Fix PL/Tcl to compile with Tcl 9 (Peter Eisentraut) [§](https://postgr.es/c/f979197eb)

  * In the ecpg preprocessor, fix possible misprocessing of cursors that reference out-of-scope variables (Tom Lane) [§](https://postgr.es/c/a963abd54)

  * In ecpg, fix compile-time warnings about unsupported use of `COPY ... FROM STDIN` (Ryo Kanbayashi) [§](https://postgr.es/c/ba2dbedd5)

Previously, the intended warning was not issued due to a typo. 

  * Fix psql to safely handle file path names that are encoded in SJIS (Tom Lane) [§](https://postgr.es/c/0b713b94b)

Some two-byte characters in SJIS have a second byte that is equal to ASCII backslash (`\`). These characters were corrupted by path name normalization, preventing access to files whose names include such characters. 

  * Add psql tab completion for `COPY (MERGE INTO)` (Jian He) [§](https://postgr.es/c/4527b9e26)

  * Fix use of wrong version of `pqsignal()` in pgbench and psql (Fujii Masao, Tom Lane) [§](https://postgr.es/c/a0dfeae0d)

This error could lead to misbehavior when using the `-T` option in pgbench or the `\watch` command in psql, due to interrupted system calls not being resumed as expected. 

  * Fix misexecution of some nested `\if` constructs in pgbench (Michail Nikolaev) [§](https://postgr.es/c/ff9dc96f3)

An `\if` command appearing within a false (not-being-executed) `\if` branch was incorrectly treated the same as `\elif`. 

  * In pgbench, fix possible misdisplay of progress messages during table initialization (Yushi Ogiwara, Tatsuo Ishii, Fujii Masao) [§](https://postgr.es/c/adb103fca) [§](https://postgr.es/c/e35d396ec)

  * Make pg_controldata more robust against corrupted `pg_control` files (Ilyasov Ian, Anton Voloshin) [§](https://postgr.es/c/1b8a9533f)

Since pg_controldata will attempt to print the contents of `pg_control` even if the CRC check fails, it must take care not to misbehave for invalid field values. This patch fixes some issues triggered by invalid timestamps and apparently-negative WAL segment sizes. 

  * Fix possible crash in pg_dump with identity sequences attached to tables that are extension members (Tom Lane) [§](https://postgr.es/c/ad950ea98)

  * Fix memory leak in pg_restore with zstd-compressed data (Tom Lane) [§](https://postgr.es/c/04b860198)

The leak was per-decompression-operation, so would be most noticeable with a dump containing many tables or large objects. 

  * Fix pg_basebackup to correctly handle `pg_wal.tar` files exceeding 2GB on Windows (Davinder Singh, Thomas Munro) [§](https://postgr.es/c/faee3185a) [§](https://postgr.es/c/af109e339)

  * Use SQL-standard function bodies in the declarations of `contrib/earthdistance`'s SQL-language functions (Tom Lane, Ronan Dunklau) [§](https://postgr.es/c/3652de36e)

This change allows their references to `contrib/cube` to be resolved during extension creation, reducing the risk of search-path-based failures and possible attacks. 

In particular, this restores their usability in contexts like generated columns, for which PostgreSQL v17 restricts the search path on security grounds. We have received reports of databases failing to be upgraded to v17 because of that. This patch has been included in v16 to provide a workaround: updating the `earthdistance` extension to this version beforehand should allow an upgrade to succeed. 

  * Detect version mismatch between `contrib/pageinspect`'s SQL declarations and the underlying shared library (Tomas Vondra) [§](https://postgr.es/c/3668c1d50)

Previously, such a mismatch could result in a crash while calling `brin_page_items()`. Instead throw an error recommending updating the extension. 

  * When trying to cancel a remote query in `contrib/postgres_fdw`, re-issue the cancel request a few times if it didn't seem to do anything (Tom Lane) [§](https://postgr.es/c/89962bfef)

This fixes a race condition where we might try to cancel a just-sent query before the remote server has started to process it, so that the initial cancel request is ignored. 

  * Update configuration probes that determine the compiler switches needed to access ARM CRC instructions (Tom Lane) [§](https://postgr.es/c/e266a0ed6)

On ARM platforms where the baseline CPU target lacks CRC instructions, we need to supply a `-march` switch to persuade the compiler to compile such instructions. Recent versions of gcc reject the value we were trying, leading to silently falling back to software CRC. 

  * Fix meson build system to support old OpenSSL libraries on Windows (Darek Slusarczyk) [§](https://postgr.es/c/0951d4ee4)

Add support for the legacy library names `ssleay32` and `libeay32`. 

  * In Windows builds using meson, ensure all libcommon and libpgport functions are exported (Vladlen Popolitov, Heikki Linnakangas) [§](https://postgr.es/c/c80acbc6f) [§](https://postgr.es/c/d8b0c6411)

This fixes “unresolved external symbol” build errors for extensions. 

  * Fix meson configuration process to correctly detect OSSP's `uuid.h` header file under MSVC (Andrew Dunstan) [§](https://postgr.es/c/7c655a04a)

  * When building with meson, install `pgevent` in _`pkglibdir`_ not _`bindir`_ (Peter Eisentraut) [§](https://postgr.es/c/e00c1e249)

This matches the behavior of the make-based build system and the old MSVC build system. 

  * When building with meson, install `sepgsql.sql` under `share/contrib/` not `share/extension/` (Peter Eisentraut) [§](https://postgr.es/c/24c5b73eb)

This matches what the make-based build system does. 

  * Update time zone data files to tzdata release 2025a for DST law changes in Paraguay, plus historical corrections for the Philippines (Tom Lane) [§](https://postgr.es/c/e292ba333)




* * *

[Prev](release-17-4.md "E.2. Release 17.4") | [Up](release.md "Appendix E. Release Notes")|  [Next](release-17-2.md "E.4. Release 17.2")  
---|---|---  
E.2. Release 17.4 | [Home](index.md "PostgreSQL 17.5 Documentation")|  E.4. Release 17.2
