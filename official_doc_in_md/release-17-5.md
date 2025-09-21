E.1. Release 17.5  
---  
[Prev](release.md "Appendix E. Release Notes") | [Up](release.md "Appendix E. Release Notes")| Appendix E. Release Notes| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](release-17-4.md "E.2. Release 17.4")  
  
* * *

## E.1. Release 17.5 #

[E.1.1. Migration to Version 17.5](release-17-5.md#RELEASE-17-5-MIGRATION)
[E.1.2. Changes](release-17-5.md#RELEASE-17-5-CHANGES)

**Release date:** 2025-05-08

This release contains a variety of fixes from 17.4. For information about new features in major release 17, see [Section E.6](release-17.md "E.6. Release 17"). 

### E.1.1. Migration to Version 17.5 #

A dump/restore is not required for those running 17.X. 

However, if you have any self-referential foreign key constraints on partitioned tables, it may be necessary to recreate those constraints to ensure that they are being enforced correctly. See the second changelog entry below. 

Also, if you have any BRIN bloom indexes, it may be advisable to reindex them after updating. See the third changelog entry below. 

Also, if you are upgrading from a version earlier than 17.1, see [Section E.5](release-17-1.md "E.5. Release 17.1"). 

### E.1.2. Changes #

  * Avoid one-byte buffer overread when examining invalidly-encoded strings that are claimed to be in GB18030 encoding (Noah Misch, Andres Freund) [§](https://postgr.es/c/ec5f89e8a) [§](https://postgr.es/c/617d34908)

While unlikely, a SIGSEGV crash could occur if an incomplete multibyte character appeared at the end of memory. This was possible both in the server and in libpq-using applications. (CVE-2025-4207) 

  * Handle self-referential foreign keys on partitioned tables correctly (Álvaro Herrera) [§](https://postgr.es/c/f51ae3187)

Creating or attaching partitions failed to make the required catalog entries for a foreign-key constraint, if the table referenced by the constraint was the same partitioned table. This resulted in failure to enforce the constraint fully. 

To fix this, you should drop and recreate any self-referential foreign keys on partitioned tables, if partitions have been created or attached since the constraint was created. Bear in mind that violating rows might already be present, in which case recreating the constraint will fail, and you'll need to fix up those rows before trying again. 

  * Avoid data loss when merging compressed BRIN summaries in `brin_bloom_union()` (Tomas Vondra) [§](https://postgr.es/c/cb0ad70b8)

The code failed to account for decompression results not being identical to the input objects, which would result in failure to add some of the data to the merged summary, leading to missed rows in index searches. 

This mistake was present back to v14 where BRIN bloom indexes were introduced, but this code path was only rarely reached then. It's substantially more likely to be hit in v17 because parallel index builds now use the code. 

  * Fix unexpected “attribute has wrong type” errors in `UPDATE`, `DELETE`, and `MERGE` queries that use whole-row table references to views or functions in `FROM` (Tom Lane) [§](https://postgr.es/c/ca0830e5a)

  * Fix `MERGE` into a partitioned table with `DO NOTHING` actions (Tender Wang) [§](https://postgr.es/c/25303678a)

Some cases failed with “unknown action in MERGE WHEN clause” errors. 

  * Prevent failure in `INSERT` commands when the table has a `GENERATED` column of a domain data type and the domain's constraints disallow null values (Jian He) [§](https://postgr.es/c/3c39c000c)

Constraint failure was reported even if the generation expression produced a perfectly okay result. 

  * Correctly process references to outer CTE names that appear within a `WITH` clause attached to an `INSERT`/`UPDATE`/`DELETE`/`MERGE` command that's inside `WITH` (Tom Lane) [§](https://postgr.es/c/5e7be43f4)

The parser failed to detect disallowed recursion cases, nor did it account for such references when sorting CTEs into a usable order. 

  * Fix misprocessing of casts within the keys of JSON constructor expressions (Amit Langote) [§](https://postgr.es/c/8b2392ae3)

  * Don't try to parallelize `array_agg()` when the argument is of an anonymous record type (Richard Guo, Tom Lane) [§](https://postgr.es/c/43847dd5e)

The protocol for communicating with parallel workers doesn't support identifying the concrete record type that a worker is returning. 

  * Fix `ARRAY(_`subquery`_)` and `ARRAY[_`expression, ...`_]` constructs to produce sane results when the input is of type `int2vector` or `oidvector` (Tom Lane) [§](https://postgr.es/c/c826cd1b1)

This patch restores the behavior that existed before PostgreSQL 9.5: the result is of type `int2vector[]` or `oidvector[]`. 

  * Fix possible erroneous reports of invalid affixes while parsing Ispell dictionaries (Jacob Brazeal) [§](https://postgr.es/c/99c01aadf)

  * Fix `ALTER TABLE ADD COLUMN` to correctly handle the case of a domain type that has a default (Jian He, Tom Lane, Tender Wang) [§](https://postgr.es/c/d6dd2a02b) [§](https://postgr.es/c/0941aadcd)

If a domain type has a default, adding a column of that type (without any explicit `DEFAULT` clause) failed to install the domain's default value in existing rows, instead leaving the new column null. 

  * Repair misbehavior when there are duplicate column names in a foreign key constraint's `ON DELETE SET DEFAULT` or `SET NULL` action (Tom Lane) [§](https://postgr.es/c/5e6e97fbf)

  * Improve the error message for disallowed attempts to alter the properties of a foreign key constraint (Álvaro Herrera) [§](https://postgr.es/c/4e026be5f)

  * Avoid error when resetting the `relhassubclass` flag of a temporary table that's marked `ON COMMIT DELETE ROWS` (Noah Misch) [§](https://postgr.es/c/d0a049987)

  * Add missing deparsing of the `INDENT` option of `XMLSERIALIZE()` (Jim Jones) [§](https://postgr.es/c/2e0f93d7c) [§](https://postgr.es/c/310907aaf)

Previously, views or rules using `XMLSERIALIZE(... INDENT)` were dumped without the `INDENT` clause, causing incorrect results after restore. 

  * Avoid premature evaluation of the arguments of an aggregate function that has both `FILTER` and `ORDER BY` (or `DISTINCT`) options (David Rowley) [§](https://postgr.es/c/065ce49a1)

If there is `ORDER BY` or `DISTINCT`, we consider pre-sorting the aggregate input values rather than doing the sort within the Agg plan node. But this is problematic if the aggregate inputs include expressions that could fail (for example, a division where some of the input divisors could be zero) and there is a `FILTER` clause that's meant to prevent such failures. Pre-sorting would push the expression evaluations to before the `FILTER` test, allowing the failures to happen anyway. Avoid this by not pre-sorting if there's a `FILTER` and the input expressions are anything more complex than a simple Var or Const. 

  * Fix erroneous deductions from column `NOT NULL` constraints in the presence of outer joins (Richard Guo) [§](https://postgr.es/c/bc5a08af3)

In some cases the planner would discard an `IS NOT NULL` query condition, even though the condition applies after an outer join and thus is not redundant. 

  * Avoid incorrect optimizations based on `IS [NOT] NULL` tests that are applied to composite values (Bruce Momjian) [§](https://postgr.es/c/b8b1e87b7)

  * Fix planner's failure to identify more than one hashable ScalarArrayOpExpr subexpression within a top-level expression (David Geier) [§](https://postgr.es/c/5672a8399)

This resulted in unnecessarily-inefficient execution of any additional subexpressions that could have been processed with a hash table (that is, `IN`, `NOT IN`, or `= ANY` clauses with all-constant right-hand sides). 

  * Fix incorrect table size estimate with low fill factor (Tomas Vondra) [§](https://postgr.es/c/587b6aa3f)

When the planner estimates the number of rows in a never-yet-analyzed table, it uses the table's fillfactor setting in the estimation, but it neglected to clamp the result to at least one row per page. A low fillfactor could thus result in an unreasonably small estimate. 

  * Disable “skip fetch” optimization in bitmap heap scan (Matthias van de Meent) [§](https://postgr.es/c/78cb2466f)

It turns out that this optimization can result in returning dead tuples when a concurrent vacuum marks a page all-visible. 

  * Fix performance issues in GIN index search startup when there are many search keys (Tom Lane, Vinod Sridharan) [§](https://postgr.es/c/9094eb25b) [§](https://postgr.es/c/8c153fcfa)

An indexable clause with many keys (for example, `jsonbcol ?| array[...]` with tens of thousands of array elements) took O(N2) time to start up, and was uncancelable for that interval too. 

  * Detect missing support procedures in a BRIN index operator class, and report an error instead of crashing (Álvaro Herrera) [§](https://postgr.es/c/ade976f8b)

  * Respond to interrupts (such as query cancel) while waiting for asynchronous subplans of an Append plan node (Heikki Linnakangas) [§](https://postgr.es/c/e731e9d5e)

Previously, nothing would happen until one of the subplans becomes ready. 

  * Report the I/O statistics of active WAL senders more frequently (Bertrand Drouvot) [§](https://postgr.es/c/5cbbe70a9)

Previously, the `pg_stat_io` view failed to accumulate I/O performed by a WAL sender until that process exited. Now such I/O will be reported after at most one second's delay. 

  * Fix race condition in handling of `synchronous_standby_names` immediately after startup (Melnikov Maksim, Michael Paquier) [§](https://postgr.es/c/3339847cc)

For a short period after system startup, backends might fail to wait for synchronous commit even though `synchronous_standby_names` is enabled. 

  * Cope with possible intra-query changes of `io_combine_limit` (Thomas Munro) [§](https://postgr.es/c/e27346807)

  * Avoid infinite loop if `scram_iterations` is set to INT_MAX (Kevin K Biju) [§](https://postgr.es/c/34fbfe1f5)

  * Avoid possible crashes due to double transformation of `json_array()`'s subquery (Tom Lane) [§](https://postgr.es/c/717e8a1e5)

  * Fix `pg_strtof()` to not crash with null endptr (Alexander Lakhin, Tom Lane) [§](https://postgr.es/c/d69c78108)

  * Fix crash after out-of-memory in certain GUC assignments (Daniel Gustafsson) [§](https://postgr.es/c/8afec4ef6)

  * Avoid crash when a Snowball stemmer encounters an out-of-memory condition (Maksim Korotkov) [§](https://postgr.es/c/7edd2cbc5)

  * Fix over-enthusiastic freeing of SpecialJoinInfo structs during planning (Richard Guo) [§](https://postgr.es/c/727bc6ac3)

This led to crashes during planning if partitionwise joining is enabled. 

  * Disallow copying of invalidated replication slots (Shlok Kyal) [§](https://postgr.es/c/a4309e85f)

This prevents trouble when the invalid slot points to WAL that's already been removed. 

  * Disallow restoring logical replication slots on standby servers that are not in hot-standby mode (Masahiko Sawada) [§](https://postgr.es/c/174952ece)

This prevents a scenario where the slot could remain valid after promotion even if `wal_level` is too low. 

  * Prevent over-advancement of catalog xmin in “fast forward” mode of logical decoding (Zhijie Hou) [§](https://postgr.es/c/36148b22e)

This mistake could allow deleted catalog entries to be vacuumed away even though they were still potentially needed by the WAL-reading process. 

  * Avoid data loss when DDL operations that don't take a strong lock affect tables that are being logically replicated (Shlok Kyal, Hayato Kuroda) [§](https://postgr.es/c/cadaf0ac4) [§](https://postgr.es/c/d96206f25)

The catalog changes caused by the DDL command were not reflected into WAL-decoding processes, allowing them to decode subsequent changes using stale catalog data, probably resulting in data corruption. 

  * Prevent incorrect reset of replication origin when an apply worker encounters an error but the error is caught and does not result in worker exit (Hayato Kuroda) [§](https://postgr.es/c/05676d87e)

This mistake could allow duplicate data to be applied. 

  * Fix crash in logical replication if the subscriber's partitioned table has a BRIN index (Tom Lane) [§](https://postgr.es/c/788baa9a2)

  * Avoid duplicate snapshot creation in logical replication index lookups (Heikki Linnakangas) [§](https://postgr.es/c/c1dd3a944) [§](https://postgr.es/c/f1ef111a0)

  * Improve detection of mixed-origin subscriptions (Hou Zhijie, Shlok Kyal) [§](https://postgr.es/c/0ae1245e0)

Subscription creation gives a warning if a subscribed-to table is also being followed through other publications, since that could cause duplicate data to be received. This change improves that logic to also detect cases where a partition parent or child table is the one being followed through another publication. 

  * Fix wrong checkpoint details in error message about incorrect recovery timeline choice (David Steele) [§](https://postgr.es/c/29cce279b)

If the requested recovery timeline is not reachable, the reported checkpoint and timeline should be the values read from the backup_label, if there is one. This message previously reported values from the control file, which is correct when recovering from the control file without a backup_label, but not when there is a backup_label. 

  * Fix order of operations in `smgropen()` (Andres Freund) [§](https://postgr.es/c/ee578921b)

Ensure that the SMgrRelation object is fully initialized before calling the smgr_open callback, so that it can be cleaned up properly if the callback fails. 

  * Remove incorrect assertion in `pgstat_report_stat()` (Michael Paquier) [§](https://postgr.es/c/4b6331e0f)

  * Fix overly-strict assertion in `gistFindCorrectParent()` (Heikki Linnakangas) [§](https://postgr.es/c/6526d0794)

  * Avoid assertion failure in parallel vacuum when `maintenance_work_mem` has a very small value (Masahiko Sawada) [§](https://postgr.es/c/a38dce3c4)

  * Fix rare assertion failure in standby servers when the primary is restarted (Heikki Linnakangas) [§](https://postgr.es/c/302ce5bd9)

  * In PL/pgSQL, avoid “unexpected plan node type” error when a scrollable cursor is defined on a simple `SELECT _`expression`_` query (Andrei Lepikhov) [§](https://postgr.es/c/1353b1161)

  * Don't try to drop individual index partitions in pg_dump's `--clean` mode (Jian He) [§](https://postgr.es/c/3424c1075)

The server rejects such `DROP` commands. That has no real consequences, since the partitions will go away anyway in the subsequent `DROP`s of either their parent tables or their partitioned index. However, the error reported for the attempted drop causes problems when restoring in `--single-transaction` mode. 

  * In pg_dumpall, avoid emitting invalid role `GRANT` commands if `pg_auth_members` contains invalid role OIDs (Tom Lane) [§](https://postgr.es/c/16eff4261)

Instead, print a warning and skip the entry. This copes better with catalog corruption that has been seen to occur in back branches as a result of race conditions between `GRANT` and `DROP ROLE`. 

  * In pg_amcheck and pg_upgrade, use the correct function to free allocations made by libpq (Michael Paquier, Ranier Vilela) [§](https://postgr.es/c/ee78823ff) [§](https://postgr.es/c/0851b6573) [§](https://postgr.es/c/f903d4da9)

These oversights could result in crashes in certain Windows build configurations, such as a debug build of libpq used by a non-debug build of the calling application. 

  * Fix reindexdb's scheduling of parallel reindex operations (Alexander Korotkov) [§](https://postgr.es/c/09ef2f8df)

The original coding failed to achieve the expected amount of parallelism. 

  * Avoid crashing with corrupt input data in `contrib/pageinspect`'s `heap_page_items()` (Dmitry Kovalenko) [§](https://postgr.es/c/ecb8e5641)

  * Prevent assertion failure in `contrib/pg_freespacemap`'s `pg_freespacemap()` (Tender Wang) [§](https://postgr.es/c/51d038da8)

Applying `pg_freespacemap()` to a relation lacking storage (such as a view) caused an assertion failure, although there was no ill effect in non-assert builds. Add an error check to reject that case. 

  * In `contrib/postgres_fdw`, avoid pulling up restriction conditions from subqueries (Alexander Pyhalov) [§](https://postgr.es/c/729fe699e)

This fix prevents rare cases of “unexpected expression in subquery output” errors. 

  * Fix build failure when an old version of `libpq_fe.h` is present in system include directories (Tom Lane) [§](https://postgr.es/c/f186f90e5)

  * Fix build failure on macOS 15.4 (Tom Lane, Peter Eisentraut) [§](https://postgr.es/c/915e88968)

This macOS update broke our configuration probe for `strchrnul()`. 

  * Fix valgrind labeling of per-buffer data of read streams (Thomas Munro) [§](https://postgr.es/c/57dca6faa)

This affects no core code in released versions of PostgreSQL, but an extension using the per-buffer data feature might have encountered spurious failures when being tested under valgrind. 

  * Avoid valgrind complaints about string hashing code (John Naylor) [§](https://postgr.es/c/fde7c0164)

  * Update time zone data files to tzdata release 2025b for DST law changes in Chile, plus historical corrections for Iran (Tom Lane) [§](https://postgr.es/c/5d5970b9f)

There is a new time zone America/Coyhaique for Chile's Aysén Region, to account for it changing to UTC-03 year-round and thus diverging from America/Santiago. 




* * *

[Prev](release.md "Appendix E. Release Notes") | [Up](release.md "Appendix E. Release Notes")|  [Next](release-17-4.md "E.2. Release 17.4")  
---|---|---  
Appendix E. Release Notes | [Home](index.md "PostgreSQL 17.5 Documentation")|  E.2. Release 17.4
