E.5. Release 17.1  
---  
[Prev](release-17-2.md "E.4. Release 17.2") | [Up](release.md "Appendix E. Release Notes")| Appendix E. Release Notes| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](release-17.md "E.6. Release 17")  
  
* * *

## E.5. Release 17.1 #

[E.5.1. Migration to Version 17.1](release-17-1.md#RELEASE-17-1-MIGRATION)
[E.5.2. Changes](release-17-1.md#RELEASE-17-1-CHANGES)

**Release date:** 2024-11-14

This release contains a variety of fixes from 17.0. For information about new features in major release 17, see [Section E.6](release-17.md "E.6. Release 17"). 

### E.5.1. Migration to Version 17.1 #

A dump/restore is not required for those running 17.X. 

However, if you have ever detached a partition from a partitioned table that has a foreign-key reference to another partitioned table, and not dropped the former partition, then you may have catalog and/or data corruption to repair, as detailed in the fifth changelog entry below. 

Also, in the uncommon case that a database's `LC_CTYPE` setting is `C` while its `LC_COLLATE` setting is some other locale, indexes on textual columns should be reindexed, as described in the sixth changelog entry below. 

### E.5.2. Changes #

  * Ensure cached plans are marked as dependent on the calling role when RLS applies to a non-top-level table reference (Nathan Bossart) [§](https://postgr.es/c/edcda9bb4)

If a CTE, subquery, sublink, security invoker view, or coercion projection in a query references a table with row-level security policies, we neglected to mark the resulting plan as potentially dependent on which role is executing it. This could lead to later query executions in the same session using the wrong plan, and then returning or hiding rows that should have been hidden or returned instead. 

The PostgreSQL Project thanks Wolfgang Walther for reporting this problem. (CVE-2024-10976) 

  * Make libpq discard error messages received during SSL or GSS protocol negotiation (Jacob Champion) [§](https://postgr.es/c/a5cc4c667)

An error message received before encryption negotiation is completed might have been injected by a man-in-the-middle, rather than being real server output. Reporting it opens the door to various security hazards; for example, the message might spoof a query result that a careless user could mistake for correct output. The best answer seems to be to discard such data and rely only on libpq's own report of the connection failure. 

The PostgreSQL Project thanks Jacob Champion for reporting this problem. (CVE-2024-10977) 

  * Fix unintended interactions between `SET SESSION AUTHORIZATION` and `SET ROLE` (Tom Lane) [§](https://postgr.es/c/cd82afdda) [§](https://postgr.es/c/f4f5d27d8)

The SQL standard mandates that `SET SESSION AUTHORIZATION` have a side-effect of doing `SET ROLE NONE`. Our implementation of that was flawed, creating more interaction between the two settings than intended. Notably, rolling back a transaction that had done `SET SESSION AUTHORIZATION` would revert `ROLE` to `NONE` even if that had not been the previous state, so that the effective user ID might now be different from what it had been before the transaction. Transiently setting `session_authorization` in a function `SET` clause had a similar effect. A related bug was that if a parallel worker inspected `current_setting('role')`, it saw `none` even when it should see something else. 

The PostgreSQL Project thanks Tom Lane for reporting this problem. (CVE-2024-10978) 

  * Prevent trusted PL/Perl code from changing environment variables (Andrew Dunstan, Noah Misch) [§](https://postgr.es/c/3ebcfa54d) [§](https://postgr.es/c/4cd4f3b97) [§](https://postgr.es/c/8d19f3fea)

The ability to manipulate process environment variables such as `PATH` gives an attacker opportunities to execute arbitrary code. Therefore, “trusted” PLs must not offer the ability to do that. To fix `plperl`, replace `%ENV` with a tied hash that rejects any modification attempt with a warning. Untrusted `plperlu` retains the ability to change the environment. 

The PostgreSQL Project thanks Coby Abrams for reporting this problem. (CVE-2024-10979) 

  * Fix updates of catalog state for foreign-key constraints when attaching or detaching table partitions (Jehan-Guillaume de Rorthais, Tender Wang, Álvaro Herrera) [§](https://postgr.es/c/5914a22f6) [§](https://postgr.es/c/936ab6de9)

If the referenced table is partitioned, then different catalog entries are needed for a referencing table that is stand-alone versus one that is a partition. `ATTACH/DETACH PARTITION` commands failed to perform this conversion correctly. In particular, after `DETACH` the now stand-alone table would be missing foreign-key enforcement triggers, which could result in the table later containing rows that fail the foreign-key constraint. A subsequent re-`ATTACH` could fail with surprising errors, too. 

The way to fix this is to do `ALTER TABLE DROP CONSTRAINT` on the now stand-alone table for each faulty constraint, and then re-add the constraint. If re-adding the constraint fails, then some erroneous data has crept in. You will need to manually re-establish consistency between the referencing and referenced tables, then re-add the constraint. 

This query can be used to identify broken constraints and construct the commands needed to recreate them: 
        
        SELECT conrelid::pg_catalog.regclass AS "constrained table",
               conname AS constraint,
               confrelid::pg_catalog.regclass AS "references",
               pg_catalog.format('ALTER TABLE %s DROP CONSTRAINT %I;',
                                 conrelid::pg_catalog.regclass, conname) AS "drop",
               pg_catalog.format('ALTER TABLE %s ADD CONSTRAINT %I %s;',
                                 conrelid::pg_catalog.regclass, conname,
                                 pg_catalog.pg_get_constraintdef(oid)) AS "add"
        FROM pg_catalog.pg_constraint c
        WHERE contype = 'f' AND conparentid = 0 AND
           (SELECT count(*) FROM pg_catalog.pg_constraint c2
            WHERE c2.conparentid = c.oid) <>
           ((SELECT count(*) FROM pg_catalog.pg_inherits i
            WHERE (i.inhparent = c.conrelid OR i.inhparent = c.confrelid) AND
              EXISTS (SELECT 1 FROM pg_catalog.pg_partitioned_table
                      WHERE partrelid = i.inhparent)) +
            CASE WHEN pg_catalog.pg_partition_root(conrelid) = confrelid THEN
                      (SELECT count(*) FROM pg_catalog.pg_partition_tree(confrelid)
                        WHERE level = 1)
                 ELSE 0 END);
        

Since it is possible that one or more of the `ADD CONSTRAINT` steps will fail, you should save the query's output in a file and then attempt to perform each step. 

  * Fix test for `C` locale when `LC_COLLATE` is different from `LC_CTYPE` (Jeff Davis) [§](https://postgr.es/c/8148e7124)

When using `libc` as the default collation provider, the test to see if `C` locale is in use for collation accidentally checked `LC_CTYPE` not `LC_COLLATE`. This has no impact in the typical case where those settings are the same, nor if both are not `C` (nor its alias `POSIX`). However, if `LC_CTYPE` is `C` while `LC_COLLATE` is some other locale, wrong query answers could ensue, and corruption of indexes on strings was possible. Users of databases with such settings should reindex affected indexes after installing this update. The converse case with `LC_COLLATE` being `C` while `LC_CTYPE` is some other locale would cause performance degradation, but no actual errors. 

  * Don't use partitionwise joins or grouping if the query's collation for the key column doesn't match the partition key's collation (Jian He, Webbo Han) [§](https://postgr.es/c/a0cdfc889) [§](https://postgr.es/c/b6484ca95)

Such plans could produce incorrect results. 

  * Avoid planner failure after converting an `IS NULL` test on a `NOT NULL` column to constant `FALSE` (Richard Guo) [§](https://postgr.es/c/78b1c553b)

This bug typically led to errors such as “variable not found in subplan target lists”. 

  * Avoid possible planner crash while inlining a SQL function whose arguments contain certain array-related constructs (Tom Lane, Nathan Bossart) [§](https://postgr.es/c/a3c4a91f1)

  * Fix possible wrong answers or “wrong varnullingrels” planner errors for `MERGE ... WHEN NOT MATCHED BY SOURCE` actions (Dean Rasheed) [§](https://postgr.es/c/d7d297f84) [§](https://postgr.es/c/34ae54af9)

  * Fix possible “could not find pathkey item to sort” error when the output of a `UNION ALL` member query needs to be sorted, and the sort column is an expression (Andrei Lepikhov, Tom Lane) [§](https://postgr.es/c/54889ea64)

  * Fix edge case in B-tree ScalarArrayOp index scans (Peter Geoghegan) [§](https://postgr.es/c/c177726ae)

When a scrollable cursor with a plan of this kind was backed up to its starting point and then run forward again, wrong answers were possible. 

  * Fix assertion failure or confusing error message for `COPY (_`query`_) TO ...`, when the _`query`_ is rewritten by a `DO INSTEAD NOTIFY` rule (Tender Wang, Tom Lane) [§](https://postgr.es/c/3685ad618)

  * Fix validation of `COPY`'s `FORCE_NOT_NULL` and `FORCE_NULL` options (Joel Jacobson) [§](https://postgr.es/c/c06a4746b)

Some incorrect usages are now rejected as they should be. 

  * Fix server crash when a `json_objectagg()` call contains a volatile function (Amit Langote) [§](https://postgr.es/c/7148cb3e3)

  * Fix detection of skewed data during parallel hash join (Thomas Munro) [§](https://postgr.es/c/4ac5d33a8)

After repartitioning the inner side of a hash join because one partition has accumulated too many tuples, we check to see if all the partition's tuples went into the same child partition, which suggests that they all have the same hash value and further repartitioning cannot improve matters. This check malfunctioned in some cases, allowing repeated futile repartitioning which would eventually end in a resource-exhaustion error. 

  * Avoid crash when `ALTER DATABASE SET` is used to set a server parameter that requires search-path-based lookup, such as `default_text_search_config` (Jeff Davis) [§](https://postgr.es/c/2fe4167bc)

  * Avoid repeated lookups of opclasses and collations while creating a new index on a partitioned table (Tom Lane) [§](https://postgr.es/c/fee8cb947)

This was problematic mainly because some of the lookups would be done with a restricted `search_path`, leading to unexpected failures if the `CREATE INDEX` command referenced objects outside `pg_catalog`. 

This fix also prevents comments on the parent partitioned index from being copied to child indexes. 

  * Add missing dependency from a partitioned table to a non-built-in access method specified in `CREATE TABLE ... USING` (Michael Paquier) [§](https://postgr.es/c/bb584e831)

Dropping the access method should be blocked when a table exists that depends on it, but it was not, allowing subsequent odd behavior. Note that this fix only prevents problems for partitioned tables created after this update. 

  * Disallow locale names containing non-ASCII characters (Thomas Munro) [§](https://postgr.es/c/9c7acc333)

This is only an issue on Windows, as such locale names are not used elsewhere. They are problematic because it's quite unclear what encoding such names are represented in (since the locale itself defines the encoding to use). In recent PostgreSQL releases, an abort in the Windows runtime library could occur because of confusion about that. 

Anyone who encounters the new error message should either create a new duplicated locale with an ASCII-only name using Windows Locale Builder, or consider using BCP 47-compliant locale names like `tr-TR`. 

  * Fix race condition in committing a serializable transaction (Heikki Linnakangas) [§](https://postgr.es/c/234f6d09e)

Mis-processing of a recently committed transaction could lead to an assertion failure or a “could not access status of transaction” error. 

  * Fix race condition in `COMMIT PREPARED` that resulted in orphaned 2PC files (wuchengwen) [§](https://postgr.es/c/f250cb29d)

A concurrent `PREPARE TRANSACTION` could cause `COMMIT PREPARED` to not remove the on-disk two-phase state file for the completed transaction. There was no immediate ill effect, but a subsequent crash-and-recovery could fail with “could not access status of transaction”, requiring manual removal of the orphaned file to restore service. 

  * Avoid invalid memory accesses after skipping an invalid toast index during `VACUUM FULL` (Tender Wang) [§](https://postgr.es/c/1532599a8)

A list tracking yet-to-be-rebuilt indexes was not properly updated in this code path, risking assertion failures or crashes later on. 

  * Fix ways in which an “in place” catalog update could be lost (Noah Misch) [§](https://postgr.es/c/fd27b878c) [§](https://postgr.es/c/3b7a689e1) [§](https://postgr.es/c/da99df15c) [§](https://postgr.es/c/e11907682) [§](https://postgr.es/c/9aef6f19a) [§](https://postgr.es/c/0bcb9d079) [§](https://postgr.es/c/54bc22fbf)

Normal row updates write a new version of the row to preserve rollback-ability of the transaction. However, certain system catalog updates are intentionally non-transactional and are done with an in-place update of the row. These patches fix race conditions that could cause the effects of an in-place update to be lost. As an example, it was possible to forget having set `pg_class`.`relhasindex` to true, preventing updates of the new index and thus causing index corruption. 

  * Reset catalog caches at end of recovery (Noah Misch) [§](https://postgr.es/c/a4668c99f)

This prevents scenarios wherein an in-place catalog update could be lost due to using stale data from a catalog cache. 

  * Avoid using parallel query while holding off interrupts (Francesco Degrassi, Noah Misch, Tom Lane) [§](https://postgr.es/c/2370582ab) [§](https://postgr.es/c/943b65358)

This situation cannot arise normally, but it can be reached with test scenarios such as using a SQL-language function as B-tree support (which would be far too slow for production usage). If it did occur it would result in an indefinite wait. 

  * Ignore not-yet-defined Portals in the `pg_cursors` view (Tom Lane) [§](https://postgr.es/c/3daeb539a)

It is possible for user-defined code that inspects this view to be called while a new cursor is being set up, and if that happens a null pointer dereference would ensue. Avoid the problem by defining the view to exclude incompletely-set-up cursors. 

  * Avoid “unexpected table_index_fetch_tuple call during logical decoding” error while decoding a transaction involving insertion of a column default value (Takeshi Ideriha, Hou Zhijie) [§](https://postgr.es/c/918107759) [§](https://postgr.es/c/c4b8a916f)

  * Reduce memory consumption of logical decoding (Masahiko Sawada) [§](https://postgr.es/c/eef9cc4dc)

Use a smaller default block size to store tuple data received during logical replication. This reduces memory wastage, which has been reported to be severe while processing long-running transactions, even leading to out-of-memory failures. 

  * Fix behavior of stable functions called from a `CALL` statement's argument list, when the `CALL` is within a PL/pgSQL `EXCEPTION` block (Tom Lane) [§](https://postgr.es/c/b5eef7539)

As with a similar fix in our previous quarterly releases, this case allowed such functions to be passed the wrong snapshot, causing them to see stale values of rows modified since the start of the outer transaction. 

  * Parse libpq's `keepalives` connection option in the same way as other integer-valued options (Yuto Sasaki) [§](https://postgr.es/c/c7a201053)

The coding used here rejected trailing whitespace in the option value, unlike other cases. This turns out to be problematic in ecpg's usage, for example. 

  * In ecpglib, fix out-of-bounds read when parsing incorrect datetime input (Bruce Momjian, Pavel Nekrasov) [§](https://postgr.es/c/2c37cb26f)

It was possible to try to read the location just before the start of a constant array. Real-world consequences seem minimal, though. 

  * Fix psql's describe commands to again work with pre-9.4 servers (Tom Lane) [§](https://postgr.es/c/923a71584)

Commands involving display of an ACL (permissions) column failed with very old PostgreSQL servers, due to use of a function not present in those versions. 

  * Avoid hanging if an interval less than 1ms is specified in psql's `\watch` command (Andrey Borodin, Michael Paquier) [§](https://postgr.es/c/8a6170860)

Instead, treat this the same as an interval of zero (no wait between executions). 

  * Fix failure to find replication password in `~/.pgpass` (Tom Lane) [§](https://postgr.es/c/e2a912909)

pg_basebackup and pg_receivewal failed to match an entry in `~/.pgpass` that had `replication` in the database name field, if no `-d` or `--dbname` switch was supplied. This resulted in an unexpected prompt for password. 

  * In pg_combinebackup, throw an error if an incremental backup file is present in a directory that is supposed to contain a full backup (Robert Haas) [§](https://postgr.es/c/e36711442)

  * In pg_combinebackup, don't construct filenames containing double slashes (Robert Haas) [§](https://postgr.es/c/0d635b615)

This caused no functional problems, but the duplicate slashes were visible in error messages, which could create confusion. 

  * Avoid trying to reindex temporary tables and indexes in vacuumdb and in parallel reindexdb (VaibhaveS, Michael Paquier, Fujii Masao, Nathan Bossart) [§](https://postgr.es/c/85cb21df6) [§](https://postgr.es/c/77f154681) [§](https://postgr.es/c/5bd26e652)

Reindexing other sessions' temporary tables cannot work, but the check to skip them was missing in some code paths, leading to unwanted failures. 

  * Fix incorrect LLVM-generated code on ARM64 platforms (Thomas Munro, Anthonin Bonnefoy) [§](https://postgr.es/c/b7467ab71)

When using JIT compilation on ARM platforms, the generated code could not support relocation distances exceeding 32 bits, allowing unlucky placement of generated code to cause server crashes on large-memory systems. 

  * Fix a few places that assumed that process start time (represented as a `time_t`) will fit into a `long` value (Max Johnson, Nathan Bossart) [§](https://postgr.es/c/a356d23fd)

On platforms where `long` is 32 bits (notably Windows), this coding would fail after Y2038. Most of the failures appear only cosmetic, but notably `pg_ctl start` would hang. 

  * Update time zone data files to tzdata release 2024b (Tom Lane) [§](https://postgr.es/c/cad65907e) [§](https://postgr.es/c/6283ff201)

This tzdata release changes the old System-V-compatibility zone names to duplicate the corresponding geographic zones; for example `PST8PDT` is now an alias for `America/Los_Angeles`. The main visible consequence is that for timestamps before the introduction of standardized time zones, the zone is considered to represent local mean solar time for the named location. For example, in `PST8PDT`, `timestamptz` input such as `1801-01-01 00:00` would previously have been rendered as `1801-01-01 00:00:00-08`, but now it is rendered as `1801-01-01 00:00:00-07:52:58`. 

Also, historical corrections for Mexico, Mongolia, and Portugal. Notably, `Asia/Choibalsan` is now an alias for `Asia/Ulaanbaatar` rather than being a separate zone, mainly because the differences between those zones were found to be based on untrustworthy data. 




* * *

[Prev](release-17-2.md "E.4. Release 17.2") | [Up](release.md "Appendix E. Release Notes")|  [Next](release-17.md "E.6. Release 17")  
---|---|---  
E.4. Release 17.2 | [Home](index.md "PostgreSQL 17.5 Documentation")|  E.6. Release 17
