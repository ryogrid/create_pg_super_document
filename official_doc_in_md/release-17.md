E.6. Release 17  
---  
[Prev](release-17-1.md "E.5. Release 17.1") | [Up](release.md "Appendix E. Release Notes")| Appendix E. Release Notes| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](release-prior.md "E.7. Prior Releases")  
  
* * *

## E.6. Release 17 #

[E.6.1. Overview](release-17.md#RELEASE-17-HIGHLIGHTS)
[E.6.2. Migration to Version 17](release-17.md#RELEASE-17-MIGRATION)
[E.6.3. Changes](release-17.md#RELEASE-17-CHANGES)
[E.6.4. Acknowledgments](release-17.md#RELEASE-17-ACKNOWLEDGEMENTS)

**Release date:** 2024-09-26

### E.6.1. Overview #

PostgreSQL 17 contains many new features and enhancements, including: 

  * New memory management system for `VACUUM`, which reduces memory consumption and can improve overall vacuuming performance. 

  * New SQL/JSON capabilities, including constructors, identity functions, and the [`JSON_TABLE()`](functions-json.md#FUNCTIONS-SQLJSON-TABLE "9.16.4. JSON_TABLE") function, which converts JSON data into a table representation. 

  * Various query performance improvements, including for sequential reads using streaming I/O, write throughput under high concurrency, and searches over multiple values in a [btree](btree.md "64.1. B-Tree Indexes") index. 

  * Logical replication enhancements, including: 

    * Failover control 

    * [pg_createsubscriber](app-pgcreatesubscriber.md "pg_createsubscriber"), a utility that creates logical replicas from physical standbys 

    * [pg_upgrade](pgupgrade.md "pg_upgrade") now preserves logical replication slots on publishers and full subscription state on subscribers. This will allow upgrades to future major versions to continue logical replication without requiring copy to resynchronize. 

  * New client-side connection option, [`sslnegotiation=direct`](libpq-connect.md#LIBPQ-CONNECT-SSLNEGOTIATION), that performs a direct TLS handshake to avoid a round-trip negotiation. 

  * [pg_basebackup](app-pgbasebackup.md "pg_basebackup") now supports incremental backup. 

  * [`COPY`](sql-copy.md "COPY") adds a new option, `ON_ERROR ignore`, that allows a copy operation to continue in the event of an error. 




The above items and other new features of PostgreSQL 17 are explained in more detail in the sections below. 

### E.6.2. Migration to Version 17 #

A dump/restore using [pg_dumpall](app-pg-dumpall.md "pg_dumpall") or use of [pg_upgrade](pgupgrade.md "pg_upgrade") or logical replication is required for those wishing to migrate data from any previous release. See [Section 18.6](upgrading.md "18.6. Upgrading a PostgreSQL Cluster") for general information on migrating to new major releases. 

Version 17 contains a number of changes that may affect compatibility with previous releases. Observe the following incompatibilities: 

  * Change functions to use a safe [search_path](runtime-config-client.md#GUC-SEARCH-PATH) during maintenance operations (Jeff Davis) [§](https://postgr.es/c/2af07e2f7) [§](https://postgr.es/c/b4da732fd64)

This prevents maintenance operations (`ANALYZE`, `CLUSTER`, `CREATE INDEX`, `CREATE MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW`, `REINDEX`, or `VACUUM`) from performing unsafe access. Functions used by expression indexes and materialized views that need to reference non-default schemas must specify a search path during function creation. 

  * Restrict `ago` to only appear at the end in `interval` values (Joseph Koshakow) [§](https://postgr.es/c/165d581f1) [§](https://postgr.es/c/617f9b7d4)

Also, prevent empty interval units from appearing multiple times. 

  * Remove server variable old_snapshot_threshold (Thomas Munro) [§](https://postgr.es/c/f691f5b80)

This variable allowed vacuum to remove rows that potentially could be still visible to running transactions, causing "snapshot too old" errors later if accessed. This feature might be re-added to PostgreSQL later if an improved implementation is found. 

  * Change [`SET SESSION AUTHORIZATION`](sql-set-session-authorization.md "SET SESSION AUTHORIZATION") handling of the initial session user's superuser status (Joseph Koshakow) [§](https://postgr.es/c/a0363ab7a)

The new behavior is based on the session user's superuser status at the time the `SET SESSION AUTHORIZATION` command is issued, rather than their superuser status at connection time. 

  * Remove feature which simulated per-database users (Nathan Bossart) [§](https://postgr.es/c/884eee5bf)

The feature, `db_user_namespace`, was rarely used. 

  * Remove adminpack contrib extension (Daniel Gustafsson) [§](https://postgr.es/c/cc09e6549)

This was used by now end-of-life pgAdmin III. 

  * Remove [wal_sync_method](runtime-config-wal.md#GUC-WAL-SYNC-METHOD) value `fsync_writethrough` on Windows (Thomas Munro) [§](https://postgr.es/c/d0c28601e)

This value was the same as `fsync` on Windows. 

  * Change file boundary handling of two WAL file name functions (Kyotaro Horiguchi, Andres Freund, Bruce Momjian) [§](https://postgr.es/c/344afc776)

The functions [`pg_walfile_name()`](functions-admin.md#FUNCTIONS-ADMIN-BACKUP-TABLE "Table 9.95. Backup Control Functions") and `pg_walfile_name_offset()` used to report the previous LSN segment number when the LSN was on a file segment boundary; it now returns the current LSN segment. 

  * Remove server variable `trace_recovery_messages` since it is no longer needed (Bharath Rupireddy) [§](https://postgr.es/c/c7a3e6b46)

  * Remove [information schema](information-schema.md "Chapter 35. The Information Schema") column `element_types`.`domain_default` (Peter Eisentraut) [§](https://postgr.es/c/78806a950)

  * Change [pgrowlocks](pgrowlocks.md "F.29. pgrowlocks — show a table's row locking information") lock mode output labels (Bruce Momjian) [§](https://postgr.es/c/15d5d7405)

  * Remove `buffers_backend` and `buffers_backend_fsync` from [`pg_stat_bgwriter`](monitoring-stats.md#MONITORING-PG-STAT-BGWRITER-VIEW "27.2.14. pg_stat_bgwriter") (Bharath Rupireddy) [§](https://postgr.es/c/74604a37f)

These fields are considered redundant to similar columns in [`pg_stat_io`](monitoring-stats.md#MONITORING-PG-STAT-IO-VIEW "27.2.13. pg_stat_io"). 

  * Rename I/O block read/write timing statistics columns of [pg_stat_statements](pgstatstatements.md "F.30. pg_stat_statements — track statistics of SQL planning and execution") (Nazir Bilal Yavuz) [§](https://postgr.es/c/13d00729d)

This renames `blk_read_time` to `shared_blk_read_time`, and `blk_write_time` to `shared_blk_write_time`. 

  * Change [`pg_attribute`.`attstattarget`](catalog-pg-attribute.md "51.7. pg_attribute") and `pg_statistic_ext`.`stxstattarget` to represent the default statistics target as `NULL` (Peter Eisentraut) [§](https://postgr.es/c/4f622503d) [§](https://postgr.es/c/012460ee9)

  * Rename [`pg_collation`.`colliculocale`](catalog-pg-collation.md "51.12. pg_collation") to `colllocale` and [`pg_database`.`daticulocale`](catalog-pg-database.md "51.15. pg_database") to `datlocale` (Jeff Davis) [§](https://postgr.es/c/f696c0cd5)

  * Rename [`pg_stat_progress_vacuum`](progress-reporting.md#VACUUM-PROGRESS-REPORTING "27.4.5. VACUUM Progress Reporting") column `max_dead_tuples` to `max_dead_tuple_bytes`, rename `num_dead_tuples` to `num_dead_item_ids`, and add `dead_tuple_bytes` (Masahiko Sawada) [§](https://postgr.es/c/667e65aac) [§](https://postgr.es/c/f1affb670)

  * Rename SLRU columns in system view [`pg_stat_slru`](monitoring-stats.md#MONITORING-PG-STAT-SLRU-VIEW "27.2.25. pg_stat_slru") (Alvaro Herrera) [§](https://postgr.es/c/bcdfa5f2e)

The column names accepted by [`pg_stat_reset_slru()`](monitoring-stats.md#MONITORING-STATS-FUNCS-TABLE "Table 27.36. Additional Statistics Functions") are also changed. 




### E.6.3. Changes #

Below you will find a detailed account of the changes between PostgreSQL 17 and the previous major release. 

#### E.6.3.1. Server #

##### E.6.3.1.1. Optimizer #

  * Allow the optimizer to improve CTE plans by considering the statistics and sort order of columns referenced in earlier row output clauses (Jian Guo, Richard Guo, Tom Lane) [§](https://postgr.es/c/f7816aec2) [§](https://postgr.es/c/a65724dfa)

  * Improve optimization of `IS NOT NULL` and `IS NULL` query restrictions (David Rowley, Richard Guo, Andy Fan) [§](https://postgr.es/c/b262ad440) [§](https://postgr.es/c/3af704098)

Remove `IS NOT NULL` restrictions from queries on `NOT NULL` columns and eliminate scans on `NOT NULL` columns if `IS NULL` is specified. 

  * Allow partition pruning on boolean columns on `IS [NOT] UNKNOWN` conditionals (David Rowley) [§](https://postgr.es/c/07c36c133)

  * Improve optimization of range values when using containment operators <@ and @> (Kim Johan Andersson, Jian He) [§](https://postgr.es/c/075df6b20)

  * Allow correlated `IN` subqueries to be transformed into joins (Andy Fan, Tom Lane) [§](https://postgr.es/c/9f1337639)

  * Improve optimization of the `LIMIT` clause on partitioned tables, inheritance parents, and `UNION ALL` queries (Andy Fan, David Rowley) [§](https://postgr.es/c/a8a968a82)

  * Allow query nodes to be run in parallel in more cases (Tom Lane) [§](https://postgr.es/c/e08d74ca1)

  * Allow `GROUP BY` columns to be internally ordered to match `ORDER BY` (Andrei Lepikhov, Teodor Sigaev) [§](https://postgr.es/c/0452b461b)

This can be disabled using server variable [enable_group_by_reordering](runtime-config-query.md#GUC-ENABLE-GROUPBY-REORDERING). 

  * Allow `UNION` (without `ALL`) to use MergeAppend (David Rowley) [§](https://postgr.es/c/66c0185a3)

  * Fix MergeAppend plans to more accurately compute the number of rows that need to be sorted (Alexander Kuzmenkov) [§](https://postgr.es/c/9d1a5354f)

  * Allow [GiST](gist.md "64.2. GiST Indexes") and [SP-GiST](spgist.md "64.3. SP-GiST Indexes") indexes to be part of incremental sorts (Miroslav Bendik) [§](https://postgr.es/c/625d5b3ca)

This is particularly useful for `ORDER BY` clauses where the first column has a GiST and SP-GiST index, and other columns do not. 

  * Add columns to [`pg_stats`](view-pg-stats.md "52.27. pg_stats") to report range-type histogram information (Egor Rogov, Soumyadeep Chakraborty) [§](https://postgr.es/c/bc3c8db8a)




##### E.6.3.1.2. Indexes #

  * Allow [btree](btree.md "64.1. B-Tree Indexes") indexes to more efficiently find a set of values, such as those supplied by `IN` clauses using constants (Peter Geoghegan, Matthias van de Meent) [§](https://postgr.es/c/5bf748b86)

  * Allow [BRIN](brin.md "64.5. BRIN Indexes") indexes to be created using parallel workers (Tomas Vondra, Matthias van de Meent) [§](https://postgr.es/c/b43757171)




##### E.6.3.1.3. General Performance #

  * Allow vacuum to more efficiently remove and freeze tuples (Melanie Plageman, Heikki Linnakangas) [§](https://postgr.es/c/6dbb49026)

WAL traffic caused by vacuum is also more compact. 

  * Allow vacuum to more efficiently store tuple references (Masahiko Sawada, John Naylor) [§](https://postgr.es/c/ee1b30f12) [§](https://postgr.es/c/30e144287) [§](https://postgr.es/c/667e65aac) [§](https://postgr.es/c/6dbb49026)

Additionally, vacuum is no longer silently limited to one gigabyte of memory when [maintenance_work_mem](runtime-config-resource.md#GUC-MAINTENANCE-WORK-MEM) or [autovacuum_work_mem](runtime-config-resource.md#GUC-AUTOVACUUM-WORK-MEM) are higher. 

  * Optimize vacuuming of relations with no indexes (Melanie Plageman) [§](https://postgr.es/c/c120550ed)

  * Increase default [vacuum_buffer_usage_limit](runtime-config-resource.md#GUC-VACUUM-BUFFER-USAGE-LIMIT) to 2MB (Thomas Munro) [§](https://postgr.es/c/98f320eb2)

  * Improve performance when checking roles with many memberships (Nathan Bossart) [§](https://postgr.es/c/d365ae705)

  * Improve performance of heavily-contended WAL writes (Bharath Rupireddy) [§](https://postgr.es/c/71e4cc6b8)

  * Improve performance when transferring large blocks of data to a client (Melih Mutlu) [§](https://postgr.es/c/c4ab7da60)

  * Allow the grouping of file system reads with the new system variable [io_combine_limit](runtime-config-resource.md#GUC-IO-COMBINE-LIMIT) (Thomas Munro, Andres Freund, Melanie Plageman, Nazir Bilal Yavuz) [§](https://postgr.es/c/210622c60) [§](https://postgr.es/c/b7b0f3f27) [§](https://postgr.es/c/041b96802)




##### E.6.3.1.4. Monitoring #

  * Create system view [`pg_stat_checkpointer`](monitoring-stats.md#MONITORING-PG-STAT-CHECKPOINTER-VIEW "27.2.15. pg_stat_checkpointer") (Bharath Rupireddy, Anton A. Melnikov, Alexander Korotkov) [§](https://postgr.es/c/96f052613) [§](https://postgr.es/c/12915a58e) [§](https://postgr.es/c/e820db5b5)

Relevant columns have been removed from [`pg_stat_bgwriter`](monitoring-stats.md#PG-STAT-BGWRITER-VIEW "Table 27.24. pg_stat_bgwriter View") and added to this new system view. 

  * Improve control over resetting statistics (Atsushi Torikoshi, Bharath Rupireddy) [§](https://postgr.es/c/23c8c0c8f) [§](https://postgr.es/c/2e8a0edc2) [§](https://postgr.es/c/e5cca6288)

Allow [`pg_stat_reset_shared()`](monitoring-stats.md#MONITORING-STATS-FUNCS-TABLE "Table 27.36. Additional Statistics Functions") (with no arguments) and pg_stat_reset_shared(`NULL`) to reset all shared statistics. Allow pg_stat_reset_shared('slru') and [`pg_stat_reset_slru()`](monitoring-stats.md#MONITORING-STATS-FUNCS-TABLE "Table 27.36. Additional Statistics Functions") (with no arguments) to reset SLRU statistics, which was already possible with pg_stat_reset_slru(NULL). 

  * Add log messages related to WAL recovery from backups (Andres Freund) [§](https://postgr.es/c/1d35f705e)

  * Add [log_connections](runtime-config-logging.md#GUC-LOG-CONNECTIONS) log line for `trust` connections (Jacob Champion) [§](https://postgr.es/c/e48b19c5d)

  * Add log message to report walsender acquisition and release of replication slots (Bharath Rupireddy) [§](https://postgr.es/c/7c3fb505b)

This is enabled by the server variable [log_replication_commands](runtime-config-logging.md#GUC-LOG-REPLICATION-COMMANDS). 

  * Add system view [`pg_wait_events`](view-pg-wait-events.md "52.36. pg_wait_events") that reports wait event types (Bertrand Drouvot) [§](https://postgr.es/c/1e68e43d3)

This is useful for adding descriptions to wait events reported in [`pg_stat_activity`](monitoring-stats.md#MONITORING-PG-STAT-ACTIVITY-VIEW "27.2.3. pg_stat_activity"). 

  * Add [wait events](view-pg-wait-events.md "52.36. pg_wait_events") for checkpoint delays (Thomas Munro) [§](https://postgr.es/c/0013ba290)

  * Allow vacuum to report the progress of index processing (Sami Imseih) [§](https://postgr.es/c/46ebdfe16)

This appears in system view [`pg_stat_progress_vacuum`](progress-reporting.md#PG-STAT-PROGRESS-VACUUM-VIEW "Table 27.45. pg_stat_progress_vacuum View") columns `indexes_total` and `indexes_processed`. 




##### E.6.3.1.5. Privileges #

  * Allow granting the right to perform maintenance operations (Nathan Bossart) [§](https://postgr.es/c/ecb0fd337)

The permission can be granted on a per-table basis using the [`MAINTAIN`](ddl-priv.md#DDL-PRIV-MAINTAIN) privilege and on a per-role basis via the [`pg_maintain`](predefined-roles.md "21.5. Predefined Roles") predefined role. Permitted operations are `VACUUM`, `ANALYZE`, `REINDEX`, `REFRESH MATERIALIZED VIEW`, `CLUSTER`, and `LOCK TABLE`. 

  * Allow roles with [`pg_monitor`](predefined-roles.md "21.5. Predefined Roles") membership to execute [`pg_current_logfile()`](functions-info.md#FUNCTIONS-INFO-SESSION-TABLE "Table 9.69. Session Information Functions") (Pavlo Golub, Nathan Bossart) [§](https://postgr.es/c/8d8afd48d)




##### E.6.3.1.6. Server Configuration #

  * Add system variable [allow_alter_system](runtime-config-compatible.md#GUC-ALLOW-ALTER-SYSTEM) to disallow [`ALTER SYSTEM`](sql-altersystem.md "ALTER SYSTEM") (Jelte Fennema-Nio, Gabriele Bartolini) [§](https://postgr.es/c/d3ae2a24f)

  * Allow [`ALTER SYSTEM`](sql-altersystem.md "ALTER SYSTEM") to set unrecognized custom server variables (Tom Lane) [§](https://postgr.es/c/2d870b4ae)

This is also possible with [`GRANT ON PARAMETER`](sql-grant.md "GRANT"). 

  * Add server variable [transaction_timeout](runtime-config-client.md#GUC-TRANSACTION-TIMEOUT) to restrict the duration of transactions (Andrey Borodin, Japin Li, Junwang Zhao, Alexander Korotkov) [§](https://postgr.es/c/51efe38cb) [§](https://postgr.es/c/bf82f4379) [§](https://postgr.es/c/28e858c0f)

  * Add a builtin platform-independent collation provider (Jeff Davis) [§](https://postgr.es/c/2d819a08a) [§](https://postgr.es/c/846311051) [§](https://postgr.es/c/f69319f2f) [§](https://postgr.es/c/9acae56ce)

This supports `C` and `C.UTF-8` collations. 

  * Add server variable [huge_pages_status](runtime-config-preset.md#GUC-HUGE-PAGES-STATUS) to report the use of huge pages by Postgres (Justin Pryzby) [§](https://postgr.es/c/a14354cac)

This is useful when [huge_pages](runtime-config-resource.md#GUC-HUGE-PAGES) is set to `try`. 

  * Add server variable to disable event triggers (Daniel Gustafsson) [§](https://postgr.es/c/7750fefdb)

The setting, [event_triggers](runtime-config-client.md#GUC-EVENT-TRIGGERS), allows for the temporary disabling of event triggers for debugging. 

  * Allow the [SLRU](monitoring-stats.md#MONITORING-PG-STAT-SLRU-VIEW "27.2.25. pg_stat_slru") cache sizes to be configured (Andrey Borodin, Dilip Kumar, Alvaro Herrera) [§](https://postgr.es/c/53c2a97a9)

The new server variables are [commit_timestamp_buffers](runtime-config-resource.md#GUC-COMMIT-TIMESTAMP-BUFFERS), [multixact_member_buffers](runtime-config-resource.md#GUC-MULTIXACT-MEMBER-BUFFERS), [multixact_offset_buffers](runtime-config-resource.md#GUC-MULTIXACT-OFFSET-BUFFERS), [notify_buffers](runtime-config-resource.md#GUC-NOTIFY-BUFFERS), [serializable_buffers](runtime-config-resource.md#GUC-SERIALIZABLE-BUFFERS), [subtransaction_buffers](runtime-config-resource.md#GUC-SUBTRANSACTION-BUFFERS), and [transaction_buffers](runtime-config-resource.md#GUC-TRANSACTION-BUFFERS). [commit_timestamp_buffers](runtime-config-resource.md#GUC-COMMIT-TIMESTAMP-BUFFERS), [transaction_buffers](runtime-config-resource.md#GUC-TRANSACTION-BUFFERS), and [subtransaction_buffers](runtime-config-resource.md#GUC-SUBTRANSACTION-BUFFERS) scale up automatically with [shared_buffers](runtime-config-resource.md#GUC-SHARED-BUFFERS). 




##### E.6.3.1.7. Streaming Replication and Recovery #

  * Add support for incremental file system backup (Robert Haas, Jakub Wartak, Tomas Vondra) [§](https://postgr.es/c/dc2123400) [§](https://postgr.es/c/f8ce4ed78)

Incremental backups can be created using [pg_basebackup](app-pgbasebackup.md "pg_basebackup")'s new `--incremental` option. The new application [pg_combinebackup](app-pgcombinebackup.md "pg_combinebackup") allows manipulation of base and incremental file system backups. 

  * Allow the creation of WAL summarization files (Robert Haas, Nathan Bossart, Hubert Depesz Lubaczewski) [§](https://postgr.es/c/174c48050) [§](https://postgr.es/c/d97ef756a) [§](https://postgr.es/c/f896057e4) [§](https://postgr.es/c/d9ef650fc)

These files record the block numbers that have changed within an [LSN](datatype-pg-lsn.md "8.20. pg_lsn Type") range and are useful for incremental file system backups. This is controlled by the server variables [summarize_wal](runtime-config-wal.md#GUC-SUMMARIZE-WAL) and [wal_summary_keep_time](runtime-config-wal.md#GUC-WAL-SUMMARY-KEEP-TIME), and introspected with [`pg_available_wal_summaries()`](functions-info.md#FUNCTIONS-WAL-SUMMARY "Table 9.92. WAL Summarization Information Functions"), `pg_wal_summary_contents()`, and `pg_get_wal_summarizer_state()`. 

  * Add the system identifier to file system [backup manifest](backup-manifest-format.md "Chapter 69. Backup Manifest Format") files (Amul Sul) [§](https://postgr.es/c/2041bc427)

This helps detect invalid WAL usage. 

  * Allow connection string value `dbname` to be written when [pg_basebackup](app-pgbasebackup.md "pg_basebackup") writes connection information to `postgresql.auto.conf` (Vignesh C, Hayato Kuroda) [§](https://postgr.es/c/a145f424d)

  * Add column [`pg_replication_slots`.`invalidation_reason`](view-pg-replication-slots.md "52.19. pg_replication_slots") to report the reason for invalid slots (Shveta Malik, Bharath Rupireddy) [§](https://postgr.es/c/007693f2a) [§](https://postgr.es/c/6ae701b43)

  * Add column [`pg_replication_slots`.`inactive_since`](view-pg-replication-slots.md "52.19. pg_replication_slots") to report slot inactivity duration (Bharath Rupireddy) [§](https://postgr.es/c/a11f330b5) [§](https://postgr.es/c/6d49c8d4b) [§](https://postgr.es/c/6f132ed69)

  * Add function [`pg_sync_replication_slots()`](functions-admin.md#FUNCTIONS-REPLICATION-TABLE "Table 9.99. Replication Management Functions") to synchronize logical replication slots (Hou Zhijie, Shveta Malik, Ajin Cherian, Peter Eisentraut) [§](https://postgr.es/c/ddd5f4f54) [§](https://postgr.es/c/7a424ece4)

  * Add the `failover` property to the [replication protocol](protocol-replication.md "53.4. Streaming Replication Protocol") (Hou Zhijie, Shveta Malik) [§](https://postgr.es/c/732924043)




##### E.6.3.1.8. [Logical Replication](logical-replication.md "Chapter 29. Logical Replication") #

  * Add application [pg_createsubscriber](app-pgcreatesubscriber.md "pg_createsubscriber") to create a logical replica from a physical standby server (Euler Taveira) [§](https://postgr.es/c/d44032d01)

  * Have [pg_upgrade](pgupgrade.md "pg_upgrade") migrate valid logical slots and subscriptions (Hayato Kuroda, Hou Zhijie, Vignesh C, Julien Rouhaud, Shlok Kyal) [§](https://postgr.es/c/29d0a77fa) [§](https://postgr.es/c/9a17be1e2)

This allows logical replication to continue quickly after the upgrade. This only works for old PostgreSQL clusters that are version 17 or later. 

  * Enable the failover of [logical slots](logical-replication-subscription.md#LOGICAL-REPLICATION-SUBSCRIPTION-SLOT "29.2.1. Replication Slot Management") (Hou Zhijie, Shveta Malik, Ajin Cherian) [§](https://postgr.es/c/c393308b6)

This is controlled by an optional fifth argument to [`pg_create_logical_replication_slot()`](functions-admin.md#FUNCTIONS-REPLICATION-TABLE "Table 9.99. Replication Management Functions"). 

  * Add server variable [sync_replication_slots](runtime-config-replication.md#GUC-SYNC-REPLICATION-SLOTS) to enable failover logical slot synchronization (Shveta Malik, Hou Zhijie, Peter Smith) [§](https://postgr.es/c/93db6cbda) [§](https://postgr.es/c/60c07820d)

  * Add logical replication failover control to [`CREATE/ALTER SUBSCRIPTION`](sql-createsubscription.md "CREATE SUBSCRIPTION") (Shveta Malik, Hou Zhijie, Ajin Cherian) [§](https://postgr.es/c/776621a5e) [§](https://postgr.es/c/22f7e61a6)

  * Allow the application of logical replication changes to use [hash](hash-index.md "64.6. Hash Indexes") indexes on the subscriber (Hayato Kuroda) [§](https://postgr.es/c/edca34243)

Previously only [btree](btree.md "64.1. B-Tree Indexes") indexes could be used for this purpose. 

  * Improve [logical decoding](logicaldecoding.md "Chapter 47. Logical Decoding") performance in cases where there are many subtransactions (Masahiko Sawada) [§](https://postgr.es/c/5bec1d6bc)

  * Restart apply workers if subscription owner's superuser privileges are revoked (Vignesh C) [§](https://postgr.es/c/79243de13)

This forces reauthentication. 

  * Add `flush` option to [`pg_logical_emit_message()`](functions-admin.md#FUNCTIONS-REPLICATION-TABLE "Table 9.99. Replication Management Functions") (Michael Paquier) [§](https://postgr.es/c/173b56f1e)

This makes the message durable. 

  * Allow specification of physical standbys that must be synchronized before they are visible to subscribers (Hou Zhijie, Shveta Malik) [§](https://postgr.es/c/bf279ddd1) [§](https://postgr.es/c/0f934b073)

The new server variable is [synchronized_standby_slots](runtime-config-replication.md#GUC-SYNCHRONIZED-STANDBY-SLOTS). 

  * Add worker type column to [`pg_stat_subscription`](monitoring-stats.md#MONITORING-PG-STAT-SUBSCRIPTION "27.2.8. pg_stat_subscription") (Peter Smith) [§](https://postgr.es/c/13aeaf079)




#### E.6.3.2. Utility Commands #

  * Add new [`COPY`](sql-copy.md "COPY") option `ON_ERROR ignore` to discard error rows (Damir Belyalov, Atsushi Torikoshi, Alex Shulgin, Jian He, Yugo Nagata) [§](https://postgr.es/c/9e2d87011) [§](https://postgr.es/c/b725b7eec) [§](https://postgr.es/c/40bbc8cf0) [§](https://postgr.es/c/a6d0fa5ef)

The default behavior is `ON_ERROR stop`. 

  * Add new `COPY` option `LOG_VERBOSITY` which reports `COPY FROM` ignored error rows (Bharath Rupireddy) [§](https://postgr.es/c/f5a227895)

  * Allow `COPY FROM` to report the number of skipped rows during processing (Atsushi Torikoshi) [§](https://postgr.es/c/729439607)

This appears in system view column [`pg_stat_progress_copy`.`tuples_skipped`](progress-reporting.md#COPY-PROGRESS-REPORTING "27.4.3. COPY Progress Reporting"). 

  * In `COPY FROM`, allow easy specification that all columns should be forced null or not null (Zhang Mingli) [§](https://postgr.es/c/f6d4c9cf1)

  * Allow partitioned tables to have identity columns (Ashutosh Bapat) [§](https://postgr.es/c/699586315)

  * Allow [exclusion constraints](ddl-constraints.md#DDL-CONSTRAINTS-EXCLUSION "5.5.6. Exclusion Constraints") on partitioned tables (Paul A. Jungwirth) [§](https://postgr.es/c/8c852ba9a)

As long as exclusion constraints compare partition key columns for equality, other columns can use exclusion constraint-specific comparisons. 

  * Add clearer [`ALTER TABLE`](sql-altertable.md "ALTER TABLE") method to set a column to the default statistics target (Peter Eisentraut) [§](https://postgr.es/c/4f622503d)

The new syntax is `ALTER TABLE ... SET STATISTICS DEFAULT`; using `SET STATISTICS -1` is still supported. 

  * Allow `ALTER TABLE` to change a column's generation expression (Amul Sul) [§](https://postgr.es/c/5d06e99a3)

The syntax is `ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION`. 

  * Allow specification of [table access methods](tableam.md "Chapter 61. Table Access Method Interface Definition") on partitioned tables (Justin Pryzby, Soumyadeep Chakraborty, Michael Paquier) [§](https://postgr.es/c/374c7a229) [§](https://postgr.es/c/e2395cdbe)

  * Add `DEFAULT` setting for `ALTER TABLE .. SET ACCESS METHOD` (Michael Paquier) [§](https://postgr.es/c/d61a6cad6)

  * Add support for [event triggers](sql-createeventtrigger.md "CREATE EVENT TRIGGER") that fire at connection time (Konstantin Knizhnik, Mikhail Gribkov) [§](https://postgr.es/c/e83d1b0c4)

  * Add event trigger support for [`REINDEX`](sql-reindex.md "REINDEX") (Garrett Thornburg, Jian He) [§](https://postgr.es/c/f21848de2)

  * Allow parenthesized syntax for [`CLUSTER`](sql-cluster.md "CLUSTER") options if a table name is not specified (Nathan Bossart) [§](https://postgr.es/c/cdaedfc96)




##### E.6.3.2.1. [`EXPLAIN`](sql-explain.md "EXPLAIN") #

  * Allow `EXPLAIN` to report optimizer memory usage (Ashutosh Bapat) [§](https://postgr.es/c/5de890e36)

The option is called `MEMORY`. 

  * Add `EXPLAIN` option `SERIALIZE` to report the cost of converting data for network transmission (Stepan Rutz, Matthias van de Meent) [§](https://postgr.es/c/06286709e)

  * Add local I/O block read/write timing statistics to `EXPLAIN`'s `BUFFERS` output (Nazir Bilal Yavuz) [§](https://postgr.es/c/295c36c0c)

  * Improve `EXPLAIN`'s display of SubPlan nodes and output parameters (Tom Lane, Dean Rasheed) [§](https://postgr.es/c/fd0398fcb)

  * Add JIT `deform_counter` details to `EXPLAIN` (Dmitry Dolgov) [§](https://postgr.es/c/5a3423ad8)




#### E.6.3.3. Data Types #

  * Allow the `interval` data type to support `+/-infinity` values (Joseph Koshakow, Jian He, Ashutosh Bapat) [§](https://postgr.es/c/519fc1bd9)

  * Allow the use of an [`ENUM`](datatype-enum.md "8.7. Enumerated Types") added via [`ALTER TYPE`](sql-altertype.md "ALTER TYPE") if the type was created in the same transaction (Tom Lane) [§](https://postgr.es/c/af1d39584)

This was previously disallowed. 




#### E.6.3.4. [MERGE](sql-merge.md "MERGE") #

  * Allow `MERGE` to modify updatable views (Dean Rasheed) [§](https://postgr.es/c/5f2e179bd)

  * Add `WHEN NOT MATCHED BY SOURCE` to `MERGE` (Dean Rasheed) [§](https://postgr.es/c/0294df2f1)

`WHEN NOT MATCHED` on target rows was already supported. 

  * Allow `MERGE` to use the `RETURNING` clause (Dean Rasheed) [§](https://postgr.es/c/c649fa24a)

The new `RETURNING` function `merge_action()` reports on the DML that generated the row. 




#### E.6.3.5. Functions #

  * Add function [`JSON_TABLE()`](functions-json.md#FUNCTIONS-SQLJSON-TABLE "9.16.4. JSON_TABLE") to convert `JSON` data to a table representation (Nikita Glukhov, Teodor Sigaev, Oleg Bartunov, Alexander Korotkov, Andrew Dunstan, Amit Langote, Jian He) [§](https://postgr.es/c/de3600452) [§](https://postgr.es/c/bb766cde6)

This function can be used in the `FROM` clause of `SELECT` queries as a tuple source. 

  * Add SQL/JSON constructor functions [`JSON()`](functions-json.md#FUNCTIONS-JSON-CREATION-TABLE "Table 9.47. JSON Creation Functions"), `JSON_SCALAR()`, and `JSON_SERIALIZE()` (Nikita Glukhov, Teodor Sigaev, Oleg Bartunov, Alexander Korotkov, Andrew Dunstan, Amit Langote) [§](https://postgr.es/c/03734a7fe)

  * Add SQL/JSON query functions [`JSON_EXISTS()`](functions-json.md#FUNCTIONS-SQLJSON-QUERYING "Table 9.52. SQL/JSON Query Functions"), `JSON_QUERY()`, and `JSON_VALUE()` (Nikita Glukhov, Teodor Sigaev, Oleg Bartunov, Alexander Korotkov, Andrew Dunstan, Amit Langote, Peter Eisentraut, Jian He) [§](https://postgr.es/c/aaaf9449e) [§](https://postgr.es/c/1edb3b491) [§](https://postgr.es/c/6185c9737) [§](https://postgr.es/c/c0fc07518) [§](https://postgr.es/c/ef744ebb7)

  * Add [jsonpath](functions-json.md#FUNCTIONS-SQLJSON-PATH-OPERATORS "9.16.2.3. SQL/JSON Path Operators and Methods") methods to convert `JSON` values to other `JSON` data types (Jeevan Chalke) [§](https://postgr.es/c/66ea94e8e)

The jsonpath methods are `.bigint()`, `.boolean()`, `.date()`, `.decimal([precision [, scale]])`, `.integer()`, `.number()`, `.string()`, `.time()`, `.time_tz()`, `.timestamp()`, and `.timestamp_tz()`. 

  * Add [`to_timestamp()`](functions-formatting.md#FUNCTIONS-FORMATTING-TABLE "Table 9.26. Formatting Functions") time zone format specifiers (Tom Lane) [§](https://postgr.es/c/8ba6fdf90)

`TZ` accepts time zone abbreviations or numeric offsets, while `OF` accepts only numeric offsets. 

  * Allow the session [time zone](runtime-config-client.md#GUC-TIMEZONE) to be specified by `AT LOCAL` (Vik Fearing) [§](https://postgr.es/c/97957fdba)

This is useful when converting adding and removing time zones from time stamps values, rather than specifying the literal session time zone. 

  * Add functions [`uuid_extract_timestamp()`](functions-uuid.md "9.14. UUID Functions") and `uuid_extract_version()` to return UUID information (Andrey Borodin) [§](https://postgr.es/c/794f10f6b)

  * Add functions to generate random numbers in a specified range (Dean Rasheed) [§](https://postgr.es/c/e6341323a)

The functions are [`random(min, max)`](functions-math.md#FUNCTIONS-MATH-RANDOM-TABLE "Table 9.6. Random Functions") and they take values of type `integer`, `bigint`, and `numeric`. 

  * Add functions to convert integers to binary and octal strings (Eric Radman, Nathan Bossart) [§](https://postgr.es/c/260a1f18d)

The functions are [`to_bin()`](functions-string.md#FUNCTIONS-STRING-OTHER "Table 9.10. Other String Functions and Operators") and `to_oct()`. 

  * Add Unicode informational functions (Jeff Davis) [§](https://postgr.es/c/a02b37fc0)

Function [`unicode_version()`](functions-info.md#FUNCTIONS-INFO-VERSION "9.27.11. Version Information Functions") returns the Unicode version, `icu_unicode_version()` returns the ICU version, and `unicode_assigned()` returns if the characters are assigned Unicode codepoints. 

  * Add function [`xmltext()`](functions-xml.md#FUNCTIONS-PRODUCING-XML-XMLTEXT "9.15.1.1. xmltext") to convert text to a single `XML` text node (Jim Jones) [§](https://postgr.es/c/526fe0d79)

  * Add function [`to_regtypemod()`](functions-info.md#FUNCTIONS-INFO-CATALOG-TABLE "Table 9.74. System Catalog Information Functions") to return the type modifier of a type specification (David Wheeler, Erik Wienhold) [§](https://postgr.es/c/1218ca995)

  * Add [`pg_basetype()`](functions-info.md#FUNCTIONS-INFO-CATALOG-TABLE "Table 9.74. System Catalog Information Functions") function to return a domain's base type (Steve Chavez) [§](https://postgr.es/c/b154d8a6d)

  * Add function [`pg_column_toast_chunk_id()`](functions-admin.md#FUNCTIONS-ADMIN-DBSIZE "Table 9.100. Database Object Size Functions") to return a value's [TOAST](storage-toast.md "65.2. TOAST") identifier (Yugo Nagata) [§](https://postgr.es/c/d1162cfda)

This returns `NULL` if the value is not stored in TOAST. 




#### E.6.3.6. [PL/pgSQL](plpgsql.md "Chapter 41. PL/pgSQL — SQL Procedural Language") #

  * Allow plpgsql [`%TYPE`](plpgsql-declarations.md#PLPGSQL-DECLARATION-TYPE "41.3.3. Copying Types") and `%ROWTYPE` specifications to represent arrays of non-array types (Quan Zongliang, Pavel Stehule) [§](https://postgr.es/c/5e8674dc8)

  * Allow plpgsql `%TYPE` specification to reference composite column (Tom Lane) [§](https://postgr.es/c/43b46aae1)




#### E.6.3.7. [libpq](libpq.md "Chapter 32. libpq — C Library") #

  * Add libpq function to change role passwords (Joe Conway) [§](https://postgr.es/c/a7be2a6c2)

The new function, [`PQchangePassword()`](libpq-misc.md#LIBPQ-PQCHANGEPASSWORD), hashes the new password before sending it to the server. 

  * Add libpq functions to close portals and prepared statements (Jelte Fennema-Nio) [§](https://postgr.es/c/28b572656)

The functions are [`PQclosePrepared()`](libpq-exec.md#LIBPQ-PQCLOSEPREPARED), [`PQclosePortal()`](libpq-exec.md#LIBPQ-PQCLOSEPORTAL), [`PQsendClosePrepared()`](libpq-async.md#LIBPQ-PQSENDCLOSEPREPARED), and [`PQsendClosePortal()`](libpq-async.md#LIBPQ-PQSENDCLOSEPORTAL). 

  * Add libpq API which allows for blocking and non-blocking [cancel requests](libpq-cancel.md "32.7. Canceling Queries in Progress"), with encryption if already in use (Jelte Fennema-Nio) [§](https://postgr.es/c/61461a300)

Previously only blocking, unencrypted cancel requests were supported. 

  * Add libpq function [`PQsocketPoll()`](libpq-connect.md#LIBPQ-PQSOCKETPOLL) to allow polling of network sockets (Tristan Partin, Tom Lane) [§](https://postgr.es/c/f5e4dedfa) [§](https://postgr.es/c/105024a47)

  * Add libpq function [`PQsendPipelineSync()`](libpq-pipeline-mode.md#LIBPQ-PQSENDPIPELINESYNC) to send a pipeline synchronization point (Anton Kirilov) [§](https://postgr.es/c/4794c2d31)

This is similar to [`PQpipelineSync()`](libpq-pipeline-mode.md#LIBPQ-PQPIPELINESYNC) but it does not flush to the server unless the size threshold of the output buffer is reached. 

  * Add libpq function [`PQsetChunkedRowsMode()`](libpq-single-row-mode.md#LIBPQ-PQSETCHUNKEDROWSMODE) to allow retrieval of results in chunks (Daniel Vérité) [§](https://postgr.es/c/4643a2b26)

  * Allow TLS connections without requiring a network round-trip negotiation (Greg Stark, Heikki Linnakangas, Peter Eisentraut, Michael Paquier, Daniel Gustafsson) [§](https://postgr.es/c/d39a49c1e) [§](https://postgr.es/c/91044ae4b) [§](https://postgr.es/c/44e27f0a6) [§](https://postgr.es/c/d80f2ce29) [§](https://postgr.es/c/03a0e0d4b) [§](https://postgr.es/c/17a834a04) [§](https://postgr.es/c/407e0b023) [§](https://postgr.es/c/fb5718f35)

This is enabled with the client-side option [`sslnegotiation=direct`](libpq-connect.md#LIBPQ-CONNECT-SSLNEGOTIATION), requires ALPN, and only works on PostgreSQL 17 and later servers. 




#### E.6.3.8. [psql](app-psql.md "psql") #

  * Improve psql display of default and empty privileges (Erik Wienhold, Laurenz Albe) [§](https://postgr.es/c/d1379ebf4)

Command `\dp` now displays `(none)` for empty privileges; default still displays as empty. 

  * Have backslash commands honor `\pset null` (Erik Wienhold, Laurenz Albe) [§](https://postgr.es/c/d1379ebf4)

Previously `\pset null` was ignored. 

  * Allow psql's `\watch` to stop after a minimum number of rows returned (Greg Sabino Mullane) [§](https://postgr.es/c/f347ec76e)

The parameter is `min_rows`. 

  * Allow psql connection attempts to be canceled with control-C (Tristan Partin) [§](https://postgr.es/c/cafe10565)

  * Allow psql to honor `FETCH_COUNT` for non-`SELECT` queries (Daniel Vérité) [§](https://postgr.es/c/90f517821)

  * Improve psql tab completion (Dagfinn Ilmari Mannsåker, Gilles Darold, Christoph Heiss, Steve Chavez, Vignesh C, Pavel Borisov, Jian He) [§](https://postgr.es/c/c951e9042) [§](https://postgr.es/c/d16eb83ab) [§](https://postgr.es/c/cd3424748) [§](https://postgr.es/c/816f10564) [§](https://postgr.es/c/927332b95) [§](https://postgr.es/c/f1bb9284f) [§](https://postgr.es/c/304b6b1a6) [§](https://postgr.es/c/2800fbb2b)




#### E.6.3.9. Server Applications #

  * Add application [pg_walsummary](app-pgwalsummary.md "pg_walsummary") to dump WAL summary files (Robert Haas) [§](https://postgr.es/c/ee1bfd168)

  * Allow [pg_dump](app-pgdump.md "pg_dump")'s large objects to be restorable in batches (Tom Lane) [§](https://postgr.es/c/a45c78e32)

This allows the restoration of many large objects to avoid transaction limits and to be restored in parallel. 

  * Add pg_dump option `--exclude-extension` (Ayush Vatsa) [§](https://postgr.es/c/522ed12f7)

  * Allow [pg_dump](app-pgdump.md "pg_dump"), [pg_dumpall](app-pg-dumpall.md "pg_dumpall"), and [pg_restore](app-pgrestore.md "pg_restore") to specify include/exclude objects in a file (Pavel Stehule, Daniel Gustafsson) [§](https://postgr.es/c/a5cf808be)

The option is called `--filter`. 

  * Add the `--sync-method` parameter to several client applications (Justin Pryzby, Nathan Bossart) [§](https://postgr.es/c/8c16ad3b4)

The applications are [initdb](app-initdb.md "initdb"), [pg_basebackup](app-pgbasebackup.md "pg_basebackup"), [pg_checksums](app-pgchecksums.md "pg_checksums"), [pg_dump](app-pgdump.md "pg_dump"), [pg_rewind](app-pgrewind.md "pg_rewind"), and [pg_upgrade](pgupgrade.md "pg_upgrade"). 

  * Add [pg_restore](app-pgrestore.md "pg_restore") option `--transaction-size` to allow object restores in transaction batches (Tom Lane) [§](https://postgr.es/c/959b38d77)

This allows the performance benefits of transaction batches without the problems of excessively large transaction blocks. 

  * Change [pgbench](pgbench.md "pgbench") debug mode option from `-d` to `--debug` (Greg Sabino Mullane) [§](https://postgr.es/c/3ff01b2b6)

Option `-d` is now used for the database name, and the new `--dbname` option can be used as well. 

  * Add pgbench option `--exit-on-abort` to exit after any client aborts (Yugo Nagata) [§](https://postgr.es/c/3c662643c)

  * Add pgbench command `\syncpipeline` to allow sending of sync messages (Anthonin Bonnefoy) [§](https://postgr.es/c/94edfe250)

  * Allow [pg_archivecleanup](pgarchivecleanup.md "pg_archivecleanup") to remove backup history files (Atsushi Torikoshi) [§](https://postgr.es/c/3f8c98d0b)

The option is `--clean-backup-history`. 

  * Add some long options to pg_archivecleanup (Atsushi Torikoshi) [§](https://postgr.es/c/dd7c60f19)

The long options are `--debug`, `--dry-run`, and `--strip-extension`. 

  * Allow [pg_basebackup](app-pgbasebackup.md "pg_basebackup") and [pg_receivewal](app-pgreceivewal.md "pg_receivewal") to use dbname in their connection specification (Jelte Fennema-Nio) [§](https://postgr.es/c/cca97ce6a)

This is useful for connection poolers that are sensitive to the database name. 

  * Add [pg_upgrade](pgupgrade.md "pg_upgrade") option `--copy-file-range` (Thomas Munro) [§](https://postgr.es/c/d93627bcb)

This is supported on Linux and FreeBSD. 

  * Allow [reindexdb](app-reindexdb.md "reindexdb") `--index` to process indexes from different tables in parallel (Maxim Orlov, Svetlana Derevyanko, Alexander Korotkov) [§](https://postgr.es/c/47f99a407)

  * Allow [reindexdb](app-reindexdb.md "reindexdb"), [vacuumdb](app-vacuumdb.md "vacuumdb"), and [clusterdb](app-clusterdb.md "clusterdb") to process objects in all databases matching a pattern (Nathan Bossart) [§](https://postgr.es/c/24c928ad9) [§](https://postgr.es/c/648928c79) [§](https://postgr.es/c/1b49d56d3)

The new option `--all` controls this behavior. 




#### E.6.3.10. Source Code #

  * Remove support for OpenSSL 1.0.1 (Michael Paquier) [§](https://postgr.es/c/8e278b657)

  * Allow tests to pass in OpenSSL FIPS mode (Peter Eisentraut) [§](https://postgr.es/c/284cbaea7) [§](https://postgr.es/c/3c44e7d8d)

  * Use CPU AVX-512 instructions for bit counting (Paul Amonson, Nathan Bossart, Ants Aasma) [§](https://postgr.es/c/792752af4) [§](https://postgr.es/c/41c51f0c6)

  * Require LLVM version 10 or later (Thomas Munro) [§](https://postgr.es/c/820b5af73)

  * Use native CRC instructions on 64-bit LoongArch CPUs (Xudong Yang) [§](https://postgr.es/c/4d14ccd6a)

  * Remove AIX support (Heikki Linnakangas) [§](https://postgr.es/c/0b16bb877)

  * Remove the Microsoft Visual Studio-specific PostgreSQL build option (Michael Paquier) [§](https://postgr.es/c/1301c80b2)

Meson is now the only available method for Visual Studio builds. 

  * Remove configure option `--disable-thread-safety` (Thomas Munro, Heikki Linnakangas) [§](https://postgr.es/c/68a4b58ec) [§](https://postgr.es/c/ce0b0fa3e)

We now assume all supported platforms have sufficient thread support. 

  * Remove configure option `--with-CC` (Heikki Linnakangas) [§](https://postgr.es/c/1c1eec0f2)

Setting the `CC` environment variable is now the only supported method for specifying the compiler. 

  * User-defined data type receive functions will no longer receive their data null-terminated (David Rowley) [§](https://postgr.es/c/f0efa5aec)

  * Add incremental `JSON` parser for use with huge `JSON` documents (Andrew Dunstan) [§](https://postgr.es/c/3311ea86e)

  * Convert top-level `README` file to Markdown (Nathan Bossart) [§](https://postgr.es/c/363eb0599)

  * Remove no longer needed top-level `INSTALL` file (Tom Lane) [§](https://postgr.es/c/e2b73f4a4)

  * Remove make's `distprep` option (Peter Eisentraut) [§](https://postgr.es/c/721856ff2)

  * Add make support for Android shared libraries (Peter Eisentraut) [§](https://postgr.es/c/79b03dbb3)

  * Add backend support for injection points (Michael Paquier) [§](https://postgr.es/c/d86d20f0b) [§](https://postgr.es/c/37b369dc6) [§](https://postgr.es/c/f587338de) [§](https://postgr.es/c/bb93640a6)

This is used for server debugging and they must be enabled at server compile time. 

  * Add dynamic shared memory registry (Nathan Bossart) [§](https://postgr.es/c/8b2bcf3f2)

This allows shared libraries which are not initialized at startup to coordinate dynamic shared memory access. 

  * Fix `emit_log_hook` to use the same time value as other log records for the same query (Kambam Vinay, Michael Paquier) [§](https://postgr.es/c/2a217c371)

  * Improve documentation for using `jsonpath` for predicate checks (David Wheeler) [§](https://postgr.es/c/7014c9a4b)




#### E.6.3.11. Additional Modules #

  * Allow joins with non-join qualifications to be pushed down to foreign servers and custom scans (Richard Guo, Etsuro Fujita) [§](https://postgr.es/c/9e9931d2b)

Foreign data wrappers and custom scans will need to be modified to handle these cases. 

  * Allow pushdown of `EXISTS` and `IN` subqueries to [postgres_fdw](postgres-fdw.md "F.36. postgres_fdw — access data stored in external PostgreSQL servers") foreign servers (Alexander Pyhalov) [§](https://postgr.es/c/824dbea3e)

  * Increase the default foreign data wrapper tuple cost (David Rowley, Umair Shahid) [§](https://postgr.es/c/cac169d68) [§](https://postgr.es/c/f7f694b21)

This value is used by the optimizer. 

  * Allow [dblink](dblink.md "F.11. dblink — connect to other PostgreSQL databases") database operations to be interrupted (Noah Misch) [§](https://postgr.es/c/d3c5f37dd)

  * Allow the creation of hash indexes on [ltree](ltree.md "F.22. ltree — hierarchical tree-like data type") columns (Tommy Pavlicek) [§](https://postgr.es/c/485f0aa85)

This also enables hash join and hash aggregation on ltree columns. 

  * Allow [unaccent](unaccent.md "F.46. unaccent — a text search dictionary which removes diacritics") character translation rules to contain whitespace and quotes (Michael Paquier) [§](https://postgr.es/c/59f47fb98)

The syntax for the `unaccent.rules` file has changed. 

  * Allow [amcheck](amcheck.md "F.1. amcheck — tools to verify table and index consistency") to check for unique constraint violations using new option `--checkunique` (Anastasia Lubennikova, Pavel Borisov, Maxim Orlov) [§](https://postgr.es/c/5ae208720)

  * Allow [citext](citext.md "F.9. citext — a case-insensitive character string type") tests to pass in OpenSSL FIPS mode (Peter Eisentraut) [§](https://postgr.es/c/3c551ebed)

  * Allow [pgcrypto](pgcrypto.md "F.26. pgcrypto — cryptographic functions") tests to pass in OpenSSL FIPS mode (Peter Eisentraut) [§](https://postgr.es/c/795592865)

  * Remove some unused [SPI](spi.md "Chapter 45. Server Programming Interface") macros (Bharath Rupireddy) [§](https://postgr.es/c/75680c3d8)

  * Allow [`ALTER OPERATOR`](sql-alteroperator.md "ALTER OPERATOR") to set more optimization attributes (Tommy Pavlicek) [§](https://postgr.es/c/2b5154bea)

This is useful for extensions. 

  * Allow extensions to define [custom wait events](xfunc-c.md#XFUNC-ADDIN-WAIT-EVENTS "36.10.12. Custom Wait Events") (Masahiro Ikeda) [§](https://postgr.es/c/c9af05465) [§](https://postgr.es/c/c8e318b1b) [§](https://postgr.es/c/d61f2538a) [§](https://postgr.es/c/c789f0f6c)

Custom wait events have been added to [postgres_fdw](postgres-fdw.md "F.36. postgres_fdw — access data stored in external PostgreSQL servers") and [dblink](dblink.md "F.11. dblink — connect to other PostgreSQL databases"). 

  * Add [pg_buffercache](pgbuffercache.md "F.25. pg_buffercache — inspect PostgreSQL buffer cache state") function `pg_buffercache_evict()` to allow shared buffer eviction (Palak Chaturvedi, Thomas Munro) [§](https://postgr.es/c/13453eedd)

This is useful for testing. 




##### E.6.3.11.1. [pg_stat_statements](pgstatstatements.md "F.30. pg_stat_statements — track statistics of SQL planning and execution") #

  * Replace [`CALL`](sql-call.md "CALL") parameters in pg_stat_statements with placeholders (Sami Imseih) [§](https://postgr.es/c/11c34b342)

  * Replace savepoint names stored in `pg_stat_statements` with placeholders (Greg Sabino Mullane) [§](https://postgr.es/c/31de7e60d)

This greatly reduces the number of entries needed to record [`SAVEPOINT`](sql-savepoint.md "SAVEPOINT"), [`RELEASE SAVEPOINT`](sql-release-savepoint.md "RELEASE SAVEPOINT"), and [`ROLLBACK TO SAVEPOINT`](sql-rollback-to.md "ROLLBACK TO SAVEPOINT") commands. 

  * Replace the two-phase commit GIDs stored in `pg_stat_statements` with placeholders (Michael Paquier) [§](https://postgr.es/c/638d42a3c)

This greatly reduces the number of entries needed to record [`PREPARE TRANSACTION`](sql-prepare-transaction.md "PREPARE TRANSACTION"), [`COMMIT PREPARED`](sql-commit-prepared.md "COMMIT PREPARED"), and [`ROLLBACK PREPARED`](sql-rollback-prepared.md "ROLLBACK PREPARED"). 

  * Track [`DEALLOCATE`](sql-deallocate.md "DEALLOCATE") in `pg_stat_statements` (Dagfinn Ilmari Mannsåker, Michael Paquier) [§](https://postgr.es/c/bb45156f3)

`DEALLOCATE` names are stored in `pg_stat_statements` as placeholders. 

  * Add local I/O block read/write timing statistics columns of `pg_stat_statements` (Nazir Bilal Yavuz) [§](https://postgr.es/c/295c36c0c) [§](https://postgr.es/c/5147ab1dd)

The new columns are `local_blk_read_time` and `local_blk_write_time`. 

  * Add JIT deform_counter details to `pg_stat_statements` (Dmitry Dolgov) [§](https://postgr.es/c/5a3423ad8)

  * Add optional fourth argument (`minmax_only`) to `pg_stat_statements_reset()` to allow for the resetting of only min/max statistics (Andrei Zubkov) [§](https://postgr.es/c/dc9f8a798)

This argument defaults to `false`. 

  * Add `pg_stat_statements` columns `stats_since` and `minmax_stats_since` to track entry creation time and last min/max reset time (Andrei Zubkov) [§](https://postgr.es/c/dc9f8a798)




### E.6.4. Acknowledgments #

The following individuals (in alphabetical order) have contributed to this release as patch authors, committers, reviewers, testers, or reporters of issues. 

Abhijit Menon-Sen  
---  
Adnan Dautovic  
Aidar Imamov  
Ajin Cherian  
Akash Shankaran  
Akshat Jaimini  
Alaa Attya  
Aleksander Alekseev  
Aleksej Orlov  
Alena Rybakina  
Alex Hsieh  
Alex Malek  
Alex Shulgin  
Alex Work  
Alexander Korotkov  
Alexander Kozhemyakin  
Alexander Kuzmenkov  
Alexander Lakhin  
Alexander Pyhalov  
Alexey Palazhchenko  
Alfons Kemper  
Álvaro Herrera  
Amadeo Gallardo  
Amit Kapila  
Amit Langote  
Amul Sul  
Anastasia Lubennikova  
Anatoly Zaretsky  
Andreas Karlsson  
Andreas Ulbrich  
Andrei Lepikhov  
Andrei Zubkov  
Andres Freund  
Andrew Alsup  
Andrew Atkinson  
Andrew Bille  
Andrew Dunstan  
Andrew Kane  
Andrey Borodin  
Andrey Rachitskiy  
Andrey Sokolov  
Andy Fan  
Anthonin Bonnefoy  
Anthony Hsu  
Anton Kirilov  
Anton Melnikov  
Anton Voloshin  
Antonin Houska  
Ants Aasma  
Antti Lampinen  
Aramaki Zyake  
Artem Anisimov  
Artur Zakirov  
Ashutosh Bapat  
Ashutosh Sharma  
Atsushi Torikoshi  
Attila Gulyás  
Ayush Tiwari  
Ayush Vatsa  
Bartosz Chrol  
Benoît Ryder  
Bernd Helmle  
Bertrand Drouvot  
Bharath Rupireddy  
Bo Andreson  
Boshomi Phenix  
Bowen Shi  
Boyu Yang  
Bruce Momjian  
Cameron Vogt  
Cary Huang  
Cédric Villemain  
Changhong Fei  
Chantal Keller  
Chapman Flack  
Chengxi Sun  
Chris Travers  
Christian Maurer  
Christian Stork  
Christoph Berg  
Christoph Heiss  
Christophe Courtois  
Christopher Kline  
Claudio Freire  
Colin Caine  
Corey Huinker  
Curt Kolovson  
Dag Lem  
Dagfinn Ilmari Mannsåker  
Damir Belyalov  
Daniel Fredouille  
Daniel Gustafsson  
Daniel Shelepanov  
Daniel Vérité  
Daniel Westermann  
Darren Rush  
Dave Cramer  
Dave Page  
David Christensen  
David Cook  
David G. Johnston  
David Geier  
David Hillman  
David Perez  
David Rowley  
David Steele  
David Wheeler  
David Zhang  
Dean Rasheed  
Denis Erokhin  
Denis Laxalde  
Devrim Gündüz  
Dilip Kumar  
Dimitrios Apostolou  
Dmitry Dolgov  
Dmitry Koval  
Dmitry Vasiliev  
Dominique Devienne  
Dong Wook Lee  
Donghang Lin  
Dongming Liu  
Drew Callahan  
Drew Kimball  
Dzmitry Jachnik  
Egor Chindyaskin  
Egor Rogov  
Ekaterina Kiryanova  
Elena Indrupskaya  
Elizabeth Christensen  
Emre Hasegeli  
Eric Cyr  
Eric Mutta  
Eric Radman  
Eric Ridge  
Erik Rijkers  
Erik Wienhold  
Erki Eessaar  
Ethan Mertz  
Etsuro Fujita  
Eugen Konkov  
Euler Taveira  
Evan Macbeth  
Evgeny Morozov  
Fabien Coelho  
Fabrízio de Royes Mello  
Farias de Oliveira  
Feliphe Pozzer  
Fire Emerald  
Flavien Guedez  
Floris Van Nee  
Francesco Degrassi  
Frank Streitzig  
Gabriele Bartolini  
Garrett Thornburg  
Gavin Flower  
Gavin Panella  
Gilles Darold  
Gilles Parc  
Grant Gryczan  
Greg Nancarrow  
Greg Sabino Mullane  
Greg Stark  
Gurjeet Singh  
Haiying Tang  
Hajime Matsunaga  
Hal Takahara  
Hanefi Onaldi  
Hannu Krosing  
Hans Buschmann  
Hao Wu  
Hao Zhang  
Hayato Kuroda  
Heikki Linnakangas  
Hemanth Sandrana  
Himanshu Upadhyaya  
Hironobu Suzuki  
Holger Reise  
Hongxu Ma  
Hongyu Song  
Horst Reiterer  
Hubert Lubaczewski  
Hywel Carver  
Ian Barwick  
Ian Ilyasov  
Ilya Nenashev  
Isaac Morland  
Israel Barth Rubio  
Ivan Kartyshov  
Ivan Kolombet  
Ivan Lazarev  
Ivan Panchenko  
Ivan Trofimov  
Jacob Champion  
Jacob Speidel  
Jacques Combrink  
Jaime Casanova  
Jakub Wartak  
James Coleman  
James Pang  
Jani Rahkola  
Japin Li  
Jeevan Chalke  
Jeff Davis  
Jeff Janes  
Jelte Fennema-Nio  
Jeremy Schneider  
Jian Guo  
Jian He  
Jim Jones  
Jim Keener  
Jim Nasby  
Jingtang Zhang  
Jingxian Li  
Jingzhou Fu  
Joe Conway  
Joel Jacobson  
John Ekins  
John Hsu  
John Morris  
John Naylor  
John Russell  
Jonathan Katz  
Jordi Gutiérrez  
Joseph Koshakow  
Josh Kupershmidt  
Joshua D. Drake  
Joshua Uyehara  
Jubilee Young  
Julien Rouhaud  
Junwang Zhao  
Justin Pryzby  
Kaido Vaikla  
Kambam Vinay  
Karen Talarico  
Karina Litskevich  
Karl O. Pinc  
Kashif Zeeshan  
Kim Johan Andersson  
Kirill Reshke  
Kirk Parker  
Kirk Wolak  
Kisoon Kwon  
Koen De Groote  
Kohei KaiGai  
Kong Man  
Konstantin Knizhnik  
Kouhei Sutou  
Krishnakumar R  
Kuntal Ghosh  
Kurt Roeckx  
Kyotaro Horiguchi  
Lang Liu  
Lars Kanis  
Laurenz Albe  
Lauri Laanmets  
Legs Mansion  
Lukas Fittl  
Magnus Hagander  
Mahendrakar Srinivasarao  
Maiquel Grassi  
Manos Emmanouilidis  
Marcel Hofstetter  
Marcos Pegoraro  
Marian Krucina  
Marina Polyakova  
Mark Dilger  
Mark Guertin  
Mark Sloan  
Markus Winand  
Marlene Reiterer  
Martín Marqués  
Martin Nash  
Martin Schlossarek  
Masahiko Sawada  
Masahiro Ikeda  
Masaki Kuwamura  
Masao Fujii  
Mason Sharp  
Matheus Alcantara  
Mats Kindahl  
Matthias Kuhn  
Matthias van de Meent  
Maxim Boguk  
Maxim Orlov  
Maxim Yablokov  
Maxime Boyer  
Melanie Plageman  
Melih Mutlu  
Merlin Moncure  
Micah Gate  
Michael Banck  
Michael Bondarenko  
Michael Paquier  
Michael Wang  
Michael Zhilin  
Michail Nikolaev  
Michal Bartak  
Michal Kleczek  
Mikhail Gribkov  
Mingli Zhang  
Miroslav Bendik  
Mitsuru Hinata  
Moaaz Assali  
Muralikrishna Bandaru  
Nathan Bossart  
Nazir Bilal Yavuz  
Neil Tiffin  
Ngigi Waithaka  
Nikhil Benesch  
Nikhil Raj  
Nikita Glukhov  
Nikita Kalinin  
Nikita Malakhov  
Nikolay Samokhvalov  
Nikolay Shaplov  
Nisha Moond  
Nishant Sharma  
Nitin Jadhav  
Noah Misch  
Noriyoshi Shinoda  
Ole Peder Brandtzæg  
Oleg Bartunov  
Oleg Sibiryakov  
Oleg Tselebrovskiy  
Olleg Samoylov  
Onder Kalaci  
Ondrej Navratil  
Pablo Kharo  
Palak Chaturvedi  
Pantelis Theodosiou  
Paul Amonson  
Paul Jungwirth  
Pavel Borisov  
Pavel Kulakov  
Pavel Luzanov  
Pavel Stehule  
Pavlo Golub  
Pedro Gallegos  
Pete Storer  
Peter Eisentraut  
Peter Geoghegan  
Peter Smith  
Philip Warner  
Philipp Salvisberg  
Pierre Ducroquet  
Pierre Fortin  
Przemyslaw Sztoch  
Quynh Tran  
Raghuveer Devulapalli  
Ranier Vilela  
Reid Thompson  
Rian McGuire  
Richard Guo  
Richard Vesely  
Ridvan Korkmaz  
Robert Haas  
Robert Scott  
Robert Treat  
Roberto Mello  
Robins Tharakan  
Roman Lozko  
Ronan Dunklau  
Rui Zhao  
Ryo Matsumura  
Ryoga Yoshida  
Sameer Kumar  
Sami Imseih  
Samuel Dussault  
Sanjay Minni  
Satoru Koizumi  
Sebastian Skalacki  
Sergei Glukhov  
Sergei Kornilov  
Sergey Prokhorenko  
Sergey Sargsyan  
Sergey Shinderuk  
Shaozhong Shi  
Shaun Thomas  
Shay Rojansky  
Shihao Zhong  
Shinya Kato  
Shlok Kyal  
Shruthi Gowda  
Shubham Khanna  
Shulin Zhou  
Shveta Malik  
Simon Riggs  
Soumyadeep Chakraborty  
Sravan Velagandula  
Stan Hu  
Stepan Neretin  
Stepan Rutz  
Stéphane Schildknecht  
Stephane Tachoires  
Stephen Frost  
Steve Atkins  
Steve Chavez  
Suraj Khamkar  
Suraj Kharage  
Svante Richter  
Svetlana Derevyanko  
Sylvain Frandaz  
Takayuki Tsunakawa  
Tatsuo Ishii  
Tatsuro Yamada  
Tender Wang  
Teodor Sigaev  
Thom Brown  
Thomas Munro  
Tim Carey-Smith  
Tim Needham  
Tim Palmer  
Tobias Bussmann  
Tom Lane  
Tomas Vondra  
Tommy Pavlicek  
Tomonari Katsumata  
Tristan Partin  
Tristen Raab  
Tung Nguyen  
Umair Shahid  
Uwe Binder  
Valerie Woolard  
Vallimaharajan G  
Vasya Boytsov  
Victor Wagner  
Victor Yegorov  
Victoria Shepard  
Vidushi Gupta  
Vignesh C  
Vik Fearing  
Viktor Leis  
Vinayak Pokale  
Vitaly Burovoy  
Vojtech Benes  
Wei Sun  
Wei Wang  
Wenjiang Zhang  
Will Mortensen  
Willi Mann  
Wolfgang Walther  
Xiang Liu  
Xiaoran Wang  
Xing Guo  
Xudong Yang  
Yahor Yuzefovich  
Yajun Hu  
Yaroslav Saburov  
Yong Li  
Yongtao Huang  
Yugo Nagata  
Yuhang Qiu  
Yuki Seino  
Yura Sokolov  
Yurii Rashkovskii  
Yuuki Fujii  
Yuya Watari  
Yves Colin  
Zhihong Yu  
Zhijie Hou  
Zongliang Quan  
Zubeyr Eryilmaz  
Zuming Jiang  
  
* * *

[Prev](release-17-1.md "E.5. Release 17.1") | [Up](release.md "Appendix E. Release Notes")|  [Next](release-prior.md "E.7. Prior Releases")  
---|---|---  
E.5. Release 17.1 | [Home](index.md "PostgreSQL 17.5 Documentation")|  E.7. Prior Releases
