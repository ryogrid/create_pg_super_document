# pgstat_prep_database_pending

## Location
src/backend/utils/activity/pgstat_database.c: 333 - 353

## Overview
pgstat_prep_database_pending is a function that finds or creates a local pending statistics entry for a specified database OID.

## Definition
PgStat_StatDBEntry *pgstat_prep_database_pending(Oid dboid)

## Detailed Description
This function serves as a helper routine in PostgreSQL's statistics system to obtain a pending statistics entry for a database. It acts as a wrapper around the more general pgstat_prep_pending_entry function, specifically configured for database-level statistics. The function includes an assertion to ensure that statistics are not being reported on database objects before a connection to a database has been established. It uses the PGSTAT_KIND_DATABASE constant to specify that this is a database-level statistics entry and returns the pending statistics structure that can be used to accumulate statistics before they are sent to the statistics collector.

## Parameters / Member Variables
- : Oid (Object Identifier) of the database for which to prepare the statistics entry

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_EntryRef (statistics entry reference structure)
  - [pgstat_prep_pending_entry](pgstat_prep_pending_entry.md) (general function for preparing pending statistics entries)
  - PGSTAT_KIND_DATABASE (constant indicating database-level statistics)
- Called from (representative examples):
  - [pgstat_report_recovery_conflict](pgstat_report_recovery_conflict.md) (from src/backend/utils/activity/pgstat_database.c:89)
  - [pgstat_report_deadlock](pgstat_report_deadlock.md) (from src/backend/utils/activity/pgstat_database.c:132)
  - [pgstat_report_tempfile](pgstat_report_tempfile.md) (from src/backend/utils/activity/pgstat_database.c:182)
  - [pgstat_report_connect](pgstat_report_connect.md) (from src/backend/utils/activity/pgstat_database.c:200)
  - [pgstat_report_disconnect](pgstat_report_disconnect.md) (from src/backend/utils/activity/pgstat_database.c:215)
  - [pgstat_update_dbstats](pgstat_update_dbstats.md) (from src/backend/utils/activity/pgstat_database.c:281)
  - [pgstat_relation_flush_cb](pgstat_relation_flush_cb.md) (from src/backend/utils/activity/pgstat_relation.c:872)

## Notes and Other Information
- The function includes an assertion that prevents reporting statistics before establishing a database connection
- InvalidOid is passed as the second OID parameter and NULL as the final parameter to pgstat_prep_pending_entry
- This function is part of PostgreSQL's lazy statistics reporting system where statistics are accumulated locally before being sent to the collector
- The returned PgStat_StatDBEntry pointer can be used to update various database-level counters and metrics
- This function is widely used throughout the database statistics reporting subsystem for various events like deadlocks, recovery conflicts, and connection events