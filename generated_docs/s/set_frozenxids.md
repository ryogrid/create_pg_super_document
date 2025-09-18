# set_frozenxids

## Location
[src/bin/pg_upgrade/pg_upgrade.c:827-928](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L827-L928)

## Overview
Sets frozen XIDs and minimum multixact IDs in the new cluster during pg_upgrade to ensure proper transaction ID management after database upgrade.

## Definition


## Detailed Description
The set_frozenxids function is a critical component of PostgreSQL's pg_upgrade utility that manages transaction ID consistency between old and new database clusters. This function operates in two distinct modes:

1. **Full mode (minmxid_only = false)**: Called on the new cluster before restoring any user data. It ensures that all initdb-created vacuumable tables have relfrozenxid/relminmxid values matching the old cluster's transaction ID and multixact ID counters. It also initializes datfrozenxid/datminmxid for built-in databases.

2. **MinMXID-only mode (minmxid_only = true)**: Used specifically when upgrading from pre-9.3 databases that don't store per-table or per-database minimum multixact IDs. This second pass initializes all tables and databases with the correct minmxid values while leaving frozenxid values unchanged.

The function connects to each database in the cluster and updates the system catalogs (pg_database and pg_class) with appropriate frozen transaction IDs. It handles databases with datallowconn = false by temporarily enabling connections, performing the updates, and then restoring the original connection setting.

## Parameters / Member Variables
- : Boolean flag that determines the operation mode. When false, updates both frozenxid and minmxid values. When true, only updates minmxid values (used for pre-9.3 database upgrades).

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) (status reporting)
  - [connectToServer](../c/connectToServer.md) (database connection)
  - [executeQueryOrDie](../e/executeQueryOrDie.md) (SQL execution)
  - [quote_identifier](../q/quote_identifier.md) (SQL identifier quoting)
  - [check_ok](../c/check_ok.md) (completion verification)
  - [PQfinish](../P/PQfinish.md) (connection cleanup)
  - CppAsString2 (macro for string conversion)
  - RELKIND_RELATION, RELKIND_MATVIEW, RELKIND_TOASTVALUE (relation type constants)
- Called from:
  - [prepare_new_globals](../p/prepare_new_globals.md) (in initial upgrade phase)
  - [create_new_objects](../c/create_new_objects.md) (in post-restore phase for pre-9.3 upgrades)

## Notes and Other Information
- This function is essential for maintaining MVCC (Multi-Version Concurrency Control) integrity during database upgrades
- The two-pass approach for pre-9.3 databases ensures backward compatibility while properly initializing multixact ID tracking
- Only operates on vacuumable relation types (heap tables, materialized views, and TOAST tables)
- Temporarily modifies datallowconn for databases that normally don't allow connections (like template0) to perform necessary updates
- Uses the old cluster's checkpoint next XID and multixact ID values as the baseline for the new cluster
- Critical for preventing transaction ID wraparound issues in the upgraded cluster