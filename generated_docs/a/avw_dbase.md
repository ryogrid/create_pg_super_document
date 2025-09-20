# avw_dbase

## Location
[src/backend/postmaster/autovacuum.c:176-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L176-L183)

## Overview
The  structure is used by PostgreSQL autovacuum workers to maintain database-specific information and statistics needed for autovacuum operations within a specific database.

## Definition

```c
typedef struct avw_dbase
{
	Oid			adw_datid;
	char	   *adw_name;
	TransactionId adw_frozenxid;
	MultiXactId adw_minmulti;
	PgStat_StatDBEntry *adw_entry;
} avw_dbase;
```
## Detailed Description
The  structure represents database-specific information that autovacuum workers need to perform their operations effectively. Unlike  which is used by the launcher for scheduling, this structure contains detailed database state information including transaction IDs and statistics that are essential for making autovacuum decisions within a particular database context.

## Parameters / Member Variables
- `adw_datid`: Database OID identifying the specific database
- `*adw_name`: Human-readable name of the database (string pointer)
- `adw_frozenxid`: Transaction ID freeze horizon for this database, used to determine when anti-wraparound vacuuming is needed
- `adw_minmulti`: Minimum MultiXact ID for this database, used for MultiXact wraparound prevention
- `*adw_entry`: Pointer to the database's statistics entry containing performance and usage metrics
## Dependencies
- Functions called/Symbols referenced:
  - MultiXactId (transaction system type)
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md) (statistics system structure)
- Called from (representative examples):
  - [rebuild_database_list](../r/rebuild_database_list.md)
  - [do_start_worker](../d/do_start_worker.md)
  - [get_database_list](../g/get_database_list.md)

## Notes and Other Information
- This structure is primarily used by autovacuum workers (as opposed to the launcher)
- The frozen XID and minimum MultiXact ID fields are critical for preventing transaction wraparound
- Contains a pointer to statistics data rather than embedding it, allowing for efficient memory usage
- The database name is stored as a pointer to avoid unnecessary memory overhead