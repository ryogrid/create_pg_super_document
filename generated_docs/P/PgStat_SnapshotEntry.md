# PgStat_SnapshotEntry

## Location
[src/backend/utils/activity/pgstat.c:132-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L132-L137)

## Overview
PgStat_SnapshotEntry is a hash table entry structure used for storing statistics snapshots in PostgreSQL's statistics subsystem.

## Definition

```c
typedef struct PgStat_SnapshotEntry
{
	PgStat_HashKey key;
	char		status;			/* for simplehash use */
	void	   *data;			/* the stats data itself */
} PgStat_SnapshotEntry;
```
## Detailed Description
PgStat_SnapshotEntry serves as an entry in the hash table for statistics snapshots within PostgreSQL's statistics collection and reporting system. This structure is designed to work with PostgreSQL's simplehash implementation and provides a mechanism for organizing and accessing statistical data snapshots. Each entry contains a key for identification, a status field required by the simplehash implementation, and a generic pointer to the actual statistics data.

The structure is defined in src/backend/utils/activity/pgstat.c at lines 132-137 and is used as part of PostgreSQL's internal statistics infrastructure to maintain snapshots of statistical information that can be efficiently retrieved and managed.

## Parameters / Member Variables
- : PgStat_HashKey structure containing the statistics entry kind, database ID, and object ID used to uniquely identify the statistics entry
- : Character field required by PostgreSQL's simplehash implementation for internal hash table management
- : Generic void pointer that points to the actual statistics data associated with this entry

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_HashKey
- Called from (representative examples):
  - [pgstat_fetch_entry](../p/pgstat_fetch_entry.md) (multiple references)
  - [pgstat_build_snapshot](../p/pgstat_build_snapshot.md)
  - SH_ELEMENT_TYPE (simplehash macro)

## Notes and Other Information
- This structure is specifically designed to work with PostgreSQL's simplehash implementation, which explains the presence of the  field
- The  field is a generic pointer allowing for different types of statistics data to be stored depending on the statistics kind
- The structure is part of the internal statistics snapshot mechanism and is not exposed to external users
- Located in src/backend/utils/activity/pgstat.c, indicating it's part of the backend's activity monitoring and statistics collection subsystem