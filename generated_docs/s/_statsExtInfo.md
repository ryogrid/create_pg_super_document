# _statsExtInfo

## Location
[src/bin/pg_dump/pg_dump.h:434-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L434-L439)

## Overview
The  structure represents extended statistics objects in PostgreSQL that need to be dumped and restored by pg_dump.

## Definition

```c
typedef struct _statsExtInfo
{
	DumpableObject dobj;
	const char *rolname;		/* owner */
	TableInfo  *stattable;		/* link to table the stats are for */
	int			stattarget;		/* statistics target */
} StatsExtInfo;
```
## Detailed Description
The  structure is used by pg_dump to manage extended statistics objects (multivariate statistics) that were introduced in PostgreSQL 10. Extended statistics allow PostgreSQL to collect statistics about correlations between multiple columns, enabling better query planning. The structure stores all necessary information to recreate these statistics objects during database restore, including the owner, target table, and statistics target setting.

When pg_dump encounters extended statistics objects in the database, it creates StatsExtInfo structures to track them and generates the appropriate CREATE STATISTICS statements during the dump process.

## Parameters / Member Variables
- : Base DumpableObject structure containing common dump object metadata (object type DO_STATSEXT, catalog ID, dump ID, name, namespace)
- : Name of the role (user) that owns the extended statistics object
- : Pointer to the TableInfo structure representing the table that the statistics are defined on
- : Statistics target value for the extended statistics (-1 if not set, otherwise a positive integer controlling the level of statistics detail)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - [TableInfo](../T/TableInfo.md) (for table association)
  
- Called from (representative examples):
  - [getExtendedStatistics](../g/getExtendedStatistics.md)() (creates StatsExtInfo objects by querying pg_statistic_ext)
  - [selectDumpableStatisticsObject](selectDumpableStatisticsObject.md)() (determines if object should be dumped)
  - [dumpStatisticsExt](../d/dumpStatisticsExt.md)() (generates CREATE STATISTICS SQL during dump)

## Notes and Other Information
- Extended statistics were introduced in PostgreSQL 10, so this structure is only used when dumping from servers with version 100000 or higher
- The structure is allocated as an array using pg_malloc() in getExtendedStatistics()
- Objects of this type have objType set to DO_STATSEXT
- The statistics target can be -1 (use default) or a specific value controlling statistics detail level
- Used exclusively within the pg_dump utility for handling extended statistics objects
- The actual statistics definition is retrieved separately via SQL queries when generating the dump output