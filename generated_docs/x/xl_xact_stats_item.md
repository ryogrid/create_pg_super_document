# xl_xact_stats_item

## Location
[src/include/access/xact.h:282-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L282-L287)

## Overview
A structure representing a transactionally dropped statistics entry in PostgreSQL's Write-Ahead Logging (WAL), designed to track statistics objects that are dropped during transaction processing.

## Definition

```c
typedef struct xl_xact_stats_item
{
	int			kind;
	Oid			dboid;
	Oid			objoid;
} xl_xact_stats_item;
```
## Detailed Description
The xl_xact_stats_item structure represents a transactionally dropped statistics entry used in WAL logging. This structure is specifically designed to be readable by frontend programs and is declared in xact.h rather than pgstat.h to avoid frontend code inclusion issues. It tracks individual statistics objects that are dropped as part of transaction commit, abort, or prepare operations. The structure is used extensively in PostgreSQL's statistics management system to ensure proper cleanup of statistics entries during various transaction states.

## Parameters / Member Variables
- : An integer identifying the type or kind of statistics entry being dropped
- : The Object Identifier (Oid) of the database containing the statistics object
- : The Object Identifier (Oid) of the specific statistics object being dropped

## Dependencies
- Functions called/Symbols referenced:
  - Oid (data type)

- Called from (representative examples):
  - ParseCommitRecord (in xactdesc.c:95)
  - ParseAbortRecord (in xactdesc.c:201)
  - StartPrepare (in twophase.c:1057, 1058, 1121, 1127)
  - FinishPreparedTransaction (in twophase.c:1502, 1503, 1538-1541)
  - RecordTransactionCommit (in xact.c:1314)
  - RecordTransactionAbort (in xact.c:1730)
  - XactLogCommitRecord (in xact.c:5755, 5891)
  - XactLogAbortRecord (in xact.c:5927, 6044)
  - pgstat_get_transactional_drops (in pgstat_xact.c:270, 287)
  - pgstat_execute_transactional_drops (in pgstat_xact.c:312, 321)

## Notes and Other Information
- Declared in xact.h rather than pgstat.h to ensure WAL format readability by frontend programs
- Used extensively in transaction preparation, commit, and abort operations
- Critical for maintaining statistics consistency during transaction processing
- Part of PostgreSQL's transactional statistics management system
- The structure is defined in src/include/access/xact.h at lines 282-287