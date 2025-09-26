# TwoPhasePgStatRecord

## Location
[src/backend/utils/activity/pgstat_relation.c:31-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L31-L43)

## Overview
TwoPhasePgStatRecord is a structure that stores transaction-dependent relation statistics data for Two-Phase Commit (2PC) operations, preserving statistical counters during PREPARE TRANSACTION operations.

## Definition

```c
typedef struct TwoPhasePgStatRecord
{
	PgStat_Counter tuples_inserted; /* tuples inserted in xact */
	PgStat_Counter tuples_updated;	/* tuples updated in xact */
	PgStat_Counter tuples_deleted;	/* tuples deleted in xact */
	/* tuples i/u/d prior to truncate/drop */
	PgStat_Counter inserted_pre_truncdrop;
	PgStat_Counter updated_pre_truncdrop;
	PgStat_Counter deleted_pre_truncdrop;
	Oid			id;				/* table's OID */
	bool		shared;			/* is it a shared catalog? */
	bool		truncdropped;	/* was the relation truncated/dropped? */
} TwoPhasePgStatRecord;
```
## Detailed Description
TwoPhasePgStatRecord serves as a persistent storage format for relation statistics during Two-Phase Commit transactions. When a transaction is prepared (PREPARE TRANSACTION), PostgreSQL needs to preserve the statistical counters that were accumulated during the transaction so they can be properly applied during COMMIT PREPARED or discarded during ROLLBACK PREPARED.

The structure captures all the essential statistical information about tuple operations performed on a relation during the transaction, including special handling for relations that were truncated or dropped. This ensures statistical accuracy across 2PC boundaries and maintains consistency in the PostgreSQL statistics system.

The data is serialized and stored in the 2PC state file, allowing the statistics to survive server restarts and be processed when the prepared transaction is eventually committed or rolled back.

## Parameters / Member Variables
- : Count of tuples inserted during the transaction (PgStat_Counter = int64)
- : Count of tuples updated during the transaction (PgStat_Counter = int64)
- : Count of tuples deleted during the transaction (PgStat_Counter = int64)
- : Count of tuples that were inserted before a truncate/drop operation
- : Count of tuples that were updated before a truncate/drop operation
- : Count of tuples that were deleted before a truncate/drop operation
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Object identifier (OID) of the relation these statistics apply to
- : Boolean flag indicating whether this is a shared catalog relation
- : Boolean flag indicating whether the relation was truncated or dropped during the transaction

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (int64 typedef)
  - Oid (built-in type)
- Called from (representative examples):
  - [AtPrepare_PgStat_Relations](../A/AtPrepare_PgStat_Relations.md) (creates and populates records)
  - [pgstat_twophase_postcommit](../p/pgstat_twophase_postcommit.md) (processes records on commit)
  - [pgstat_twophase_postabort](../p/pgstat_twophase_postabort.md) (processes records on rollback)

## Notes and Other Information
- This structure is part of PostgreSQL's Two-Phase Commit implementation and is critical for maintaining statistical accuracy across prepared transactions
- The record is registered with the 2PC system using RegisterTwoPhaseRecord() with resource manager ID TWOPHASE_RM_PGSTAT_ID
- Special handling is provided for truncated/dropped relations through the truncdropped flag and separate pre-truncate counters
- The structure size is fixed and used directly in sizeof() operations for serialization
- Located in src/backend/utils/activity/pgstat_relation.c:31-43