# spgxlogVacuumRedirect

## Location
[src/include/access/spgxlog.h:238-248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgxlog.h#L238-L248)

## Overview
The spgxlogVacuumRedirect struct is a PostgreSQL WAL record structure used to log vacuum operations that clean up redirect tuples and placeholder tuples in SP-GiST indexes, which are created during tuple movements and updates.

## Definition

```c
typedef struct spgxlogVacuumRedirect
{
	uint16		nToPlaceholder; /* number of redirects to make placeholders */
	OffsetNumber firstPlaceholder;	/* first placeholder tuple to remove */
	TransactionId snapshotConflictHorizon;	/* newest XID of removed redirects */
	bool		isCatalogRel;	/* to handle recovery conflict during logical
								 * decoding on standby */

	/* offsets of redirect tuples to make placeholders follow */
	OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} spgxlogVacuumRedirect;
```
## Detailed Description
This structure represents a WAL record for cleaning up redirect and placeholder tuples in SP-GiST indexes. Redirect tuples are created when a tuple is moved from one location to another (typically during page splits or reorganization), pointing from the old location to the new location. Over time, these redirect tuples can be safely converted to placeholder tuples, and eventually, placeholder tuples can be removed entirely. This vacuum operation handles the transition of redirect tuples to placeholders and the removal of old placeholder tuples, which is essential for maintaining index efficiency and preventing excessive space usage.

## Parameters / Member Variables
- : Number of redirect tuples that should be converted to placeholder tuples during this operation
- : Offset number of the first placeholder tuple that should be removed entirely
- : Transaction ID representing the newest transaction ID of removed redirect tuples, used for handling recovery conflicts
- : Boolean flag indicating whether this is a catalog relation, important for handling recovery conflicts during logical decoding on standby servers
- : Flexible array member containing the offset numbers of redirect tuples that should be converted to placeholders

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [vacuumRedirectAndPlaceholder](../v/vacuumRedirectAndPlaceholder.md) (src/backend/access/spgist/spgvacuum.c:504)
  - [spgRedoVacuumRedirect](spgRedoVacuumRedirect.md) (src/backend/access/spgist/spgxlog.c:864)
  - [spg_desc](spg_desc.md) (src/backend/access/rmgrdesc/spgdesc.c:119)
  - SizeOfSpgxlogVacuumRedirect (src/include/access/spgxlog.h:250)

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [vacuumRedirectAndPlaceholder](../v/vacuumRedirectAndPlaceholder.md) (src/backend/access/spgist/spgvacuum.c:504)
  - [spgRedoVacuumRedirect](spgRedoVacuumRedirect.md) (src/backend/access/spgist/spgxlog.c:864)
  - [spg_desc](spg_desc.md) (src/backend/access/rmgrdesc/spgdesc.c:119)
  - SizeOfSpgxlogVacuumRedirect (src/include/access/spgxlog.h:250)

## Notes and Other Information
- This operation is part of the SP-GiST garbage collection system that prevents the accumulation of redirect and placeholder tuples
- The snapshotConflictHorizon is crucial for MVCC (Multi-Version Concurrency Control) correctness, ensuring that concurrent transactions see consistent data
- The isCatalogRel flag is important for logical replication and standby server consistency, as catalog relations have special handling requirements
- Redirect tuples typically exist temporarily to maintain referential integrity during tuple movements, but they consume space and should be cleaned up when no longer needed
- The conversion from redirect to placeholder is a gradual process that respects transaction visibility rules to maintain data consistency