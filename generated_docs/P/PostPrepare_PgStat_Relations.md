# PostPrepare_PgStat_Relations

## Location
[src/backend/utils/activity/pgstat_relation.c:714-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L714-L732)

## Overview
Unlinks transaction statistics state from nontransactional state after a transaction has been prepared for two-phase commit.

## Definition

```c
void
PostPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state)
```
## Detailed Description
This function performs cleanup after a transaction has been successfully prepared for two-phase commit. Its primary purpose is to unlink the transaction statistics state from the base table statistics state by setting the trans pointer to NULL in each affected table status entry.

The function ensures that nontransactional action counts (like tuple insert/update/delete attempts) are immediately reported to the statistics system, while the effects on live and dead tuple counts are preserved in the 2PC state file for later application during the commit phase. This separation is crucial because some statistics need immediate reporting while others must wait for the final transaction outcome.

Note that AtEOXact_PgStat_Relations is not called during the PREPARE phase, making this function responsible for the necessary state cleanup.

## Parameters / Member Variables
- : Subtransaction status containing the prepared transaction's relation statistics

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_SubXactStatus](PgStat_SubXactStatus.md) (subtransaction status structure)
  - [PgStat_TableXactStatus](PgStat_TableXactStatus.md) (transaction-level table statistics)
  - [PgStat_TableStatus](PgStat_TableStatus.md) (base table statistics structure)
- Called from (representative examples):
  - [PostPrepare_PgStat](PostPrepare_PgStat.md) (main post-prepare statistics handler)

## Notes and Other Information
- Called only after successful transaction preparation in two-phase commit
- Does not free transaction state memory - that remains for potential commit/abort processing
- Allows immediate reporting of nontransactional action counts to the statistics collector
- Live/dead tuple count effects are preserved in 2PC state file for later application
- Simpler than AtEOXact_PgStat_Relations because it only needs to unlink, not transfer statistics
- Essential for proper statistics handling in distributed transaction scenarios
- Works in conjunction with AtPrepare_PgStat_Relations to maintain statistics consistency