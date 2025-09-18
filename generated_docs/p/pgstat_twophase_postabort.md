# pgstat_twophase_postabort

## Location
src/backend/utils/activity/pgstat_relation.c: 769 - 801

## Overview
Handles the post-abort processing for two-phase commit transactions by restoring saved statistics counts into the local pgstats state, treating them as aborted operations.

## Definition
```c
void pgstat_twophase_postabort(TransactionId xid, uint16 info, void *recdata, uint32 len)
```

## Detailed Description
This function is the counterpart to pgstat_twophase_postcommit, handling the ROLLBACK PREPARED case in PostgreSQL's two-phase commit protocol. When a prepared transaction is aborted, this function restores the previously saved relation statistics from the transaction record, but applies the abort-case logic similar to AtEOXact_PgStat's abort handling.

The key difference from the commit case is in handling truncated relations: if a relation was truncated during the prepared transaction, the function restores the pre-truncation tuple counts before applying the statistics. Additionally, unlike the commit case, aborted operations don't affect live tuple counts - only inserted and updated tuples are added to dead tuple counts.

## Parameters / Member Variables
- `xid`: Transaction ID of the prepared transaction being aborted
- `info`: Additional information flags (currently unused)
- `recdata`: Pointer to the TwoPhasePgStatRecord containing saved statistics data
- `len`: Length of the record data

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_prep_relation_pending](pgstat_prep_relation_pending.md)
  - TwoPhasePgStatRecord (data structure)
  - PgStat_TableStatus (data structure)
- Called from (representative examples):
  - Two-phase commit recovery system (referenced in pgstat.h)

## Notes and Other Information
- This function is paired with pgstat_twophase_postcommit for handling the commit case
- The statistics calculations mirror those in AtEOXact_PgStat's abort logic to maintain consistency
- Special handling for truncated relations: restores pre-truncation counts before applying statistics
- Unlike the commit case, aborted transactions only contribute to dead tuple counts, not live tuple deltas
- Part of PostgreSQL's comprehensive statistics collection system ensuring proper cleanup during transaction aborts