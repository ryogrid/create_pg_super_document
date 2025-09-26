# PgStat_TableCounts

## Location
[src/include/pgstat.h:160-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L160-L180)

## Overview
PgStat_TableCounts is a structure that contains actual per-table event counters maintained by a backend for tracking table access and modification statistics.

## Definition
```c
typedef struct PgStat_TableCounts
{
    PgStat_Counter numscans;

    PgStat_Counter tuples_returned;
    PgStat_Counter tuples_fetched;

    PgStat_Counter tuples_inserted;
    PgStat_Counter tuples_updated;
    PgStat_Counter tuples_deleted;
    PgStat_Counter tuples_hot_updated;
    PgStat_Counter tuples_newpage_updated;
    bool           truncdropped;

    PgStat_Counter delta_live_tuples;
    PgStat_Counter delta_dead_tuples;
    PgStat_Counter changed_tuples;

    PgStat_Counter blocks_fetched;
    PgStat_Counter blocks_hit;
} PgStat_TableCounts;
```

## Detailed Description
This structure serves as the core component for tracking table-level statistics within backend local memory. It contains only actual event counters to enable efficient zero-detection through memcmp operations for determining pending statistics updates. The structure is a component of PgStat_TableStatus and tracks various aspects of table access including scans, tuple operations, and buffer access patterns.

The tuple count fields distinguish between different types of access: tuples_returned counts tuples fetched by heap_getnext, while tuples_fetched counts tuples fetched by heap_fetch under bitmap indexscans. For indexes, the semantics differ slightly - tuples_returned counts index entries returned by the index AM, while tuples_fetched counts tuples fetched under simple indexscans.

The modification counters (tuples_inserted/updated/deleted/hot_updated/newpage_updated) count attempted actions regardless of transaction outcome, while the delta fields (delta_live_tuples, delta_dead_tuples) reflect actual changes based on commit or abort status.

## Parameters / Member Variables
- : Number of sequential scans performed on the table
- : Number of tuples successfully fetched by heap_getnext (or index entries for indexes)
- : Number of tuples fetched by heap_fetch under bitmap/simple indexscans
- : Count of tuple insertion attempts
- : Count of tuple update attempts
- : Count of tuple deletion attempts
- : Count of HOT (Heap-Only Tuple) update attempts
- : Count of updates that created new pages
- : Boolean flag indicating if table was truncated or dropped
- : Change in live tuple count (can be negative)
- : Change in dead tuple count (can be negative)
- : Total number of tuples changed
- : Number of disk blocks fetched for this table
- : Number of buffer cache hits for this table

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (used for all counter fields)
- Called from (representative examples):
  - pgstat_relation_flush_cb (statistics flushing callback)
  - PgStat_TableStatus (embedded within table status structure)

## Notes and Other Information
- Optimized for performance with memcmp-based zero detection for pending updates
- Part of PostgreSQL's table statistics collection system
- Delta tuple counts can be negative to reflect decreases in live/dead tuple counts
- Modification counters track attempts regardless of transaction commit status
- Buffer access tracking provides insights into I/O patterns and cache effectiveness
- Located at src/include/pgstat.h:160-180