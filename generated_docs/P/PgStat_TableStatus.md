# PgStat_TableStatus

## Location
src/include/pgstat.h: 197 - 204

## Overview
PgStat_TableStatus represents per-table status information within a backend, managing both transactional and non-transactional event counters for table statistics.

## Definition
```c
typedef struct PgStat_TableStatus
{
    Oid                        id;           /* table's OID */
    bool                       shared;       /* is it a shared catalog? */
    struct PgStat_TableXactStatus *trans;   /* lowest subxact's counts */
    PgStat_TableCounts         counts;       /* event counts to be sent */
    Relation                   relation;     /* rel that is using this entry */
} PgStat_TableStatus;
```

## Detailed Description
This structure serves as the comprehensive per-table status tracking mechanism within a backend process. It handles the complex interaction between transactional and non-transactional statistics by maintaining both immediate event counts and transaction-aware delta tracking.

The structure addresses the dual nature of PostgreSQL statistics: many event counters (like scans and fetches) are nontransactional and count events regardless of transaction outcome, while others (like live/dead tuple counts) must be adjusted based on transaction commit or abort status. To handle this, it maintains a stack of per-subtransaction status records that track changes at each transaction nesting level.

At transaction commit or abort, the system propagates tuple modification counts (tuples_inserted/updated/deleted) either up to the parent subtransaction level or out to the main PgStat_TableStatus, ensuring accurate statistics regardless of transaction nesting and outcome.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): The OID (Object Identifier) of the table being tracked
- : Boolean flag indicating whether this is a shared system catalog table
- : Pointer to the stack of per-subtransaction status records for transactional event tracking
- : The PgStat_TableCounts structure containing accumulated event counters ready for transmission
- : Pointer to the Relation structure currently using this statistics entry

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_TableXactStatus (for transaction-level tracking)
  - PgStat_TableCounts (embedded counter structure)
  - Oid (PostgreSQL object identifier type)
  - Relation (table relation structure)
- Called from (representative examples):
  - pgstat_count_heap_insert/update/delete (table modification tracking)
  - find_tabstat_entry (statistics entry lookup)
  - AtEOXact_PgStat_Relations (end-of-transaction processing)
  - pgstat_relation_flush_cb (statistics flushing)

## Notes and Other Information
- Manages complex transactional vs. non-transactional statistics semantics
- Maintains transaction nesting awareness through subtransaction stack
- Critical component of PostgreSQL's table-level statistics collection system
- Handles both regular user tables and shared system catalog tracking
- Located at src/include/pgstat.h:197-204
- Works closely with the relation cache system through the relation pointer