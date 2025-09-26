# subxids_array_status

## Location
[src/include/storage/standby.h:84-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/standby.h#L84-L85)

## Overview
subxids_array_status is an enumeration that indicates the completeness and storage location of subtransaction information in running transaction snapshots.

## Definition
```c
typedef enum
{
    SUBXIDS_IN_ARRAY,           /* xids array includes all running subxids */
    SUBXIDS_MISSING,            /* snapshot overflowed, subxids are missing */
    SUBXIDS_IN_SUBTRANS,        /* subxids are not included in 'xids', but
                                 * pg_subtrans is fully up-to-date */
} subxids_array_status;
```

## Detailed Description
This enumeration provides critical information about the state and availability of subtransaction data within PostgreSQL's transaction management system. It helps the system understand whether all subtransaction information is immediately available in memory arrays or if it needs to be retrieved from other sources like the pg_subtrans system.

The enum is essential for hot standby operations and WAL replay, where accurate knowledge of subtransaction states is required for maintaining proper MVCC semantics and query visibility.

## Parameters / Member Variables
- `SUBXIDS_IN_ARRAY`: All running subtransaction IDs are included in the xids array of the RunningTransactionsData structure. This represents the ideal case where complete subtransaction information is immediately available.
- `SUBXIDS_MISSING`: The snapshot has overflowed its capacity, and some subtransaction information is missing entirely. This indicates that not all subtransaction data could be captured.
- `SUBXIDS_IN_SUBTRANS`: Subtransaction IDs are not stored in the xids array, but the pg_subtrans system catalog is fully up-to-date and can be consulted to retrieve the missing subtransaction information.

## Dependencies
- Functions called/Symbols referenced: (None - this is an enum definition)
- Called from (representative examples):
  - [RunningTransactionsData](../R/RunningTransactionsData.md) (as a member field)

## Notes and Other Information
- This enum is crucial for determining how to handle subtransaction visibility during WAL replay and hot standby operations
- The SUBXIDS_MISSING state can occur when there are too many active subtransactions to fit in the available snapshot space
- When in SUBXIDS_IN_SUBTRANS state, the system must perform additional lookups in pg_subtrans to get complete subtransaction information
- The choice between these states affects performance and completeness of transaction visibility information