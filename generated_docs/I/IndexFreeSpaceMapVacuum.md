# IndexFreeSpaceMapVacuum

## Location
[src/backend/storage/freespace/indexfsm.c:71-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/indexfsm.c#L71-L74)

## Overview
IndexFreeSpaceMapVacuum scans and fixes any inconsistencies in the Free Space Map (FSM) for index relations.

## Definition
```c
void IndexFreeSpaceMapVacuum(Relation rel)
```

## Detailed Description
IndexFreeSpaceMapVacuum is a maintenance function that performs cleanup and consistency checking on the Free Space Map for index relations. It serves as an index-specific wrapper around the general FreeSpaceMapVacuum function, ensuring that the FSM accurately reflects the actual free space distribution within index pages.

This function is typically called during vacuum operations on indexes, where it helps maintain the integrity of free space tracking by identifying and correcting any discrepancies between the FSM's recorded free space and the actual free space available in index pages. Such discrepancies can arise from various factors including incomplete operations, system crashes, or other exceptional circumstances.

The function ensures that the FSM remains an accurate and reliable guide for future page allocation decisions, which is crucial for maintaining optimal index performance and storage efficiency.

## Parameters / Member Variables
- `rel`: The Relation structure representing the index whose FSM should be vacuumed

## Dependencies
- Functions called/Symbols referenced:
  - [FreeSpaceMapVacuum](../F/FreeSpaceMapVacuum.md) (performs the actual FSM vacuum operation)
- Called from (representative examples):
  - [ginInsertCleanup](../g/ginInsertCleanup.md) (GIN index cleanup after fast updates)
  - [ginvacuumcleanup](../g/ginvacuumcleanup.md) (GIN index vacuum cleanup)
  - [gistvacuumscan](../g/gistvacuumscan.md) (GiST index vacuum scan)
  - [btvacuumscan](../b/btvacuumscan.md) (B-tree index vacuum scan)
  - [spgvacuumscan](../s/spgvacuumscan.md) (SP-GiST index vacuum scan)

## Notes and Other Information
- Essential for maintaining FSM accuracy and consistency
- Called during index vacuum operations across all access methods
- Simple wrapper around the general FreeSpaceMapVacuum function
- Helps ensure optimal index storage utilization and performance
- Critical for long-term index health and preventing FSM degradation