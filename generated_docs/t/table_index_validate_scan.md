# table_index_validate_scan

## Location
[src/include/access/tableam.h:1840-1868](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1840-L1868)

## Overview
Performs the second table scan during concurrent index build operations to validate index completeness and consistency.

## Definition
```c
static inline void
table_index_validate_scan(Relation table_rel,
                          Relation index_rel,
                          struct IndexInfo *index_info,
                          Snapshot snapshot,
                          struct ValidateIndexState *state)
```

## Detailed Description
This function is specifically designed for concurrent index build operations where the index needs to be validated after the initial build phase. During concurrent index creation, new tuples may be inserted into the table while the index is being built. This validation scan ensures that all tuples that should be in the index are actually present.

The function uses a specific snapshot to determine which tuples should be visible during the validation process. It works in conjunction with the `validate_index()` function to complete the concurrent index build process by checking for any missing entries that may have been inserted during the initial build phase.

Unlike regular index build scans, this function doesn't return a tuple count as it's focused on validation rather than statistics collection.

## Parameters / Member Variables
- `table_rel`: Relation - The parent table relation being validated
- `index_rel`: Relation - The index relation being validated
- `index_info`: struct IndexInfo* - Information about the index being validated
- `snapshot`: Snapshot - The snapshot to use for determining tuple visibility during validation
- `state`: struct ValidateIndexState* - State information for the validation process

## Dependencies
- Functions called/Symbols referenced:
  - table_rel->rd_tableam->index_validate_scan (delegates to table AM implementation)
- Types referenced:
  - [Relation](../R/Relation.md)
  - [IndexInfo](../I/IndexInfo.md)
  - [Snapshot](../S/Snapshot.md)
  - ValidateIndexState
- Called from (representative examples):
  - [validate_index](../v/validate_index.md) (src/backend/catalog/index.c:3391)

## Notes and Other Information
- Part of the concurrent index build process - see `validate_index()` for complete context
- Does not return a value (void function) unlike other index scan functions
- Uses a specific snapshot for visibility determination during validation
- Part of the table access method abstraction layer
- Critical for ensuring data consistency in concurrent index builds
- Only called during the validation phase of concurrent index creation
- The ValidateIndexState parameter maintains state across the validation process