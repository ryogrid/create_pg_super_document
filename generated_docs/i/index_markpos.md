# index_markpos

## Location
[src/backend/access/index/indexam.c:408-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L408-L431)

## Overview
The index_markpos function marks the current position within an index scan, allowing the scan to be restored to this position later using index_restrpos.

## Definition
```c
void index_markpos(IndexScanDesc scan)
```

## Detailed Description
index_markpos provides the capability to mark a specific position during an index scan operation. This function serves as a generic interface that delegates to the access method-specific position marking routine (ammarkpos). The marked position can later be restored using the corresponding index_restrpos function, enabling backtracking within the scan.

The function performs validation checks to ensure the scan descriptor is valid and that the underlying access method supports position marking through the ammarkpos procedure.

## Parameters / Member Variables
- `scan`: IndexScanDesc - The active index scan descriptor whose current position should be marked

## Dependencies
- Functions called/Symbols referenced:
  - SCAN_CHECKS (validation macro for scan descriptor)
  - CHECK_SCAN_PROCEDURE (validation macro for ammarkpos availability)
  - ammarkpos (access method-specific position marking routine)
- Called from (representative examples):
  - [ExecIndexMarkPos](../E/ExecIndexMarkPos.md)
  - [ExecIndexOnlyMarkPos](../E/ExecIndexOnlyMarkPos.md)

## Notes and Other Information
- This function is part of the position management functionality for index scans
- Not all access methods support position marking - the CHECK_SCAN_PROCEDURE macro ensures the feature is available
- Must be paired with index_restrpos to restore the marked position
- The actual position marking logic is implemented by the specific index access method
- Located in src/backend/access/index/indexam.c:408-431
- Position marking is commonly used in executor nodes that need to revisit previous scan positions

## Simplified Source

```c
void index_markpos(IndexScanDesc scan) {
    // Validate scan descriptor and check that access method supports position marking
    SCAN_CHECKS;
    CHECK_SCAN_PROCEDURE(ammarkpos);

    // Delegate to access method-specific position marking routine
    scan->indexRelation->rd_indam->ammarkpos(scan);
}
```