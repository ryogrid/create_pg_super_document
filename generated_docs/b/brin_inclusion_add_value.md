# brin_inclusion_add_value

## Location
src/backend/access/brin/brin_inclusion.c: 138 - 249

## Overview
BRIN inclusion add value function that updates a BRIN index tuple by incorporating a new heap tuple value, expanding the inclusion set if necessary.

## Definition
```c
Datum brin_inclusion_add_value(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the core add_value handler for BRIN inclusion operator classes. It examines an existing index tuple representing a page range and determines if a new heap value needs to be incorporated into the inclusion summary. The function handles several key scenarios: initializing null ranges, detecting empty values, checking containment, testing mergeability, and performing union operations. When values cannot be merged, it marks the range as containing unmergeable elements. The function implements sophisticated logic to maintain minimal yet accurate inclusion summaries.

## Parameters / Member Variables  
- `bdesc` (BrinDesc *): BRIN index descriptor containing metadata and operator class information
- `column` (BrinValues *): Current BRIN values structure for the column being updated  
- `newval` (Datum): New value from heap tuple to potentially add to the inclusion summary
- `isnull` (bool): Flag indicating whether the new value is null (asserted to be false)
- `colloid` (Oid): Collation OID for comparison operations

## Dependencies
- Functions called/Symbols referenced:
  - [inclusion_get_procinfo](../i/inclusion_get_procinfo.md)
  - [datumCopy](../d/datumCopy.md)
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - TupleDescAttr
  - [pfree](../p/pfree.md)
- Constants:
  - INCLUSION_UNION
  - INCLUSION_UNMERGEABLE  
  - INCLUSION_CONTAINS_EMPTY
  - PROCNUM_EMPTY
  - PROCNUM_CONTAINS
  - PROCNUM_MERGEABLE
  - PROCNUM_MERGE
- Data structures:
  - [BrinDesc](../B/BrinDesc.md)
  - [BrinValues](../B/BrinValues.md)
  - Form_pg_attribute
  - [FmgrInfo](../F/FmgrInfo.md)
- Called from (representative examples):
  - No direct references found (typically called via BRIN framework)

## Notes and Other Information
- Returns true if the index tuple was modified, false if no update was needed
- Handles three stored values per tuple: union, unmergeable flag, and contains-empty flag
- Implements lazy evaluation - stops processing early when possible (e.g., when unmergeable flag is set)
- Manages memory carefully by freeing old union values when they are replaced
- The function assumes the new value is not null (asserted with PG_USED_FOR_ASSERTS_ONLY)
- Contains optimization potential noted in comments regarding removal of union values when marked unmergeable