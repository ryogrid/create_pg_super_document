# ExecTypeFromTL

## Location
[src/backend/executor/execTuples.c:2025-2036](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L2025-L2036)

## Overview
Generates a tuple descriptor for the result tuple of a target list, including resjunk columns in the result.

## Definition
```c
TupleDesc ExecTypeFromTL(List *targetList)
```

## Detailed Description
This function serves as a convenience wrapper around ExecTypeFromTLInternal, specifically designed to create a TupleDesc from a parse/plan target list. It includes all columns in the target list, including resjunk columns, which makes it suitable for creating descriptors that need to accommodate all target list entries.

The function is part of PostgreSQL's tuple descriptor creation infrastructure and represents one of several places in the codebase where TupleDescriptors are created. The comment suggests these various creation points should potentially be consolidated.

The function delegates the actual work to ExecTypeFromTLInternal with the 'hasoid' parameter set to false, indicating that the resulting tuple descriptor should not include an OID column.

## Parameters / Member Variables
- `targetList`: A List of TargetEntry nodes representing the target list from a parse or plan tree (must not be an ExprState target list)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecTypeFromTLInternal](ExecTypeFromTLInternal.md) (the actual implementation function)
- Called from (representative examples):
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [find_hash_columns](../f/find_hash_columns.md)
  - [build_pertrans_for_aggref](../b/build_pertrans_for_aggref.md)
  - [ExecInitCustomScan](ExecInitCustomScan.md)
  - [ExecInitForeignScan](ExecInitForeignScan.md)
  - [ExecInitIndexOnlyScan](ExecInitIndexOnlyScan.md)
  - [ExecInitSubPlan](ExecInitSubPlan.md)
  - [ordered_set_startup](../o/ordered_set_startup.md)

## Notes and Other Information
- This function specifically handles parse/plan target lists, not ExprState target lists
- Resjunk columns are included in the resulting descriptor, which may be important for certain executor operations
- The function is part of a broader set of TupleDescriptor creation functions that the codebase comments suggest could benefit from consolidation
- Uses ExecTypeFromTLInternal with hasoid=false, meaning OID columns are not supported in the result
- The resulting TupleDesc must be properly managed for memory cleanup by the caller

## Simplified Source

```c
TupleDesc
ExecTypeFromTL(List *targetList)
{
    // Delegate to internal implementation with hasoid=false
    return ExecTypeFromTLInternal(targetList, false);
}
```