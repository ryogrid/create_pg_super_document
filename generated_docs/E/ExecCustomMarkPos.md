# ExecCustomMarkPos

## Location
[src/backend/executor/nodeCustom.c:139-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCustom.c#L139-L149)

## Overview
Marks the current scan position for a Custom Scan node to enable later restoration, with error handling for unsupported operations.

## Definition
```c
void ExecCustomMarkPos(CustomScanState *node)
```

## Detailed Description
ExecCustomMarkPos is used to mark the current position in a custom scan so that it can be restored later using ExecCustomRestrPos. This functionality is essential for certain query execution patterns like merge joins that need to backtrack to previously scanned positions. The function first checks if the custom scan provider supports position marking by verifying the MarkPosCustomScan callback is implemented. If not supported, it raises a FEATURE_NOT_SUPPORTED error with a descriptive message. If supported, it delegates to the provider's MarkPosCustomScan method.

## Parameters / Member Variables
- `node`: The CustomScanState node for which to mark the current scan position

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - MarkPosCustomScan (via node->methods callback, if supported)
- Called from (representative examples):
  - [ExecMarkPos](ExecMarkPos.md)

## Notes and Other Information
- Not all custom scan providers are required to support position marking
- Raises ERRCODE_FEATURE_NOT_SUPPORTED error if the custom scan doesn't implement MarkPosCustomScan
- Used in conjunction with ExecCustomRestrPos for scan position management
- Essential for merge joins and other algorithms that require backtracking capability
- The error message includes the custom scan's name for better debugging

## Simplified Source

```c
void
ExecCustomMarkPos(CustomScanState *node)
{
    // Check if position marking is supported
    if (!node->methods->MarkPosCustomScan)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("custom scan \"%s\" does not support MarkPos",
                        node->methods->CustomName)));

    // Delegate to custom scan's mark position implementation
    node->methods->MarkPosCustomScan(node);
}
```