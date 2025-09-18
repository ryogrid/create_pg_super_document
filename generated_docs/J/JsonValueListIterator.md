# JsonValueListIterator

## Location
[src/backend/utils/adt/jsonpath_exec.c:155-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L155-L160)

## Overview
An iterator structure for traversing JsonValueList collections, providing efficient sequential access to JSONB values during path execution.

## Definition
```c
typedef struct JsonValueListIterator
{
    JsonbValue *value;
    List       *list;
    ListCell   *next;
} JsonValueListIterator;
```

## Detailed Description
JsonValueListIterator provides a standardized way to iterate through JsonValueList collections. It handles both the singleton optimization case (single value) and the full list case transparently. The iterator maintains the current position and provides methods to advance through the collection, supporting the common pattern of processing multiple JSON values sequentially during path expression evaluation.

## Parameters / Member Variables
- `value`: Pointer to the current JsonbValue being processed (used for singleton optimization)
- `list`: Pointer to the underlying List structure when iterating over multi-item collections
- `next`: Pointer to the next ListCell in the iteration sequence

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbValue](JsonbValue.md)
  - [List](../L/List.md) (PostgreSQL list type)
  - ListCell (PostgreSQL list cell type)
- Called from (representative examples):
  - [executeItemOptUnwrapResult](../e/executeItemOptUnwrapResult.md)
  - [executePredicate](../e/executePredicate.md)
  - [executeUnaryArithmExpr](../e/executeUnaryArithmExpr.md)
  - [JsonValueListInitIterator](JsonValueListInitIterator.md)
  - [JsonValueListNext](JsonValueListNext.md)

## Notes and Other Information
- Works seamlessly with both singleton and list storage modes of JsonValueList
- Provides consistent iteration interface regardless of the underlying storage method
- Used extensively in JSON path expression evaluation for processing result sets
- The iterator state is maintained across calls to support incremental processing
- Essential for handling multi-valued JSON path results efficiently