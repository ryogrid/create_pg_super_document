# JsonValueListNext

## Location
[src/backend/utils/adt/jsonpath_exec.c:3580-3600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3580-L3600)

## Overview
JsonValueListNext is a static function that advances an iterator through a JsonValueList sequence and returns the next JsonbValue item.

## Definition
static JsonbValue *JsonValueListNext(const JsonValueList *jvl, JsonValueListIterator *it)

## Detailed Description
This function implements iterator functionality for JsonValueList sequences in PostgreSQL's JSON path execution engine. It advances the given iterator to the next position and returns the current JsonbValue. The function follows a standard iterator pattern where it returns the current value before advancing to the next position. When the iterator reaches the end of the sequence, subsequent calls will return NULL and the iterator's value field will be set to NULL.

## Parameters / Member Variables
- : Pointer to the JsonValueList being iterated (const, indicating read-only access)
- : Pointer to the JsonValueListIterator that maintains the current position and state

## Dependencies
- Functions called/Symbols referenced:
  - [JsonValueList](JsonValueList.md) (type/structure)
  - [JsonValueListIterator](JsonValueListIterator.md) (type/structure)
  - lfirst (PostgreSQL list macro for getting current list element)
  - [lnext](../l/lnext.md) (PostgreSQL list macro for advancing to next list element)
- Called from (representative examples):
  - [executeItemOptUnwrapResult](../e/executeItemOptUnwrapResult.md)
  - [executePredicate](../e/executePredicate.md)
  - [executeUnaryArithmExpr](../e/executeUnaryArithmExpr.md)
  - [wrapItemsInArray](../w/wrapItemsInArray.md)
  - [JsonTablePlanScanNextRow](JsonTablePlanScanNextRow.md)

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c, not exposed in the public API
- The function safely handles iterator advancement by checking if it->next exists before dereferencing
- Returns the current value before advancing, following standard iterator semantics
- Part of PostgreSQL's JSON path expression evaluation system
- Used extensively in JSON path execution for iterating through result sets and intermediate values

## Simplified Source

```c
static JsonbValue *JsonValueListNext(const JsonValueList *jvl, JsonValueListIterator *it) {
    // Get current value before advancing iterator
    JsonbValue *result = it->value;

    // Advance to next element if available
    if (it->next) {
        it->value = lfirst(it->next);
        it->next = lnext(it->list, it->next);
    } else {
        it->value = NULL;  // End of iteration
    }

    return result;
}
```