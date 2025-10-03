# jspGetNext

## Location
[src/backend/utils/adt/jsonpath.c:1092-1158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L1092-L1158)

## Overview
Retrieves the next item in a JSON path expression sequence and initializes a JsonPathItem structure with its data.

## Definition

```c
bool
jspGetNext(JsonPathItem *v, JsonPathItem *a)
```
## Detailed Description
The jspGetNext function is a core navigation utility in PostgreSQL's JSON path implementation that allows sequential traversal through linked JSON path items. It first checks if the current JsonPathItem (v) has a next item using jspHasNext(). If a next item exists, the function validates that the current item's type is one of the many supported JSON path item types through an extensive Assert statement covering all valid types from basic values (null, string, numeric, bool) to complex operations (arithmetic, comparison, array access, filters, and built-in functions). When a next item is found and the destination pointer (a) is provided, it initializes the destination JsonPathItem structure using jspInitByBuffer() with the base buffer and the next position offset.

## Parameters / Member Variables
- `*v`: Pointer to the current JsonPathItem from which to get the next item
- `*a`: Optional pointer to JsonPathItem structure to initialize with the next item (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - jspHasNext
  - [jspInitByBuffer](jspInitByBuffer.md)
  - JsonPathItem (struct type)
  - Multiple jpi* enumeration constants (jpiNull, jpiString, jpiNumeric, etc.)
- Called from (representative examples):
  - [extract_jsp_path_expr_nodes](../e/extract_jsp_path_expr_nodes.md)
  - [printJsonPathItem](../p/printJsonPathItem.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [executeNextItem](../e/executeNextItem.md)
  - [executeBinaryArithmExpr](../e/executeBinaryArithmExpr.md)

## Notes and Other Information
- Returns true if a next item exists and was successfully retrieved, false otherwise
- The extensive Assert statement serves as both validation and documentation of all supported JSON path item types
- The function is designed to work with PostgreSQL's internal JSON path buffer format
- Part of the JSON path execution engine used for JSON querying and manipulation
- The 'a' parameter can be NULL if the caller only wants to check for the existence of a next item without retrieving it

## Simplified Source

```c
bool jspGetNext(JsonPathItem *v, JsonPathItem *a) {
    // Check if there is a next item in the sequence
    if (jspHasNext(v)) {
        // Validate that the current item type supports having a next item
        Assert(v->type == jpiNull || v->type == jpiString || v->type == jpiNumeric ||
               v->type == jpiBool || v->type == jpiAnd || v->type == jpiOr ||
               v->type == jpiNot || v->type == jpiIsUnknown || v->type == jpiEqual ||
               v->type == jpiNotEqual || v->type == jpiLess || v->type == jpiGreater ||
               v->type == jpiLessOrEqual || v->type == jpiGreaterOrEqual ||
               v->type == jpiAdd || v->type == jpiSub || v->type == jpiMul ||
               v->type == jpiDiv || v->type == jpiMod || v->type == jpiPlus ||
               v->type == jpiMinus || v->type == jpiAnyArray || v->type == jpiAnyKey ||
               v->type == jpiIndexArray || v->type == jpiAny || v->type == jpiKey ||
               v->type == jpiCurrent || v->type == jpiRoot || v->type == jpiVariable ||
               v->type == jpiFilter || v->type == jpiExists || v->type == jpiType ||
               v->type == jpiSize || v->type == jpiAbs || v->type == jpiFloor ||
               v->type == jpiCeiling || v->type == jpiDouble || v->type == jpiDatetime ||
               v->type == jpiKeyValue || v->type == jpiLast || v->type == jpiStartsWith ||
               v->type == jpiLikeRegex || v->type == jpiBigint || v->type == jpiBoolean ||
               v->type == jpiDate || v->type == jpiDecimal || v->type == jpiInteger ||
               v->type == jpiNumber || v->type == jpiStringFunc || v->type == jpiTime ||
               v->type == jpiTimeTz || v->type == jpiTimestamp || v->type == jpiTimestampTz);

        // Initialize the next item if caller provided a destination
        if (a)
            jspInitByBuffer(a, v->base, v->nextPos);

        return true;
    }

    return false;  // No next item available
}
```