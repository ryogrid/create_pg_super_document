# compareItems

## Location
[src/backend/utils/adt/jsonpath_exec.c:3341-3436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3341-L3436)

## Overview
The main comparison function for SQL/JSON items that handles type checking, value comparison, and operation evaluation for all supported JSON types.

## Definition
static JsonPathBool compareItems(int32 op, JsonbValue *jb1, JsonbValue *jb2, bool useTz)

## Detailed Description
The compareItems function is the central comparison engine for SQL/JSON path expressions. It first performs type compatibility checking, handling null comparisons and type mismatches according to SQL/JSON standards. For compatible types, it delegates to appropriate specialized comparison functions (numeric, string, datetime) or performs direct comparison for simple types like booleans. Finally, it evaluates the comparison result against the requested operation (equal, not equal, less than, etc.) and returns the appropriate JsonPathBool result. The function properly handles the three-valued logic of SQL with jpbUnknown for incomparable items.

## Parameters / Member Variables
- `op`: The comparison operation type (jpiEqual, jpiNotEqual, jpiLess, jpiGreater, jpiLessOrEqual, jpiGreaterOrEqual)
- `jb1`: First JsonbValue to compare
- `jb2`: Second JsonbValue to compare
- `useTz`: Boolean flag indicating whether to use timezone information for datetime comparisons

## Dependencies
- Functions called/Symbols referenced:
  - [compareNumeric](compareNumeric.md)
  - [compareStrings](compareStrings.md)
  - [compareDatetime](compareDatetime.md)
  - memcmp
  - elog
  - Various JsonbValue type constants (jbvNull, jbvBool, jbvNumeric, jbvString, jbvDatetime, jbvBinary, jbvArray, jbvObject)
  - [JsonPath](../J/JsonPath.md) operation constants (jpiEqual, jpiNotEqual, jpiLess, etc.)
  - JsonPathBool constants (jpbTrue, jpbFalse, jpbUnknown)
- Called from (representative examples):
  - [executeComparison](../e/executeComparison.md)
  - RETURN_ERROR

## Notes and Other Information
The function implements SQL/JSON comparison semantics where null comparisons with non-nulls return false for equality/ordering but true for inequality. Non-scalar types (arrays, objects, binary) are considered incomparable and return jpbUnknown. For string equality operations, the function includes a fast-path optimization using direct memory comparison before falling back to the more complex Unicode-aware comparison. Error handling includes explicit type validation with elog for unexpected JsonbValue types.

## Simplified Source

```c
static JsonPathBool
compareItems(int32 op, JsonbValue *jb1, JsonbValue *jb2, bool useTz)
{
    int cmp;
    bool res;

    // Handle type mismatches
    if (jb1->type != jb2->type) {
        if (jb1->type == jbvNull || jb2->type == jbvNull)
            return op == jpiNotEqual ? jpbTrue : jpbFalse;

        // Different non-null types are incomparable
        return jpbUnknown;
    }

    // Type-specific comparison
    switch (jb1->type) {
        case jbvNull:
            cmp = 0;
            break;

        case jbvBool:
            cmp = jb1->val.boolean == jb2->val.boolean ? 0 :
                  jb1->val.boolean ? 1 : -1;
            break;

        case jbvNumeric:
            cmp = compareNumeric(jb1->val.numeric, jb2->val.numeric);
            break;

        case jbvString:
            // Fast path for equality
            if (op == jpiEqual) {
                return jb1->val.string.len != jb2->val.string.len ||
                       memcmp(jb1->val.string.val, jb2->val.string.val,
                              jb1->val.string.len) ? jpbFalse : jpbTrue;
            }
            cmp = compareStrings(jb1->val.string.val, jb1->val.string.len,
                               jb2->val.string.val, jb2->val.string.len);
            break;

        case jbvDatetime:
            bool cast_error;
            cmp = compareDatetime(jb1->val.datetime.value, jb1->val.datetime.typid,
                                jb2->val.datetime.value, jb2->val.datetime.typid,
                                useTz, &cast_error);
            if (cast_error)
                return jpbUnknown;
            break;

        case jbvBinary:
        case jbvArray:
        case jbvObject:
            return jpbUnknown;  // Non-scalars not comparable

        default:
            elog(ERROR, "invalid jsonb value type %d", jb1->type);
    }

    // Evaluate comparison result against operation
    switch (op) {
        case jpiEqual:        res = (cmp == 0); break;
        case jpiNotEqual:     res = (cmp != 0); break;
        case jpiLess:         res = (cmp < 0);  break;
        case jpiGreater:      res = (cmp > 0);  break;
        case jpiLessOrEqual:  res = (cmp <= 0); break;
        case jpiGreaterOrEqual: res = (cmp >= 0); break;
        default:
            elog(ERROR, "unrecognized jsonpath operation: %d", op);
            return jpbUnknown;
    }

    return res ? jpbTrue : jpbFalse;
}
```