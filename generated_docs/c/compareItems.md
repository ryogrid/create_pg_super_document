# compareItems

## Location
src/backend/utils/adt/jsonpath_exec.c: 3341 - 3436

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
  - compareNumeric
  - compareStrings
  - compareDatetime
  - memcmp
  - elog
  - Various JsonbValue type constants (jbvNull, jbvBool, jbvNumeric, jbvString, jbvDatetime, jbvBinary, jbvArray, jbvObject)
  - JsonPath operation constants (jpiEqual, jpiNotEqual, jpiLess, etc.)
  - JsonPathBool constants (jpbTrue, jpbFalse, jpbUnknown)
- Called from (representative examples):
  - executeComparison
  - RETURN_ERROR

## Notes and Other Information
The function implements SQL/JSON comparison semantics where null comparisons with non-nulls return false for equality/ordering but true for inequality. Non-scalar types (arrays, objects, binary) are considered incomparable and return jpbUnknown. For string equality operations, the function includes a fast-path optimization using direct memory comparison before falling back to the more complex Unicode-aware comparison. Error handling includes explicit type validation with elog for unexpected JsonbValue types.