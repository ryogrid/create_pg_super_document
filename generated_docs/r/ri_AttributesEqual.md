# ri_AttributesEqual

## Location
[src/backend/utils/adt/ri_triggers.c:2866-2907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2866-L2907)

## Overview
Performs semantic equality comparison between two attribute values using the appropriate equality operator, with optional type casting.

## Definition
```c
static bool ri_AttributesEqual(Oid eq_opr, Oid typeid, Datum oldvalue, Datum newvalue)
```

## Detailed Description
This function implements type-aware equality comparison for referential integrity constraint checking. It retrieves the appropriate comparison operator information from a hash table cache and applies the necessary type casting before performing the equality test.

The function handles type coercion when needed by applying cast functions before comparison. It uses the default collation for string comparisons, which may lead to some false negatives in cross-table foreign key scenarios but provides a practical balance between correctness and simplicity.

The comparison is performed using the PostgreSQL function call interface with collation support, ensuring that complex data types are compared according to their defined equality semantics rather than simple bitwise comparison.

## Parameters / Member Variables
- `eq_opr`: Object ID of the equality operator to use for comparison
- `typeid`: Object ID of the data type being compared
- `oldvalue`: The original attribute value (as a Datum)
- `newvalue`: The new attribute value to compare against (as a Datum)

## Dependencies
- Functions called/Symbols referenced:
  - [ri_HashCompareOp](ri_HashCompareOp.md) (retrieves cached comparison operator information)
  - FunctionCall3 (performs type casting when required)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (executes the equality comparison with collation)
- Called from (representative examples):
  - [ri_KeysEqual](ri_KeysEqual.md) (when comparing foreign key attributes for equality)

## Notes and Other Information
- Assumes both input values are non-NULL (caller responsibility to check)
- Uses DEFAULT_COLLATION_OID for string comparisons, which may not be optimal for cross-table comparisons
- Performs automatic type casting when the cached comparison entry includes a cast function
- Returns boolean result indicating whether the values are considered equal
- Part of the referential integrity system's optimization for determining when constraint checks are necessary
- Could produce false negatives in scenarios involving different collations between related tables