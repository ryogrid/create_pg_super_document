# compareJsonbScalarValue

## Location
[src/backend/utils/adt/jsonb_util.c:1439-1483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1439-L1483)

## Overview
compareJsonbScalarValue performs three-way comparison between two JsonbValue scalar values, returning -1, 0, or 1 for use in B-tree operations and sorting.

## Definition

```c
static int
compareJsonbScalarValue(JsonbValue *a, JsonbValue *b)
```
## Detailed Description
This static function implements lexicographic ordering comparison for JsonbValue scalar types, designed specifically for B-tree operators where consistent sort ordering is required. It handles type-specific comparison logic: null values are always equal (return 0), string values are compared using varstr_cmp with the default collation for proper lexical ordering, numeric values use PostgreSQL's numeric_cmp function to ensure mathematically correct ordering, and boolean values are compared with false < true ordering. The function enforces type safety by requiring identical types and generating errors for mismatched or invalid types.

## Parameters / Member Variables
- `*a`: Pointer to the first JsonbValue scalar for comparison
- `*b`: Pointer to the second JsonbValue scalar for comparison
## Dependencies
- Functions called/Symbols referenced:
  - [varstr_cmp](../v/varstr_cmp.md) (for string comparison with collation)
  - DirectFunctionCall2 (for calling numeric_cmp)
  - [numeric_cmp](../n/numeric_cmp.md) (for numeric comparison)
  - [PointerGetDatum](../P/PointerGetDatum.md) (for datum conversion)
  - [DatumGetInt32](../D/DatumGetInt32.md) (for integer result conversion)
  - DEFAULT_COLLATION_OID (collation constant)
- Called from (representative examples):
  - [compareJsonbContainers](compareJsonbContainers.md)

## Notes and Other Information
The function is declared static and limited to jsonb_util.c scope. It provides consistent ordering semantics required for B-tree indexing and sorting operations on JSONB data. String comparisons respect PostgreSQL's default collation rules, ensuring proper locale-aware sorting. Boolean comparison follows the convention that false (0) is less than true (1). The function will generate ERROR conditions for type mismatches or invalid scalar types, maintaining type safety in comparison operations.

## Simplified Source

```c
static int compareJsonbScalarValue(JsonbValue *a, JsonbValue *b) {
    // Only compare values of same type
    if (a->type == b->type) {
        switch (a->type) {
            case jbvNull:
                return 0;  // All nulls are equal
            case jbvString:
                return varstr_cmp(a->val.string.val,
                                a->val.string.len,
                                b->val.string.val,
                                b->val.string.len,
                                DEFAULT_COLLATION_OID);
            case jbvNumeric:
                return DatumGetInt32(DirectFunctionCall2(numeric_cmp,
                    PointerGetDatum(a->val.numeric),
                    PointerGetDatum(b->val.numeric)));
            case jbvBool:
                if (a->val.boolean == b->val.boolean)
                    return 0;
                else if (a->val.boolean > b->val.boolean)
                    return 1;
                else
                    return -1;
            default:
                elog(ERROR, "invalid jsonb scalar type");
        }
    }
    elog(ERROR, "jsonb scalar type mismatch");
    return -1;
}
```