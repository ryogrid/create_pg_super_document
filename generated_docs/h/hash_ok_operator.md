# hash_ok_operator

## Location
[src/backend/optimizer/plan/subselect.c:832-879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L832-L879)

## Overview
Validates whether an operator expression is both hashable and strict, making it suitable for hash-based operations in query execution.

## Definition
```c
static bool hash_ok_operator(OpExpr *expr)
```

## Detailed Description
This function determines if an operator can be used in hash-based query execution strategies by checking two critical properties:

1. **Hashability**: The operator must support hashing operations, which is essential for hash joins and hash-based subplan execution.
2. **Strictness**: The operator must be strict (returns NULL if any input is NULL), ensuring predictable behavior with NULL values.

The function implements an optimized approach by handling special cases for ARRAY_EQ_OP and RECORD_EQ_OP operators, which are known to be strict but require additional type checking for hashability. For other operators, it performs a system catalog lookup to check the operator properties.

The implementation avoids redundant cache lookups by combining the checks for both properties in a single function, rather than calling separate op_hashjoinable() and op_strict() functions.

## Parameters
- `expr`: The OpExpr (operator expression) to be validated for hash compatibility

## Dependencies
- Functions called/Symbols referenced:
  - [op_hashjoinable](../o/op_hashjoinable.md)
  - exprType
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [func_strict](../f/func_strict.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - linitial
  - list_length
- Called from (representative examples):
  - [test_opexpr_is_hashable](../t/test_opexpr_is_hashable.md)
  - [convert_EXISTS_to_ANY](../c/convert_EXISTS_to_ANY.md)

## Notes and Other Information
- Only binary operators (exactly 2 arguments) are considered valid for hashing
- Special handling for array and record equality operators which are strict by definition
- Uses system catalog caching for efficient operator property lookup
- The function performs error checking for missing operators in the system catalog
- Optimization technique combines multiple property checks to avoid redundant cache operations