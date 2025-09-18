# RowCompareType

## Location
[src/include/nodes/primnodes.h:1461-1462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1461-L1462)

## Overview
RowCompareType is an enumeration that defines the comparison operators supported for row-wise comparisons in PostgreSQL, such as (a, b) <= (1, 2).

## Definition


## Detailed Description
RowCompareType specifies the type of comparison operation for row-wise expressions where entire rows (tuples) are compared element-by-element. This enumeration supports all standard comparison operators that can be determined to act like equality, inequality, and ordering operations.

The system determines operator behavior by looking for operators in btree operator families. Importantly, the same operator name might map to different actual operators for each pair of row elements since element datatypes can vary within a row.

The enumeration values are specifically chosen to match btree strategy numbers for efficiency in operator lookup and execution. However, only the ordering operators (< <= > >=) generate actual RowCompareExpr nodes - equality and inequality comparisons are translated to simple AND/OR combinations of pairwise element comparisons.

## Parameters / Member Variables
-  (1): Less than comparison (<), matches BTLessStrategyNumber
-  (2): Less than or equal comparison (<=), matches BTLessEqualStrategyNumber  
-  (3): Equality comparison (=), matches BTEqualStrategyNumber
-  (4): Greater than or equal comparison (>=), matches BTGreaterEqualStrategyNumber
-  (5): Greater than comparison (>), matches BTGreaterStrategyNumber
-  (6): Not equal comparison (<>), no corresponding btree strategy

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this enum)
- Called from (representative examples):
  - RowCompareExpr struct (uses RowCompareType as rctype field)
  - [ExecInterpExpr](../E/ExecInterpExpr.md) function
  - llvm_compile_expr function
  - [expand_indexqual_rowcompare](../e/expand_indexqual_rowcompare.md) function
  - [make_row_comparison_op](../m/make_row_comparison_op.md) function
  - ExprEvalStep struct

## Notes and Other Information
- Enum values deliberately match btree strategy numbers for efficient operator resolution
- Only ROWCOMPARE_LT, ROWCOMPARE_LE, ROWCOMPARE_GE, and ROWCOMPARE_GT generate RowCompareExpr nodes
- ROWCOMPARE_EQ and ROWCOMPARE_NE are included for parser convenience but are transformed to AND/OR combinations
- ROWCOMPARE_NE has no corresponding btree strategy number since btree indexes don't directly support not-equal strategies
- Essential for implementing SQL row value constructors and row-wise comparisons
- Supports complex comparisons where different column pairs may use different underlying comparison operators based on their datatypes
- Critical for query optimization, especially for composite index usage with row-wise comparisons