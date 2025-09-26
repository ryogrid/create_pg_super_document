# TidListEval

## Location
[src/backend/executor/nodeTidscan.c:134-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidscan.c#L134-L282)

## Overview
TidListEval is a static function that evaluates TID (Tuple Identifier) expressions to compute a sorted, deduplicated array of TIDs to be visited during a TID scan operation.

## Definition
```c
static void TidListEval(TidScanState *tidstate)
```

## Detailed Description
This function is responsible for computing the list of TIDs that need to be visited during a TID scan. It evaluates various types of TID expressions including simple OpExprs, ScalarArrayOpExprs, and CurrentOfExprs to build a comprehensive list of target TIDs.

The function handles three main types of TID expressions:
1. **Simple expressions** (tidexpr->exprstate && !tidexpr->isarray): Single TID values from regular expressions
2. **Array expressions** (tidexpr->exprstate && tidexpr->isarray): Multiple TID values from array expressions  
3. **CurrentOf expressions** (tidexpr->cexpr): TIDs from cursor CURRENT OF clauses

The function includes several important optimizations and safety measures:
- Lazy initialization of the table scan descriptor
- Dynamic array resizing when more TIDs are found than initially allocated
- Validation of TID validity using table_tuple_tid_valid()
- Sorting and deduplication of the final TID list for optimal access patterns
- Proper handling of NULL values and invalid TIDs

## Parameters / Member Variables
- `tidstate`: Pointer to TidScanState structure that maintains the state for TID scan execution. The function updates multiple fields including tss_TidList, tss_NumTids, and tss_TidPtr.

## Dependencies
- Functions called/Symbols referenced:
  - table_beginscan_tid
  - ExecEvalExprSwitchContext
  - table_tuple_tid_valid
  - DatumGetArrayTypeP
  - deconstruct_array_builtin
  - execCurrentOf
  - repalloc
  - pjfree
  - qsort
  - qunique
  - itemptr_comparator
  - DatumGetPointer
  - RelationGetRelid
- Types used:
  - TidScanState
  - ExprContext
  - TableScanDesc
  - ItemPointerData
  - TidExpr
  - ArrayType
  - ItemPointer
- Called from:
  - TidNext

## Notes and Other Information
- This is a static function, only accessible within nodeTidscan.c
- Implements lazy initialization of table scan descriptor for performance reasons
- Dynamically allocates and grows the TID array as needed to handle ScalarArrayOpExprs efficiently
- The final TID list is sorted using itemptr_comparator and deduplicated using qunique for optimal heap access patterns
- Silently discards invalid TIDs rather than throwing errors, providing robust operation
- Handles OR semantics across multiple TID expressions by collecting all valid TIDs
- Part of PostgreSQL's executor infrastructure for direct tuple access via TID values