# BitmapHeapRecheck

## Location
[src/backend/executor/nodeBitmapHeapscan.c:562-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapHeapscan.c#L562-L580)

## Overview
BitmapHeapRecheck is an access method routine used to recheck a tuple during EvalPlanQual processing in bitmap heap scans, ensuring the tuple still meets the original qualification conditions after potential concurrent updates.

## Definition

```c
static bool
BitmapHeapRecheck(BitmapHeapScanState *node, TupleTableSlot *slot)
```
## Detailed Description
BitmapHeapRecheck is a helper function specifically designed for EvalPlanQual (EPQ) processing within bitmap heap scans. When PostgreSQL needs to handle concurrent updates during query execution, EPQ is invoked to recheck whether tuples still satisfy the original query conditions after being modified by other transactions. This function extracts the expression context from the bitmap heap scan node and evaluates the original qualification conditions against the provided tuple slot.

The function is essential for maintaining MVCC (Multi-Version Concurrency Control) consistency by ensuring that tuples retrieved during bitmap heap scans still meet the query's WHERE clause conditions, even after potential modifications by concurrent transactions.

## Parameters / Member Variables
- : BitmapHeapScanState pointer containing the bitmap heap scan execution state, including the expression context and original qualification conditions
- : TupleTableSlot pointer containing the tuple to be rechecked against the original qualification conditions

## Dependencies
- Functions called/Symbols referenced:
  - [ExecQualAndReset](../E/ExecQualAndReset.md)
- Data types referenced:
  - [BitmapHeapScanState](BitmapHeapScanState.md)
  - [TupleTableSlot](../T/TupleTableSlot.md)
  - [ExprContext](../E/ExprContext.md)
- Called from:
  - [ExecBitmapHeapScan](../E/ExecBitmapHeapScan.md) (src/backend/executor/nodeBitmapHeapscan.c:587)

## Notes and Other Information
- This is a static function, only accessible within the nodeBitmapHeapscan.c file
- The function is specifically designed for EvalPlanQual processing, not regular tuple qualification
- It reuses the original qualification conditions (bitmapqualorig) stored in the scan state
- The function uses ExecQualAndReset to evaluate conditions and automatically reset the expression context
- Part of PostgreSQL's MVCC implementation for handling concurrent tuple modifications during bitmap heap scans