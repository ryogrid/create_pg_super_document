# IndexRuntimeKeyInfo

## Location
[src/include/nodes/execnodes.h:1613-1622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1613-L1622)

## Overview
IndexRuntimeKeyInfo represents metadata for index scan keys that require runtime evaluation, linking scan key structures with expressions that must be computed during execution.

## Definition

```c
typedef struct
{
	struct ScanKeyData *scan_key;	/* scankey to put value into */
	ExprState  *array_expr;		/* expr to evaluate to get array value */
	int			next_elem;		/* next array element to use */
	int			num_elems;		/* number of elems in current array value */
	Datum	   *elem_values;	/* array of num_elems Datums */
	bool	   *elem_nulls;		/* array of num_elems is-null flags */
} IndexArrayKeyInfo;
```
## Detailed Description
IndexRuntimeKeyInfo is a crucial structure in PostgreSQL's index scanning infrastructure that handles dynamic scan keys where the comparison values cannot be determined at plan time. Unlike static scan keys with constant values, runtime keys require expression evaluation during execution to determine the actual values used for index lookups.

This structure bridges the gap between PostgreSQL's expression evaluation system and the low-level index access machinery. It maintains a pointer to the target ScanKeyData structure that will receive the computed value, along with the ExprState needed to evaluate the runtime expression. The structure also tracks whether the expression result is of a toastable datatype, which affects how the value is handled in the storage layer.

Runtime keys are essential for supporting parameterized queries, subplan results, and other dynamic conditions in index scans where the comparison values are not known until execution time.

## Parameters / Member Variables
- : Pointer to the ScanKeyData structure that will receive the computed value for the index scan
- : Expression state for evaluating the runtime expression to obtain the comparison value
- : Boolean flag indicating whether the expression result is a toastable datatype, affecting storage handling

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyData](../S/ScanKeyData.md) (structure for index scan keys)
  - ExprState (expression evaluation state)
- Called from (representative examples):
  - [ExecIndexEvalRuntimeKeys](../E/ExecIndexEvalRuntimeKeys.md)
  - [ExecIndexBuildScanKeys](../E/ExecIndexBuildScanKeys.md)
  - [IndexScanState](IndexScanState.md)
  - [IndexOnlyScanState](IndexOnlyScanState.md)
  - [BitmapIndexScanState](../B/BitmapIndexScanState.md)

## Notes and Other Information
- Primarily used in src/backend/executor/nodeIndexscan.c for dynamic index scan key evaluation
- Part of PostgreSQL's index scan optimization infrastructure for handling non-constant scan conditions
- The key_toastable flag is important for proper handling of variable-length and potentially compressed data types
- Runtime key evaluation occurs before each index scan to ensure current values are used
- Essential for supporting prepared statements and parameterized queries with efficient index usage
- Works in conjunction with ExecIndexBuildScanKeys() and ExecIndexEvalRuntimeKeys() functions