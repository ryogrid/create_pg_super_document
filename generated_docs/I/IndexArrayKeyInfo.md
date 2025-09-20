# IndexArrayKeyInfo

## Location
[src/include/nodes/execnodes.h:1623-1650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1623-L1650)

## Overview
IndexArrayKeyInfo manages the state for index scan keys that use array expressions, supporting ScalarArrayOpExpr conditions like "column = ANY(array_expr)" in index scans.

## Definition

```c
structures for index quals
 *		NumScanKeys		   number of ScanKeys
 *		OrderByKeys		   Skey structures for index ordering operators
 *		NumOrderByKeys	   number of OrderByKeys
 *		RuntimeKeys		   info about Skeys that must be evaluated at runtime
 *		NumRuntimeKeys	   number of RuntimeKeys
 *		RuntimeKeysReady   true if runtime Skeys have been computed
 *		RuntimeContext	   expr context for evaling runtime Skeys
 *		RelationDesc	   index relation descriptor
 *		ScanDesc		   index scan descriptor
 *
 *		ReorderQueue	   tuples that need reordering due to re-check
 *		ReachedEnd		   have we fetched all tuples from index already?
 *		OrderByValues	   values of ORDER BY exprs of last fetched tuple
 *		OrderByNulls	   null flags for OrderByValues
 *		SortSupport		   for reordering ORDER BY exprs
 *		OrderByTypByVals   is the datatype of order by expression pass-by-value?
 *		OrderByTypLens	   typlens of the datatypes of order by expressions
 *		PscanLen		   size of parallel index scan descriptor
 * ----------------
 */
typedef struct IndexScanState
{
	ScanState	ss;				/* its first field is NodeTag */
	ExprState  *indexqualorig;
	List	   *indexorderbyorig;
	struct ScanKeyData *iss_ScanKeys;
	int			iss_NumScanKeys;
	struct ScanKeyData *iss_OrderByKeys;
	int			iss_NumOrderByKeys;
	IndexRuntimeKeyInfo *iss_RuntimeKeys;
	int			iss_NumRuntimeKeys;
	bool		iss_RuntimeKeysReady;
	ExprContext *iss_RuntimeContext;
	Relation	iss_RelationDesc;
	struct IndexScanDescData *iss_ScanDesc;

	/* These are needed for re-checking ORDER BY expr ordering */
	pairingheap *iss_ReorderQueue;
	bool		iss_ReachedEnd;
	Datum	   *iss_OrderByValues;
	bool	   *iss_OrderByNulls;
	SortSupport iss_SortSupport;
	bool	   *iss_OrderByTypByVals;
	int16	   *iss_OrderByTypLens;
	Size		iss_PscanLen;
} IndexScanState;
```
## Detailed Description
IndexArrayKeyInfo is a specialized structure that enables PostgreSQL to efficiently handle array-based index scan conditions, particularly ScalarArrayOpExpr operations of the form "column = ANY(array_expression)". This structure manages the stateful iteration through array elements during index scanning, allowing a single scan key to be reused with different values from the array.

The structure maintains both the array evaluation state and the current position within the array elements. When an array expression is encountered in an index scan condition, PostgreSQL evaluates the array expression once and then iterates through each element, using each value in turn with the same scan key structure. This approach enables efficient index utilization for array-based queries without requiring separate index lookups for each array element.

The structure handles NULL values appropriately through the elem_nulls array, ensuring proper SQL semantics for NULL handling in array operations.

## Parameters / Member Variables
- : Pointer to the ScanKeyData structure that will receive each array element value during iteration
- : Expression state for evaluating the array expression to obtain the complete array value
- : Index of the next array element to be processed during scan iteration
- : Total number of elements in the currently evaluated array value
- : Array containing the Datum values extracted from the evaluated array expression
- : Array of boolean flags indicating which elements in elem_values are NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyData](../S/ScanKeyData.md) (structure for index scan keys)
  - ExprState (expression evaluation state)
  - Datum (PostgreSQL's generic data value type)
- Called from (representative examples):
  - [ExecIndexEvalArrayKeys](../E/ExecIndexEvalArrayKeys.md)
  - [ExecIndexAdvanceArrayKeys](../E/ExecIndexAdvanceArrayKeys.md)
  - [ExecIndexBuildScanKeys](../E/ExecIndexBuildScanKeys.md)
  - [BitmapIndexScanState](../B/BitmapIndexScanState.md)

## Notes and Other Information
- Essential for optimizing queries with IN clauses and ANY/ALL array operators in index scans
- Implemented primarily in src/backend/executor/nodeIndexscan.c for array key management
- Enables PostgreSQL to avoid multiple separate index scans for array-based conditions
- The iteration state (next_elem) allows for resumable scanning through large arrays
- Properly handles SQL NULL semantics in array operations through the elem_nulls tracking
- Part of PostgreSQL's advanced index scanning optimizations for complex WHERE conditions
- Works in conjunction with the SK_SEARCHARRAY flag in ScanKeyData structures