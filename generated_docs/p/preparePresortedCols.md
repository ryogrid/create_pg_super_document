# preparePresortedCols

## Location
[src/backend/executor/nodeIncrementalSort.c:164-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIncrementalSort.c#L164-L211)

## Overview
A static initialization function that prepares comparison functions and metadata for pre-sorted columns in an incremental sort operation.

## Definition

```c
static void
preparePresortedCols(IncrementalSortState *node)
```
## Detailed Description
This function initializes the data structures needed to perform efficient comparisons on pre-sorted columns during incremental sort execution. It allocates and populates an array of PresortedKeyData structures, one for each pre-sorted column. For each pre-sorted column, the function:

1. Extracts the column attribute number from the sort specification
2. Finds the equality operator corresponding to the ordering operator 
3. Looks up the actual comparison function for the equality operator
4. Caches the function information and pre-initializes function call info structures for efficient repeated calls

This preparation is essential for the incremental sort algorithm to quickly determine when the values in pre-sorted columns change, which triggers the need to sort the accumulated group of tuples.

## Parameters / Member Variables
- `*node`: Pointer to IncrementalSortState structure that will be populated with pre-sorted column comparison information
## Dependencies
- Functions called/Symbols referenced:
  - castNode (macro to safely cast plan node)
  - [palloc](palloc.md) (memory allocation)
  - [get_equality_op_for_ordering_op](../g/get_equality_op_for_ordering_op.md) (finds equality operator for sort operator)
  - [get_opcode](../g/get_opcode.md) (gets function OID for operator)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (initializes function manager info)
  - SizeForFunctionCallInfo (calculates size for function call structure)
  - InitFunctionCallInfoData (initializes function call info structure)
  - [IncrementalSort](../I/IncrementalSort.md) (plan node type)
  - [PresortedKeyData](../P/PresortedKeyData.md) (structure for storing pre-sorted key information)
- Called from (representative examples):
  - [ExecIncrementalSort](../E/ExecIncrementalSort.md) (main execution function for incremental sort)

## Notes and Other Information
- This function is called once during incremental sort initialization to cache all comparison functions
- The pre-initialized function call info structures avoid the overhead of repeated initialization during tuple comparison
- Error checking ensures that valid equality operators and functions exist for all sort operators
- The cached comparison functions are used by other functions like isCurrentGroup to efficiently detect group boundaries
- Memory is allocated in the CurrentMemoryContext to persist for the duration of the query execution

## Simplified Source

```c
static void preparePresortedCols(IncrementalSortState *node)
{
    IncrementalSort *plannode = castNode(IncrementalSort, node->ss.ps.plan);

    // Allocate array for presorted key comparison data
    node->presorted_keys = (PresortedKeyData *) palloc(plannode->nPresortedCols * sizeof(PresortedKeyData));

    // Pre-cache comparison functions for each pre-sorted column
    for (int i = 0; i < plannode->nPresortedCols; i++)
    {
        Oid equalityOp, equalityFunc;
        PresortedKeyData *key = &node->presorted_keys[i];

        // Store the column attribute number
        key->attno = plannode->sort.sortColIdx[i];

        // Find equality operator for the sort operator
        equalityOp = get_equality_op_for_ordering_op(plannode->sort.sortOperators[i], NULL);
        if (!OidIsValid(equalityOp))
            elog(ERROR, "missing equality operator for ordering operator %u",
                 plannode->sort.sortOperators[i]);

        // Get the actual function for the equality operator
        equalityFunc = get_opcode(equalityOp);
        if (!OidIsValid(equalityFunc))
            elog(ERROR, "missing function for operator %u", equalityOp);

        // Cache function manager information
        fmgr_info_cxt(equalityFunc, &key->flinfo, CurrentMemoryContext);

        // Pre-initialize function call info for efficiency
        key->fcinfo = palloc0(SizeForFunctionCallInfo(2));
        InitFunctionCallInfoData(*key->fcinfo, &key->flinfo, 2,
                                 plannode->sort.collations[i], NULL, NULL);

        // Mark arguments as non-null by default
        key->fcinfo->args[0].isnull = false;
        key->fcinfo->args[1].isnull = false;
    }
}
```