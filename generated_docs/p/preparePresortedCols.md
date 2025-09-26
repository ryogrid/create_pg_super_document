# preparePresortedCols

## Location
src/backend/executor/nodeIncrementalSort.c: 164 - 211

## Overview
A static initialization function that prepares comparison functions and metadata for pre-sorted columns in an incremental sort operation.

## Definition


## Detailed Description
This function initializes the data structures needed to perform efficient comparisons on pre-sorted columns during incremental sort execution. It allocates and populates an array of PresortedKeyData structures, one for each pre-sorted column. For each pre-sorted column, the function:

1. Extracts the column attribute number from the sort specification
2. Finds the equality operator corresponding to the ordering operator 
3. Looks up the actual comparison function for the equality operator
4. Caches the function information and pre-initializes function call info structures for efficient repeated calls

This preparation is essential for the incremental sort algorithm to quickly determine when the values in pre-sorted columns change, which triggers the need to sort the accumulated group of tuples.

## Parameters / Member Variables
- : Pointer to IncrementalSortState structure that will be populated with pre-sorted column comparison information

## Dependencies
- Functions called/Symbols referenced:
  - castNode (macro to safely cast plan node)
  - palloc (memory allocation)
  - get_equality_op_for_ordering_op (finds equality operator for sort operator)
  - get_opcode (gets function OID for operator)
  - fmgr_info_cxt (initializes function manager info)
  - SizeForFunctionCallInfo (calculates size for function call structure)
  - InitFunctionCallInfoData (initializes function call info structure)
  - IncrementalSort (plan node type)
  - PresortedKeyData (structure for storing pre-sorted key information)
- Called from (representative examples):
  - ExecIncrementalSort (main execution function for incremental sort)

## Notes and Other Information
- This function is called once during incremental sort initialization to cache all comparison functions
- The pre-initialized function call info structures avoid the overhead of repeated initialization during tuple comparison
- Error checking ensures that valid equality operators and functions exist for all sort operators
- The cached comparison functions are used by other functions like isCurrentGroup to efficiently detect group boundaries
- Memory is allocated in the CurrentMemoryContext to persist for the duration of the query execution