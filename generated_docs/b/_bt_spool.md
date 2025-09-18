# _bt_spool

## Location
src/backend/access/nbtree/nbtsort.c: 525 - 535

## Overview
Adds a single index tuple to a BTSpool structure for temporary storage during B-tree index construction.

## Definition


## Detailed Description
 is a simple wrapper function that adds index tuple data to a spool structure during B-tree index construction. It serves as an interface between the index building callback mechanism and the underlying tuplesort functionality.

The function takes the components of an index tuple (item pointer, attribute values, and null flags) and forwards them to the tuplesort subsystem for temporary storage and sorting. This allows the index construction process to collect tuples from the heap scan and sort them efficiently before building the actual B-tree structure.

The function delegates all the actual work to , which handles the details of tuple formation, sorting, and temporary file management. This design provides a clean abstraction layer between the B-tree specific index building logic and the general-purpose tuple sorting infrastructure.

## Parameters
- : The BTSpool structure containing the tuplesort state and index metadata
- : Item pointer (TID) identifying the heap tuple location  
- : Array of Datum values for each index column
- : Array of boolean flags indicating which values are NULL

## Dependencies
- Functions called/Symbols referenced:
  -  - Adds the tuple data to the underlying tuplesort
  -  - The spool structure type
- Called from:
  -  - Index building callback function (called twice for live and dead tuples)

## Notes and Other Information
- This function is a thin wrapper that provides a B-tree specific interface to the general tuplesort functionality
- The actual tuple construction and sorting logic is handled by the tuplesort subsystem
- Called once for each tuple encountered during the heap scan phase of index construction
- The function doesn't perform any validation or processing of the input data - it simply forwards it to tuplesort
- Part of the data flow from heap scanning through temporary storage to final B-tree construction