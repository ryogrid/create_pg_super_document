# JunkFilter

## Location
src/include/nodes/execnodes.h: 393 - 400

## Overview
JunkFilter is a structure used to store information regarding junk attributes and filter them from tuples, keeping only the attributes needed in the final output.

## Definition


## Detailed Description
JunkFilter handles junk attributes, which are attributes in a tuple needed only for storing intermediate information in the executor and do not belong in emitted tuples. For example, during UPDATE queries, the planner adds a "junk" entry to the targetlist containing the ctid of the tuple to be updated. This ctid is needed for the update operation but should not be part of the stored new tuple.

The junk filter removes junk attributes to form the real output tuple. It also provides routines to extract the values of junk attributes from the input tuple when needed for execution purposes.

## Parameters / Member Variables
- : NodeTag identifier for the structure type
- : The original target list including junk attributes  
- : The tuple descriptor for the "clean" tuple with junk attributes removed
- : A mapping between non-junk attribute numbers of the original tuple and attribute numbers of the clean tuple
- : Tuple slot used to hold the cleaned tuple

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - [List](../L/List.md)
  - [TupleDesc](../T/TupleDesc.md)
  - AttrNumber
  - TupleTableSlot
- Called from (representative examples):
  - ExecInitJunkFilter
  - ExecFilterJunk
  - ExecFindJunkAttribute
  - [InitPlan](../I/InitPlan.md)

## Notes and Other Information
- Essential for UPDATE and DELETE operations where row identifiers (ctid) are needed for execution but not for output
- Provides a clean separation between execution-needed data and user-visible results
- The cleanMap provides efficient mapping between original and filtered attribute positions
- Used extensively in modify table operations and plan execution
- Helps maintain data integrity by ensuring only intended attributes reach the final result set