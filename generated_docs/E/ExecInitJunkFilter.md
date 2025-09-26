# ExecInitJunkFilter

## Location
src/backend/executor/execJunk.c: 60 - 136

## Overview
Initializes a JunkFilter data structure that manages the filtering of "junk" attributes from tuples during query execution, creating a mapping between original tuples and "clean" tuples without junk attributes.

## Definition

```c
struct.
	 */
	junkfilter = makeNode(JunkFilter);
```
## Detailed Description
ExecInitJunkFilter creates and initializes a JunkFilter structure that serves as the foundation for managing junk attributes in PostgreSQL's executor. Junk attributes are special columns that exist only within the executor for internal purposes (like system attributes "ctid" or sort keys) and are never exposed to the final output.

The function performs several key operations:
1. Computes a "clean" tuple descriptor that excludes all junk attributes using ExecCleanTypeFromTL
2. Sets up or creates a TupleTableSlot for the cleaned tuples
3. Creates a mapping array that correlates positions between original and clean tuples
4. Initializes the JunkFilter structure with all necessary components

The mapping mechanism is crucial - it creates an array where each position corresponds to a "clean" attribute and contains the attribute number from the original tuple, enabling efficient attribute projection during tuple filtering.

## Parameters / Member Variables
- : List of TargetEntry nodes representing the query's target list, where each entry may be marked as junk via the resjunk field
- : Optional TupleTableSlot to use for clean tuples; if NULL, a new virtual slot is created

## Dependencies
- Functions called/Symbols referenced:
  - ExecCleanTypeFromTL: Generates tuple descriptor excluding junk columns
  - ExecSetSlotDescriptor: Sets tuple descriptor for existing slot
  - MakeSingleTupleTableSlot: Creates new virtual tuple slot if none provided
  - JunkFilter: The result structure type
- Called from (representative examples):
  - ExecInitWholeRowVar: For whole-row variable initialization
  - InitPlan: During plan initialization in main executor
  - init_sql_fcache: For SQL function caching setup

## Notes and Other Information
- The function includes a TODO comment suggesting this should be rewritten to use ExecProject() and ProjectionInfo nodes for better integration
- The cleanMap array is allocated only when there are non-junk attributes (cleanLength > 0)
- Uses virtual tuple table slot operations (TTSOpsVirtual) for efficiency when creating new slots
- Part of PostgreSQL's junk attribute filtering system that allows internal executor attributes to be cleanly separated from user-visible results