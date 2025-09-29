# ExecInitJunkFilter

## Location
[src/backend/executor/execJunk.c:60-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execJunk.c#L60-L136)

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
  - [ExecCleanTypeFromTL](ExecCleanTypeFromTL.md): Generates tuple descriptor excluding junk columns
  - ExecSetSlotDescriptor: Sets tuple descriptor for existing slot
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md): Creates new virtual tuple slot if none provided
  - [JunkFilter](../J/JunkFilter.md): The result structure type
- Called from (representative examples):
  - [ExecInitWholeRowVar](ExecInitWholeRowVar.md): For whole-row variable initialization
  - [InitPlan](../I/InitPlan.md): During plan initialization in main executor
  - [init_sql_fcache](../i/init_sql_fcache.md): For SQL function caching setup

## Notes and Other Information
- The function includes a TODO comment suggesting this should be rewritten to use ExecProject() and ProjectionInfo nodes for better integration
- The cleanMap array is allocated only when there are non-junk attributes (cleanLength > 0)
- Uses virtual tuple table slot operations (TTSOpsVirtual) for efficiency when creating new slots
- Part of PostgreSQL's junk attribute filtering system that allows internal executor attributes to be cleanly separated from user-visible results

## Simplified Source

```c
JunkFilter *
ExecInitJunkFilter(List *targetList, TupleTableSlot *slot)
{
    JunkFilter *junkfilter;
    TupleDesc cleanTupType;
    int cleanLength;
    AttrNumber *cleanMap;

    // Compute the tuple descriptor for the cleaned tuple
    cleanTupType = ExecCleanTypeFromTL(targetList);

    // Use the given slot, or make a new slot if we weren't given one
    if (slot)
        ExecSetSlotDescriptor(slot, cleanTupType);
    else
        slot = MakeSingleTupleTableSlot(cleanTupType, &TTSOpsVirtual);

    // Calculate mapping between original and "clean" tuple attributes
    cleanLength = cleanTupType->natts;
    if (cleanLength > 0)
    {
        AttrNumber cleanResno;
        ListCell *t;

        cleanMap = (AttrNumber *) palloc(cleanLength * sizeof(AttrNumber));
        cleanResno = 0;
        foreach(t, targetList)
        {
            TargetEntry *tle = lfirst(t);

            if (!tle->resjunk)
            {
                cleanMap[cleanResno] = tle->resno;
                cleanResno++;
            }
        }
        Assert(cleanResno == cleanLength);
    }
    else
        cleanMap = NULL;

    // Create and initialize the JunkFilter struct
    junkfilter = makeNode(JunkFilter);
    junkfilter->jf_targetList = targetList;
    junkfilter->jf_cleanTupType = cleanTupType;
    junkfilter->jf_cleanMap = cleanMap;
    junkfilter->jf_resultSlot = slot;

    return junkfilter;
}
```