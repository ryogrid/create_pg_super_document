# spgExtractNodeLabels

## Location
[src/backend/access/spgist/spgutils.c:1152-1194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L1152-L1194)

## Overview
Extracts the label datums from child nodes within an SP-GiST inner tuple, returning an array of label values or NULL if all labels are null.

## Definition

```c
Datum *
spgExtractNodeLabels(SpGistState *state, SpGistInnerTuple innerTuple)
```
## Detailed Description
The  function retrieves the label values from all child nodes contained within an SP-GiST inner tuple. It enforces the constraint that either all node labels must be NULL or none can be NULL - mixed states are considered an error condition. If all labels are null, the function returns NULL instead of allocating an array. For non-null labels, it allocates memory and extracts each label datum using the appropriate type handling.

The function iterates through all nodes using the  macro and validates the consistency of null states across all nodes. This validation is crucial for maintaining the integrity of the SP-GiST index structure, where label nullness must be uniform within a single inner tuple.

## Parameters / Member Variables
- `*state`: Pointer to SpGistState containing index configuration and type information needed for datum extraction
- `innerTuple`: The SP-GiST inner tuple containing the nodes whose labels should be extracted
## Dependencies
- Functions called/Symbols referenced:
  -  - macro to get pointer to the first node in the inner tuple
  -  - checks if a node tuple contains null values
  -  - macro for iterating through all nodes in an inner tuple
  -  - macro to extract the datum value from a node tuple
  -  - allocates memory for the label array
- Called from (representative examples):
  -  - during insertion operations to access node labels
  -  - [when](../w/when.md) initializing consistency checking for inner nodes

## Notes and Other Information
- Enforces strict consistency: either all node labels are NULL or none are NULL
- Returns NULL when all labels are null to avoid allocating unnecessary memory
- Uses the state parameter to handle type-specific datum extraction via SGNTDATUM
- The returned array must be freed by the caller when non-NULL
- Critical for SP-GiST operations that need to examine or compare node labels
- Error conditions result in elog(ERROR) calls, which abort the current operation
- The uniformity requirement for null states simplifies label processing logic throughout the SP-GiST codebase

## Simplified Source

```c
Datum *
spgExtractNodeLabels(SpGistState *state, SpGistInnerTuple innerTuple)
{
    Datum *nodeLabels;
    int i;
    SpGistNodeTuple node;

    // Check first node to determine if all labels are null
    node = SGITNODEPTR(innerTuple);
    if (IndexTupleHasNulls(node))
    {
        // All labels must be null - verify consistency
        SGITITERATE(innerTuple, i, node)
        {
            if (!IndexTupleHasNulls(node))
                elog(ERROR, "some but not all node labels are null in SPGiST inner tuple");
        }
        return NULL;  // All null labels
    }
    else
    {
        // Extract non-null labels
        nodeLabels = (Datum *) palloc(sizeof(Datum) * innerTuple->nNodes);
        SGITITERATE(innerTuple, i, node)
        {
            if (IndexTupleHasNulls(node))
                elog(ERROR, "some but not all node labels are null in SPGiST inner tuple");
            nodeLabels[i] = SGNTDATUM(node, state);
        }
        return nodeLabels;
    }
}
```