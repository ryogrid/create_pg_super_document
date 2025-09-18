# gistpenalty

## Location
[src/backend/access/gist/gistutil.c:723-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L723-L755)

## Overview
Calculates the penalty value for inserting a new entry into a GiST index node by calling the operator class penalty function, with special handling for NULL values and result validation.

## Definition
```c
float gistpenalty(GISTSTATE *giststate, int attno,
                 GISTENTRY *orig, bool isNullOrig,
                 GISTENTRY *add, bool isNullAdd)
```

## Detailed Description
This function computes the penalty for adding a new entry to an existing GiST index node entry. The penalty represents the cost of enlarging the existing node's key to accommodate the new entry. It calls the penalty function from the operator class, which measures how much the original key would need to be enlarged to include the new key. The function handles NULL values specially: if both values are NULL, the penalty is 0; if only one is NULL, it returns infinity to discourage mixing NULL and non-NULL values. It also validates the result by ensuring the penalty is not negative or NaN.

## Parameters / Member Variables
- `giststate`: GiST state information containing operator class functions and collation information
- `attno`: The attribute number (index) being processed
- `orig`: GISTENTRY representing the original/existing key in the index node
- `isNullOrig`: Boolean flag indicating if the original entry is NULL
- `add`: GISTENTRY representing the new key to be added
- `isNullAdd`: Boolean flag indicating if the new entry is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall3Coll](../F/FunctionCall3Coll.md)
  - isnan
  - get_float4_infinity
  - [GISTENTRY](../G/GISTENTRY.md) (struct)
  - [GISTSTATE](../G/GISTSTATE.md) (struct)
- Called from (representative examples):
  - gistchoose
  - [findDontCares](../f/findDontCares.md)
  - [placeOne](../p/placeOne.md)
  - [supportSecondarySplit](../s/supportSecondarySplit.md)
  - [gistRelocateBuildBuffersOnSplit](gistRelocateBuildBuffersOnSplit.md)

## Notes and Other Information
- The penalty function is crucial for GiST's insertion algorithm, helping determine the best path down the tree
- Lower penalties indicate better placement choices, guiding the algorithm to minimize index bloat
- The function respects the operator class penalty function's strict flag, only calling it with non-NULL values if strict
- Special handling prevents mixing NULL and non-NULL values by assigning infinite penalty
- [Result](../R/Result.md) validation ensures penalties are always non-negative finite values
- Used extensively during index splits and node selection for insertions
- The penalty value influences both insertion performance and index quality by minimizing key enlargement