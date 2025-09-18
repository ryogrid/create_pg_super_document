# mark_dummy_rel

## Location
src/backend/optimizer/path/joinrels.c: 1382 - 1424

## Overview
Marks a relation as proven empty by creating a childless Append path and updating the relation's metadata, with special memory context handling for GEQO planning scenarios.

## Definition


## Detailed Description
This function transforms a RelOptInfo into a "dummy" relation that represents an empty result set. When the optimizer determines that a relation will contain no rows (through constraint exclusion, contradictory WHERE clauses, or other logical analysis), this function creates the appropriate path structure and metadata to represent that emptiness.

The function performs several key operations:
1. Checks if the relation is already marked as dummy to avoid redundant work
2. Switches to the relation's own memory context to ensure proper memory management
3. Sets the row count to zero and clears existing paths
4. Creates a childless Append path, which serves as the canonical "dummy" path representation
5. Updates the relation's cost estimates through set_cheapest()

Special attention is paid to memory context management, particularly for GEQO (Genetic Query Optimization) planning. During GEQO cycles, the function ensures that dummy paths for base relations survive across multiple GEQO iterations while avoiding memory context pollution for join relations.

The childless Append path created by this function will be recognized by is_dummy_rel() and other parts of the optimizer, allowing them to take appropriate shortcuts when processing empty relations.

## Parameters / Member Variables
- : Pointer to the RelOptInfo to be marked as empty/dummy

## Dependencies
- Functions called/Symbols referenced:
  -  - Checks if relation is already marked as dummy
  -  - Gets the memory context of the relation structure
  -  - Adds the dummy path to the relation's pathlist
  -  - Creates a childless Append path representing emptiness
  -  - Updates the relation's cost and path selection metadata

- Called from (representative examples):
  -  - Marks empty partition joins
  -  - Marks join relations that become empty
  -  - Marks base relations determined to be empty
  - Referenced in  header for external visibility

## Notes and Other Information
- The function is idempotent - can be called multiple times safely on the same relation
- Memory context switching ensures dummy paths survive GEQO cycles for base relations
- The created dummy path has zero cost, making it the cheapest option
- Clears both regular and partial pathlist to ensure clean state
- Works in conjunction with is_dummy_rel() to provide complete dummy relation support
- Located in src/backend/optimizer/path/joinrels.c:1382-1424