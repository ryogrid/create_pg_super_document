# makeTargetEntry

## Location
src/backend/nodes/makefuncs.c: 287 - 319

## Overview
Creates a TargetEntry node in PostgreSQL's query tree structure, representing a single output column or expression in the target list of a query.

## Definition


## Detailed Description
The  function allocates and initializes a new TargetEntry node, which is a fundamental component in PostgreSQL's query processing system. TargetEntry nodes represent items in the target list (SELECT clause) of a query, including both regular output columns and internal "junk" columns used for query processing but not returned to the user.

The function creates a new TargetEntry node using PostgreSQL's node allocation system and initializes the essential fields. It deliberately sets several fields to default values (0 or InvalidOid) to reduce the chance of errors, requiring callers to explicitly modify these fields if needed.

## Parameters / Member Variables
- : Expression to be evaluated for this target entry (can be a column reference, function call, constant, etc.)
- : Result column number (position in the target list, starting from 1)  
- : Result column name (can be NULL for unnamed expressions)
- : Boolean flag indicating whether this is a "junk" column (used internally but not returned to user)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (macro for node allocation)
  - TargetEntry (node type being created)
  - InvalidOid (constant for invalid object identifier)
- Called from (representative examples):
  - transformTargetEntry (parser)
  - build_physical_tlist (optimizer)
  - add_to_flat_tlist (optimizer utilities)
  - rewriteTargetListIU (rewriter)

## Notes and Other Information
- The function automatically initializes , , and  to 0/InvalidOid, requiring explicit modification by callers when these values are needed
- This is a core function used extensively throughout the parser, optimizer, and rewriter subsystems
- The  flag is crucial for distinguishing between user-visible columns and internal processing columns
- Located in src/backend/nodes/makefuncs.c:287-319