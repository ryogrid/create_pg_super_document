# ResTarget

## Location
src/include/nodes/parsenodes.h: 514 - 521

## Overview
ResTarget represents result targets in PostgreSQL parse trees, used across SELECT, INSERT, and UPDATE statements to specify columns, expressions, and their destinations.

## Definition
```c
typedef struct ResTarget
{
    NodeTag     type;
    char       *name;           /* column name or NULL */
    List       *indirection;    /* subscripts, field names, and '*', or NIL */
    Node       *val;            /* the value expression to compute or assign */
    ParseLoc    location;       /* token location, or -1 if unknown */
} ResTarget;
```

## Detailed Description
ResTarget is a versatile parse tree node that serves different purposes depending on the SQL statement context. In SELECT target lists, 'name' represents the column label from an AS clause (or NULL if absent), while 'val' contains the value expression, and 'indirection' is unused. For INSERT statements, ResTarget appears in target-column-names lists where 'name' is the destination column name, 'indirection' stores subscripts for complex destinations, and 'val' is unused. In UPDATE target lists, 'name' identifies the destination column, 'indirection' handles subscripts for array/composite updates, and 'val' contains the assignment expression. The indirection field can contain the same types of nodes as A_Indirection.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a ResTarget node
- `name`: Column name string or NULL (usage varies by statement type)
- `indirection`: List containing subscripts, field names, and wildcard selectors, or NIL if not used
- `val`: Expression node for the value to compute or assign (usage varies by statement type)
- `location`: ParseLoc indicating position in source query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited node type system)
  - List (PostgreSQL's list data structure)
  - Node (base node type for expressions)
  - ParseLoc (parse location tracking type)
- Called from (representative examples):
  - transformInsertStmt (src/backend/parser/analyze.c:947)
  - transformInsertRow (src/backend/parser/analyze.c:1062)
  - transformUpdateTargetList (src/backend/parser/analyze.c:2506, 2523)
  - transformTargetList (src/backend/parser/parse_target.c:136)
  - transformMergeStmt (src/backend/parser/parse_merge.c:367)
  - DoCopy (src/backend/commands/copy.c:186, 214, 238)
  - checkInsertTargets (src/backend/parser/parse_target.c:1030, 1038, 1058)

## Notes and Other Information
- Central to PostgreSQL's target list processing across multiple statement types
- Provides unified representation for column targets in different SQL contexts
- Supports complex destination specifications through indirection lists
- Essential for query transformation and analysis phases
- Location tracking enables accurate error reporting during parsing
- Used extensively in INSERT, UPDATE, SELECT, and MERGE statement processing
- Integrates with PostgreSQL's indirection mechanism for complex data structure updates