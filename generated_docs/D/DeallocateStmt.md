# DeallocateStmt

## Location
[src/include/nodes/parsenodes.h:4056-4070](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4056-L4070)

## Overview
DeallocateStmt represents the parsed form of a DEALLOCATE SQL statement, which removes a previously prepared statement from the current session.

## Definition
```c
typedef struct DeallocateStmt
{
    NodeTag     type;
    /* The name of the plan to remove, NULL if DEALLOCATE ALL */
    char       *name pg_node_attr(query_jumble_ignore);

    /*
     * True if DEALLOCATE ALL.  This is redundant with "name == NULL", but we
     * make it a separate field so that exactly this condition (and not the
     * precise name) will be accounted for in query jumbling.
     */
    bool        isall;
    /* token location, or -1 if unknown */
    ParseLoc    location pg_node_attr(query_jumble_location);
} DeallocateStmt;
```

## Detailed Description
DeallocateStmt is a parse node structure that holds the information needed to deallocate (remove) a prepared statement in PostgreSQL. It can either deallocate a specific prepared statement by name, or deallocate all prepared statements in the current session using DEALLOCATE ALL.

The structure includes special attributes for query jumbling - the name field is ignored during jumbling for privacy, while the isall field and location are considered for jumbling purposes. This structure is created during the parsing phase and processed by the deallocate command execution system.

## Parameters / Member Variables
- `type`: Standard NodeTag for parse tree node identification
- `name`: String containing the name of the prepared statement to deallocate (NULL for DEALLOCATE ALL)
- `isall`: Boolean flag indicating whether this is a DEALLOCATE ALL statement (redundant with name == NULL but separate for query jumbling)
- `location`: ParseLoc indicating the token location in the source query (-1 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc (for source location tracking)
- Called from (representative examples):
  - DeallocateQuery (command execution function)
  - standard_ProcessUtility (utility command dispatcher)  
  - CreateCommandTag (for logging and command tagging)

## Notes and Other Information
- Part of the SQL prepared statement functionality in PostgreSQL
- Located in src/include/nodes/parsenodes.h along with other statement structures
- The actual deallocation logic is implemented in DeallocateQuery() in src/backend/commands/prepare.c
- DEALLOCATE ALL removes all prepared statements for the current session
- Uses special pg_node_attr annotations for query jumbling behavior
- Works in conjunction with PrepareStmt and ExecuteStmt to provide complete prepared statement lifecycle management