# markTargetListOrigins

## Location
[src/backend/parser/parse_target.c:318-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L318-L342)

## Overview
Marks targetlist columns that are simple Vars with their source table OID and column number for frontend communication purposes.

## Definition
```c
void
markTargetListOrigins(ParseState *pstate, List *targetlist)
```

## Detailed Description
This function iterates through a target list and marks target entries that correspond to simple Var expressions with information about their source table and column. The marking process involves recording the originating table's OID and the column number from which the target entry derives. This metadata is primarily used when sending query results to the frontend, allowing clients to understand the provenance of result columns. The function delegates the actual marking work to markTargetListOrigin for each individual TargetEntry.

## Parameters / Member Variables
- `pstate`: ParseState structure containing parser state and context information  
- `targetlist`: List of TargetEntry nodes to be marked with origin information

## Dependencies
- Functions called/Symbols referenced:
  - [markTargetListOrigin](markTargetListOrigin.md)
  - lfirst (macro)
  - [TargetEntry](../T/TargetEntry.md)
  - [Var](../V/Var.md)
- Called from (representative examples):
  - [transformSelectStmt](../t/transformSelectStmt.md)
  - [transformReturningList](../t/transformReturningList.md)

## Notes and Other Information
- Only used for SELECT targetlists and RETURNING lists since origin information is only needed when sending results to the frontend
- The function casts tle->expr to Var*, indicating it expects simple variable references rather than complex expressions
- Origin marking helps clients understand which table and column each result column comes from
- Essential for tools and applications that need to track data provenance in query results
- Works in conjunction with markTargetListOrigin which performs the actual metadata assignment for individual entries

## Simplified Source

```c
void markTargetListOrigins(ParseState *pstate, List *targetlist)
{
    ListCell *l;

    // Mark each target entry with its source table and column info
    foreach(l, targetlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(l);
        markTargetListOrigin(pstate, tle, (Var *) tle->expr, 0);
    }
}
```