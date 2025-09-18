# isLockedRefname

## Location
[src/backend/parser/parse_relation.c:2575-2618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L2575-L2618)

## Overview
Determines whether a specified table reference name has been selected for locking with FOR UPDATE or FOR SHARE clauses before the locking clause transformation has been performed.

## Definition
```c
bool isLockedRefname(ParseState *pstate, const char *refname)
```

## Detailed Description
This function checks if a given table reference name is subject to row-level locking based on FOR UPDATE or FOR SHARE clauses in the current query context. It is used during the initial opening of relations to determine the correct lock level before the full locking clause transformation is complete. The function handles three scenarios: inheritance of locking from parent queries, global locking clauses that affect all tables, and specific table name matches in locking clauses. It treats FOR UPDATE and FOR SHARE identically since they require the same table-level lock. The function can handle NULL refnames (for unnamed subqueries) which can only be locked through global locking clauses.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and locking information
- `refname`: Name of the table reference to check for locking (can be NULL for unnamed subqueries)

## Dependencies
- Functions called/Symbols referenced:
  - lfirst (for list cell access)
  - strcmp (for string comparison)
- Called from (representative examples):
  - [transformRangeSubselect](../t/transformRangeSubselect.md) (in parse_clause.c:433)
  - [addRangeTableEntry](../a/addRangeTableEntry.md) (in parse_relation.c:1494)

## Notes and Other Information
- Returns true if the parent query has marked this subquery as locked (p_locked_from_parent)
- Returns true for global locking clauses where lockedRels is NIL (affecting all tables)
- Returns true if refname matches any table name in specific locking clauses
- Returns false for NULL refnames when no global locking clause exists
- Does not distinguish between FOR UPDATE and FOR SHARE since table-level locking requirements are identical
- Used during relation opening to determine appropriate lock levels before full transformation
- Part of the early locking analysis phase of query processing
- Located in src/backend/parser/parse_relation.c:2575-2618