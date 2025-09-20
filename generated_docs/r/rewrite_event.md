# rewrite_event

## Location
[src/backend/rewrite/rewriteHandler.c:50-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L50-L54)

## Overview
A structure used to detect recursion during query rewriting by tracking relations and their associated rule events.

## Definition

```c
typedef struct rewrite_event
{
	Oid			relation;		/* OID of relation having rules */
	CmdType		event;			/* type of rule being fired */
} rewrite_event;
```
## Detailed Description
The  structure is a simple data structure designed to prevent infinite recursion during the query rewriting process in PostgreSQL's rule system. It maintains a record of which relations are currently being processed and what type of command triggered the rule, allowing the rewriter to detect when it encounters the same relation-event combination again and avoid endless loops.

## Parameters / Member Variables
- `relation`: The Object ID (OID) of the relation (table/view) that has rules being processed
- `event`: The type of command (INSERT, UPDATE, DELETE, SELECT) that triggered the rule firing
## Dependencies
- Functions called/Symbols referenced:
  - CmdType (enumeration for command types)
- Called from (representative examples):
  - [RewriteQuery](../R/RewriteQuery.md) (used in recursion detection logic)

## Notes and Other Information
- This structure is used as part of a list to maintain the call stack during rule rewriting
- Essential for preventing infinite recursion when rules reference each other
- Location: src/backend/rewrite/rewriteHandler.c:50-54