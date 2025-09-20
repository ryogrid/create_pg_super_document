# fireRIRonSubLink_context

## Location
[src/backend/rewrite/rewriteHandler.c:61-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L61-L65)

## Overview
A context structure used during the firing of Row-level Insert/Update/Delete (RIR) rules on sublinks, maintaining state about active rules and row security settings.

## Definition

```c
typedef struct fireRIRonSubLink_context
{
	List	   *activeRIRs;
	bool		hasRowSecurity;
} fireRIRonSubLink_context;
```
## Detailed Description
The  structure provides essential context information when processing RIR (Row-level Insert/Update/Delete) rules on sublinks within query rewriting. It tracks the list of currently active RIR rules to prevent infinite recursion and maintains information about whether row-level security policies are involved in the current rewrite operation. This context is crucial for ensuring proper rule processing while avoiding infinite loops and correctly handling security constraints.

## Parameters / Member Variables
- `*activeRIRs`: A list of currently active RIR rules, used to detect and prevent recursive rule firing
- `hasRowSecurity`: Boolean flag indicating whether row-level security policies are active in the current context
## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (PostgreSQL's list data structure)
- Called from (representative examples):
  - [fireRIRonSubLink](fireRIRonSubLink.md)
  - [fireRIRrules](fireRIRrules.md)

## Notes and Other Information
- Essential for preventing infinite recursion when RIR rules reference each other
- Integrates with PostgreSQL's row-level security (RLS) feature
- Part of the query rewriting infrastructure for handling complex rule scenarios
- Used specifically in sublink processing during rule firing
- Location: src/backend/rewrite/rewriteHandler.c:61-65