# PortalDefineQuery

## Location
[src/backend/utils/mmgr/portalmem.c:282-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L282-L309)

## Overview
Establishes a portal's query by storing the query definition, source text, command tag, statement list, and optional cached plan. This function initializes a newly created portal with its query information and transitions it to the PORTAL_DEFINED status.

## Definition
```c
void PortalDefineQuery(Portal portal,
                      const char *prepStmtName,
                      const char *sourceText,
                      CommandTag commandTag,
                      List *stmts,
                      CachedPlan *cplan)
```

## Detailed Description
PortalDefineQuery is a fundamental function in PostgreSQL's portal management system that establishes the query definition for a portal. It takes a newly created portal (with status PORTAL_NEW) and populates it with all the necessary information to define what query it represents, including the source text, parsed statements, and optional cached plan.

The function performs minimal processing to avoid risking elog(ERROR) before properly storing the cached plan reference, which is crucial for reference counting. It simply stores the provided values and transitions the portal status to PORTAL_DEFINED.

Key design considerations:
- The sourceText parameter is mandatory (cannot be NULL) as of PostgreSQL 8.4
- If a cached plan is provided, the caller must have incremented its reference count
- The function ensures proper lifecycle management of the cached plan reference
- Command tag validation ensures consistency with the statement list

## Parameters / Member Variables
- `portal`: Target portal that must be valid and in PORTAL_NEW status
- `prepStmtName`: Name of prepared statement (can be NULL for unnamed statements)
- `sourceText`: Original SQL query text (mandatory, cannot be NULL)
- `commandTag`: Command type identifier (NULL only if original query was empty)
- `stmts`: List of parsed statement trees
- `cplan`: Optional cached plan with incremented reference count

## Dependencies
- Functions called/Symbols referenced:
  - PortalIsValid
  - [Portal](Portal.md) (type)
  - CommandTag (type)
  - CachedPlan (type)
  - PORTAL_NEW (constant)
  - PORTAL_DEFINED (constant)
- Called from (representative examples):
  - [PerformCursorOpen](PerformCursorOpen.md)
  - [ExecuteQuery](../E/ExecuteQuery.md)
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md)
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_bind_message](../e/exec_bind_message.md)

## Notes and Other Information
- Introduced mandatory sourceText requirement in PostgreSQL 8.4 for better debugging and logging
- Critical for proper cached plan reference counting - the function must not fail after accepting a cached plan reference
- The portal transitions from PORTAL_NEW to PORTAL_DEFINED status after successful completion
- [Command](../C/Command.md) tag must be a pointer to a constant string as it is not copied
- Caller is responsible for ensuring adequate lifetime of prepStmtName and sourceText parameters
- Used in both simple query execution and prepared statement execution paths