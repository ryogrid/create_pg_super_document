# DoCopy

## Location
[src/backend/commands/copy.c:62-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copy.c#L62-L328)

## Overview
DoCopy executes the SQL COPY statement, handling both copying data from files/programs/stdin into tables (COPY FROM) and copying table data or query results to files/programs/stdout (COPY TO).

## Definition

```c
void
DoCopy(ParseState *pstate, const CopyStmt *stmt,
	   int stmt_location, int stmt_len,
	   uint64 *processed)
```
## Detailed Description
DoCopy is the main entry point for executing COPY statements in PostgreSQL. It performs comprehensive permission checking, handles both table-based and query-based COPY operations, and manages row-level security (RLS) requirements. For COPY FROM operations, it transfers data from external sources into database tables. For COPY TO operations, it exports table data or query results to external destinations. The function handles various security restrictions including role-based permissions for file and program access, and automatically converts table-based COPY TO operations to query-based operations when row-level security is enabled.

## Parameters / Member Variables
- : ParseState containing query parsing context and namespace information
- : CopyStmt structure containing the parsed COPY statement details including source/destination, options, and column lists  
- : Character position where the COPY statement starts in the original query string
- : Length of the COPY statement in characters
- : Output parameter returning the number of rows processed during the COPY operation

## Dependencies
- Functions called/Symbols referenced:
  - [has_privs_of_role](../h/has_privs_of_role.md)
  - [table_openrv](../t/table_openrv.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [transformExpr](../t/transformExpr.md)
  - [CopyGetAttnums](../C/CopyGetAttnums.md)
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md)
  - [check_enable_rls](../c/check_enable_rls.md)
  - [BeginCopyFrom](../B/BeginCopyFrom.md)/CopyFrom/EndCopyFrom
  - [BeginCopyTo](../B/BeginCopyTo.md)/DoCopyTo/EndCopyTo
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Enforces strict permission checking for file and program access through role-based security
- Automatically handles row-level security by converting table COPY TO operations to SELECT-based operations
- Supports WHERE clauses for filtering data during COPY operations
- Manages proper locking (RowExclusiveLock for COPY FROM, AccessShareLock for COPY TO)
- Prevents COPY FROM operations when row-level security is enabled, requiring INSERT statements instead
- Handles both pipe-based operations (stdin/stdout) and file-based operations with appropriate security checks

## Simplified Source

```c
void DoCopy(ParseState *pstate, const CopyStmt *stmt,
           int stmt_location, int stmt_len, uint64 *processed) {
    bool is_from = stmt->is_from;
    bool pipe = (stmt->filename == NULL);
    Relation rel;
    Oid relid;
    RawStmt *query = NULL;
    Node *whereClause = NULL;

    // Security checks for file/program access
    if (!pipe) {
        if (stmt->is_program) {
            if (!has_privs_of_role(GetUserId(), ROLE_PG_EXECUTE_SERVER_PROGRAM))
                ereport(ERROR, "permission denied to COPY to/from external program");
        } else {
            if (is_from && !has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES))
                ereport(ERROR, "permission denied to COPY from file");
            if (!is_from && !has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES))
                ereport(ERROR, "permission denied to COPY to file");
        }
    }

    if (stmt->relation) {
        // Table-based COPY
        LOCKMODE lockmode = is_from ? RowExclusiveLock : AccessShareLock;

        // Open and lock the relation
        rel = table_openrv(stmt->relation, lockmode);
        relid = RelationGetRelid(rel);

        // Set up parser namespace and permissions
        ParseNamespaceItem *nsitem = addRangeTableEntryForRelation(pstate, rel,
                                      lockmode, NULL, false, false);
        nsitem->p_perminfo->requiredPerms = (is_from ? ACL_INSERT : ACL_SELECT);

        // Handle WHERE clause if present
        if (stmt->whereClause) {
            addNSItemToQuery(pstate, nsitem, false, true, true);
            whereClause = transformExpr(pstate, stmt->whereClause, EXPR_KIND_COPY_WHERE);
            whereClause = coerce_to_boolean(pstate, whereClause, "WHERE");
            // ... additional expression processing
        }

        // Process column list and check permissions
        List *attnums = CopyGetAttnums(RelationGetDescr(rel), rel, stmt->attlist);
        // ... set up column permissions
        ExecCheckPermissions(pstate->p_rtable, list_make1(nsitem->p_perminfo), true);

        // Handle row-level security
        if (check_enable_rls(relid, InvalidOid, false) == RLS_ENABLED) {
            if (is_from)
                ereport(ERROR, "COPY FROM not supported with row-level security");

            // Convert to SELECT query for RLS compliance
            // ... build SELECT statement
            query = makeNode(RawStmt);
            // ... set up query structure

            table_close(rel, NoLock);
            rel = NULL;
        }
    } else {
        // Query-based COPY
        Assert(stmt->query);
        query = makeNode(RawStmt);
        query->stmt = stmt->query;
        query->stmt_location = stmt_location;
        query->stmt_len = stmt_len;
        relid = InvalidOid;
        rel = NULL;
    }

    if (is_from) {
        // COPY FROM: Import data
        Assert(rel);

        if (XactReadOnly && !rel->rd_islocaltemp)
            PreventCommandIfReadOnly("COPY FROM");

        CopyFromState cstate = BeginCopyFrom(pstate, rel, whereClause,
                                           stmt->filename, stmt->is_program,
                                           NULL, stmt->attlist, stmt->options);
        *processed = CopyFrom(cstate);
        EndCopyFrom(cstate);
    } else {
        // COPY TO: Export data
        CopyToState cstate = BeginCopyTo(pstate, rel, query, relid,
                                       stmt->filename, stmt->is_program,
                                       NULL, stmt->attlist, stmt->options);
        *processed = DoCopyTo(cstate);
        EndCopyTo(cstate);
    }

    if (rel != NULL)
        table_close(rel, NoLock);
}
```