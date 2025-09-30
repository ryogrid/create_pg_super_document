# ExecReindex

## Location
[src/backend/commands/indexcmds.c:2693-2787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L2693-L2787)

## Overview
ExecReindex is the primary entry point for manual REINDEX commands, serving as a preparation wrapper that parses options and delegates to appropriate subroutines based on the type of object being reindexed.

## Definition

```c
struct ReindexIndexCallbackState state;
```
## Detailed Description
ExecReindex processes REINDEX statements by parsing command options (verbose, concurrently, tablespace), validating permissions, and dispatching to the appropriate reindex function based on the target object type. It handles five types of reindex operations:
- REINDEX_OBJECT_INDEX: Single index reindexing via ReindexIndex
- REINDEX_OBJECT_TABLE: Table reindexing via ReindexTable  
- REINDEX_OBJECT_SCHEMA: Schema reindexing via ReindexMultipleTables
- REINDEX_OBJECT_SYSTEM: System catalog reindexing via ReindexMultipleTables
- REINDEX_OBJECT_DATABASE: Database reindexing via ReindexMultipleTables

The function enforces transaction block restrictions for concurrent operations and multi-object reindexing, and validates tablespace permissions when a target tablespace is specified.

## Parameters / Member Variables
- : ParseState for error reporting with location information
- : ReindexStmt containing the parsed REINDEX command details
- : Boolean indicating if this is a top-level command (affects transaction block checking)

## Dependencies
- Functions called/Symbols referenced:
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md) (prevents execution in transaction blocks for certain operations)
  - [defGetBoolean](../d/defGetBoolean.md), defGetString (option parsing functions)
  - [get_tablespace_oid](../g/get_tablespace_oid.md) (tablespace name to OID conversion)
  - [object_aclcheck](../o/object_aclcheck.md), aclcheck_error (permission checking)
  - [ReindexIndex](../R/ReindexIndex.md) (single index reindexing)
  - [ReindexTable](../R/ReindexTable.md) (table reindexing)
  - [ReindexMultipleTables](../R/ReindexMultipleTables.md) (schema/system/database reindexing)
  - Various REINDEXOPT_* and REINDEX_OBJECT_* constants
- Called from:
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1567)

## Notes and Other Information
- This is a public function declared in defrem.h
- Supports three main options: VERBOSE, CONCURRENTLY, and TABLESPACE
- REINDEX CONCURRENTLY cannot run inside transaction blocks due to its multi-transaction nature
- Schema, system catalog, and database reindexing operations also cannot run in transaction blocks
- When specifying a tablespace, the function validates CREATE permissions on the target tablespace
- The function builds a ReindexParams structure to pass configuration to the actual reindex implementations
- Error handling includes both syntax errors for invalid options and permission errors for tablespace access

## Simplified Source

```c
void
ExecReindex(ParseState *pstate, const ReindexStmt *stmt, bool isTopLevel)
{
    ReindexParams params = {0};
    bool concurrently = false;
    bool verbose = false;
    char *tablespacename = NULL;

    // Parse command options
    foreach(lc, stmt->params)
    {
        DefElem *opt = (DefElem *) lfirst(lc);

        if (strcmp(opt->defname, "verbose") == 0)
            verbose = defGetBoolean(opt);
        else if (strcmp(opt->defname, "concurrently") == 0)
            concurrently = defGetBoolean(opt);
        else if (strcmp(opt->defname, "tablespace") == 0)
            tablespacename = defGetString(opt);
        else
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("unrecognized REINDEX option \"%s\"", opt->defname)));
    }

    // Prevent concurrent reindex in transaction blocks
    if (concurrently)
        PreventInTransactionBlock(isTopLevel, "REINDEX CONCURRENTLY");

    // Set up parameters
    params.options = (verbose ? REINDEXOPT_VERBOSE : 0) |
                    (concurrently ? REINDEXOPT_CONCURRENTLY : 0);

    // Handle tablespace option and permissions
    if (tablespacename != NULL)
    {
        params.tablespaceOid = get_tablespace_oid(tablespacename, false);

        // Check permissions for non-default tablespace
        if (OidIsValid(params.tablespaceOid) &&
            params.tablespaceOid != MyDatabaseTableSpace)
        {
            AclResult aclresult = object_aclcheck(TableSpaceRelationId,
                                                 params.tablespaceOid,
                                                 GetUserId(), ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, OBJECT_TABLESPACE,
                              get_tablespace_name(params.tablespaceOid));
        }
    }
    else
        params.tablespaceOid = InvalidOid;

    // Dispatch to appropriate reindex function
    switch (stmt->kind)
    {
        case REINDEX_OBJECT_INDEX:
            ReindexIndex(stmt, &params, isTopLevel);
            break;
        case REINDEX_OBJECT_TABLE:
            ReindexTable(stmt, &params, isTopLevel);
            break;
        case REINDEX_OBJECT_SCHEMA:
        case REINDEX_OBJECT_SYSTEM:
        case REINDEX_OBJECT_DATABASE:
            // Prevent multi-object reindex in transaction blocks
            PreventInTransactionBlock(isTopLevel, "REINDEX [SCHEMA|SYSTEM|DATABASE]");
            ReindexMultipleTables(stmt, &params);
            break;
        default:
            elog(ERROR, "unrecognized object type: %d", (int) stmt->kind);
            break;
    }
}
```