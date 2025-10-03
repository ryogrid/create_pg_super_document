# ImportForeignSchema

## Location
[src/backend/commands/foreigncmds.c:1495-1610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L1495-L1610)

## Overview
ImportForeignSchema implements the SQL IMPORT FOREIGN SCHEMA command, which allows users to import table definitions from a foreign database into the local PostgreSQL instance as foreign tables.

## Definition

```c
void
ImportForeignSchema(ImportForeignSchemaStmt *stmt)
```
## Detailed Description
This function is the main entry point for executing IMPORT FOREIGN SCHEMA statements. It validates permissions, retrieves foreign data wrapper (FDW) information, calls the FDW's ImportForeignSchema routine to generate CREATE FOREIGN TABLE commands, and then executes those commands to create the foreign tables in the local schema.

The function performs several key operations:
1. Validates that the foreign server exists and the user has USAGE permissions
2. Checks that the target local schema exists and the user has CREATE permissions
3. Retrieves the FDW and verifies it supports schema import functionality
4. Calls the FDW's ImportForeignSchema routine to generate SQL commands
5. Parses and executes each generated CREATE FOREIGN TABLE statement
6. Applies filtering based on the IMPORT statement's LIMIT TO or EXCEPT clauses

## Parameters / Member Variables
- `*stmt`: ImportForeignSchemaStmt structure containing the parsed IMPORT FOREIGN SCHEMA statement, including server name, remote schema, local schema, and filtering options
## Dependencies
- Functions called/Symbols referenced:
  - [GetForeignServerByName](../G/GetForeignServerByName.md): Retrieves foreign server definition
  - [object_aclcheck](../o/object_aclcheck.md): Checks ACL permissions on the foreign server
  - [aclcheck_error](../a/aclcheck_error.md): Reports ACL permission errors
  - [LookupCreationNamespace](../L/LookupCreationNamespace.md): Validates and retrieves the target schema
  - [GetForeignDataWrapper](../G/GetForeignDataWrapper.md): Retrieves FDW definition
  - [GetFdwRoutine](../G/GetFdwRoutine.md): Gets the FDW's function pointers
  - [import_error_callback](../i/import_error_callback.md): Error context callback for better error reporting
  - [pg_parse_query](../p/pg_parse_query.md): Parses SQL commands returned by the FDW
  - [IsImportableForeignTable](IsImportableForeignTable.md): Filters tables based on LIMIT TO/EXCEPT clauses
  - [ProcessUtility](../P/ProcessUtility.md): Executes the CREATE FOREIGN TABLE statements
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md): Ensures visibility of created tables between commands
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command dispatcher

## Notes and Other Information
- The function sets up error context callbacks to provide better error messages when parsing or executing FDW-generated SQL fails
- Only CREATE FOREIGN TABLE statements are allowed from the FDW's ImportForeignSchema routine
- The function supports multiple CREATE FOREIGN TABLE commands per string returned by the FDW
- Table filtering is applied using IsImportableForeignTable based on the statement's options
- All created foreign tables are placed in the schema specified by the local_schema parameter
- [Command](../C/Command.md) counter incrementation ensures that each newly created table is visible to subsequent commands in the same transaction

## Simplified Source

```c
void
ImportForeignSchema(ImportForeignSchemaStmt *stmt)
{
    ForeignServer *server;
    ForeignDataWrapper *fdw;
    FdwRoutine *fdw_routine;
    AclResult aclresult;
    List *cmd_list;

    // Validate foreign server exists and check USAGE permission
    server = GetForeignServerByName(stmt->server_name, false);
    aclresult = object_aclcheck(ForeignServerRelationId, server->serverid,
                               GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER, server->servername);

    // Validate target schema exists and check CREATE permission
    (void) LookupCreationNamespace(stmt->local_schema);

    // Get FDW and verify it supports schema import
    fdw = GetForeignDataWrapper(server->fdwid);
    if (!OidIsValid(fdw->fdwhandler))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("foreign-data wrapper \"%s\" has no handler",
                              fdw->fdwname)));

    fdw_routine = GetFdwRoutine(fdw->fdwhandler);
    if (fdw_routine->ImportForeignSchema == NULL)
        ereport(ERROR, (errcode(ERRCODE_FDW_NO_SCHEMAS),
                       errmsg("foreign-data wrapper \"%s\" does not support IMPORT FOREIGN SCHEMA",
                              fdw->fdwname)));

    // Get list of CREATE FOREIGN TABLE commands from FDW
    cmd_list = fdw_routine->ImportForeignSchema(stmt, server->serverid);

    // Parse and execute each command
    foreach(lc, cmd_list)
    {
        char *cmd = (char *) lfirst(lc);
        import_error_callback_arg callback_arg;
        ErrorContextCallback sqlerrcontext;

        // Setup error callback for better error reporting
        callback_arg.tablename = NULL;
        callback_arg.cmd = cmd;
        sqlerrcontext.callback = import_error_callback;
        sqlerrcontext.arg = (void *) &callback_arg;
        sqlerrcontext.previous = error_context_stack;
        error_context_stack = &sqlerrcontext;

        // Parse SQL commands from FDW
        List *raw_parsetree_list = pg_parse_query(cmd);

        // Process each CREATE FOREIGN TABLE statement
        foreach(lc2, raw_parsetree_list)
        {
            RawStmt *rs = lfirst_node(RawStmt, lc2);
            CreateForeignTableStmt *cstmt = (CreateForeignTableStmt *) rs->stmt;

            // Validate statement type
            if (!IsA(cstmt, CreateForeignTableStmt))
                elog(ERROR, "foreign-data wrapper \"%s\" returned incorrect statement type %d",
                     fdw->fdwname, (int) nodeTag(cstmt));

            // Apply filtering based on LIMIT TO/EXCEPT clauses
            if (!IsImportableForeignTable(cstmt->base.relation->relname, stmt))
                continue;

            // Set schema and execute
            callback_arg.tablename = cstmt->base.relation->relname;
            cstmt->base.relation->schemaname = pstrdup(stmt->local_schema);

            // Create planned statement and execute
            PlannedStmt *pstmt = makeNode(PlannedStmt);
            pstmt->commandType = CMD_UTILITY;
            pstmt->canSetTag = false;
            pstmt->utilityStmt = (Node *) cstmt;
            pstmt->stmt_location = rs->stmt_location;
            pstmt->stmt_len = rs->stmt_len;

            ProcessUtility(pstmt, cmd, false, PROCESS_UTILITY_SUBCOMMAND,
                          NULL, NULL, None_Receiver, NULL);

            CommandCounterIncrement();
            callback_arg.tablename = NULL;
        }

        error_context_stack = sqlerrcontext.previous;
    }
}
```