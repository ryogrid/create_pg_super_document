# CreateSchemaCommand

## Location
[src/backend/commands/schemacmds.c:52-248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/schemacmds.c#L52-L248)

## Overview
CreateSchemaCommand implements the CREATE SCHEMA SQL command, creating a new database schema and executing any embedded SQL statements within the schema creation context.

## Definition

```c
Oid
CreateSchemaCommand(CreateSchemaStmt *stmt, const char *queryString,
					int stmt_location, int stmt_len)
```
## Detailed Description
CreateSchemaCommand orchestrates the complete CREATE SCHEMA operation, handling authorization, namespace creation, and execution of embedded statements. The function performs comprehensive security checks, creates the schema namespace, temporarily modifies the search path to include the new schema, and processes any embedded SQL statements (like CREATE TABLE, CREATE VIEW) within the schema context.

Key behaviors include:
- Resolving the schema owner (either specified or defaulting to current user)
- Performing ACL checks for database CREATE privilege and role ownership
- Handling IF NOT EXISTS logic with extension membership validation
- Creating the actual namespace through NamespaceCreate
- Temporarily prepending the new schema to search_path during embedded statement execution
- Processing embedded statements in dependency-resolved order
- Proper cleanup of security context and GUC settings

## Parameters / Member Variables
- : CreateSchemaStmt containing schema name, owner role spec, IF NOT EXISTS flag, and embedded statement list
- : Original SQL query string for error reporting and logging
- : Character offset of the statement in the query string
- : Length of the statement in characters

## Dependencies
- Functions called/Symbols referenced:
  - [NamespaceCreate](../N/NamespaceCreate.md) (creates the actual namespace)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)/SetUserIdAndSecContext (security context management)
  - [get_rolespec_oid](../g/get_rolespec_oid.md) (resolves owner role specification)
  - [object_aclcheck](../o/object_aclcheck.md) (checks CREATE privilege on database)
  - [checkMembershipInCurrentExtension](../c/checkMembershipInCurrentExtension.md) (validates extension membership for IF NOT EXISTS)
  - [transformCreateSchemaStmtElements](../t/transformCreateSchemaStmtElements.md) (reorganizes embedded statements)
  - [ProcessUtility](../P/ProcessUtility.md) (executes embedded statements)
  - [EventTriggerCollectSimpleCommand](../E/EventTriggerCollectSimpleCommand.md) (reports schema creation to event triggers)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (main utility command processing)
  - [CreateExtensionInternal](CreateExtensionInternal.md) (schema creation during extension installation)

## Notes and Other Information
- Returns the OID of the created namespace, or InvalidOid if skipped due to IF NOT EXISTS
- Temporarily modifies search_path to include the new schema during embedded statement execution
- Uses function-level SET to ensure search_path changes persist only for the duration of schema creation
- Validates against reserved schema names (pg_* prefix) unless allowSystemTableMods is enabled
- Handles ownership transfer by temporarily switching user context during creation
- Location information is passed through to embedded statements for consistent error reporting

## Simplified Source

```c
Oid CreateSchemaCommand(CreateSchemaStmt *stmt, const char *queryString,
                       int stmt_location, int stmt_len)
{
    const char *schemaName = stmt->schemaname;
    Oid namespaceId;
    Oid owner_uid, saved_uid;
    int save_sec_context, save_nestlevel;

    // Get current security context
    GetUserIdAndSecContext(&saved_uid, &save_sec_context);

    // Determine schema owner (specified role or current user)
    if (stmt->authrole)
        owner_uid = get_rolespec_oid(stmt->authrole, false);
    else
        owner_uid = saved_uid;

    // Use owner's name as schema name if not specified
    if (!schemaName) {
        HeapTuple tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(owner_uid));
        schemaName = pstrdup(NameStr(((Form_pg_authid) GETSTRUCT(tuple))->rolname));
        ReleaseSysCache(tuple);
    }

    // Check permissions: CREATE privilege on database and role ownership
    if (object_aclcheck(DatabaseRelationId, MyDatabaseId, saved_uid, ACL_CREATE) != ACLCHECK_OK)
        aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_DATABASE, get_database_name(MyDatabaseId));

    check_can_set_role(saved_uid, owner_uid);

    // Prevent creation of reserved schema names (pg_*)
    if (!allowSystemTableMods && IsReservedName(schemaName))
        ereport(ERROR, (errcode(ERRCODE_RESERVED_NAME),
                       errmsg("unacceptable schema name \"%s\"", schemaName)));

    // Handle IF NOT EXISTS - skip if schema already exists
    if (stmt->if_not_exists) {
        namespaceId = get_namespace_oid(schemaName, true);
        if (OidIsValid(namespaceId)) {
            // Validate extension membership for security
            ObjectAddress address;
            ObjectAddressSet(address, NamespaceRelationId, namespaceId);
            checkMembershipInCurrentExtension(&address);
            return InvalidOid;  // Skip creation
        }
    }

    // Switch to target owner's security context if different
    if (saved_uid != owner_uid)
        SetUserIdAndSecContext(owner_uid, save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

    // Create the schema namespace
    namespaceId = NamespaceCreate(schemaName, owner_uid, false);
    CommandCounterIncrement();

    // Temporarily add new schema to search_path for embedded statements
    save_nestlevel = NewGUCNestLevel();
    StringInfoData pathbuf;
    initStringInfo(&pathbuf);
    appendStringInfoString(&pathbuf, quote_identifier(schemaName));
    appendStringInfo(&pathbuf, ", %s", namespace_search_path);
    set_config_option("search_path", pathbuf.data, PGC_USERSET, PGC_S_SESSION,
                     GUC_ACTION_SAVE, true, 0, false);

    // Report schema creation to event triggers
    ObjectAddress address;
    ObjectAddressSet(address, NamespaceRelationId, namespaceId);
    EventTriggerCollectSimpleCommand(address, InvalidObjectAddress, (Node *) stmt);

    // Process embedded statements (CREATE TABLE, etc.) in correct order
    List *parsetree_list = transformCreateSchemaStmtElements(stmt->schemaElts, schemaName);

    ListCell *parsetree_item;
    foreach(parsetree_item, parsetree_list) {
        Node *embedded_stmt = (Node *) lfirst(parsetree_item);

        // Wrap statement for ProcessUtility
        PlannedStmt *wrapper = makeNode(PlannedStmt);
        wrapper->commandType = CMD_UTILITY;
        wrapper->canSetTag = false;
        wrapper->utilityStmt = embedded_stmt;
        wrapper->stmt_location = stmt_location;
        wrapper->stmt_len = stmt_len;

        // Execute embedded statement
        ProcessUtility(wrapper, queryString, false, PROCESS_UTILITY_SUBCOMMAND,
                      NULL, NULL, None_Receiver, NULL);
        CommandCounterIncrement();
    }

    // Restore search_path and security context
    AtEOXact_GUC(true, save_nestlevel);
    SetUserIdAndSecContext(saved_uid, save_sec_context);

    return namespaceId;
}
```