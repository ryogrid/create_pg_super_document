# pg_event_trigger_ddl_commands

## Location
[src/backend/commands/event_trigger.c:1925-2120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1925-L2120)

## Overview
A PostgreSQL system function that returns detailed information about DDL commands being executed, accessible only within event trigger functions during ddl_command_end events.

## Definition
```c
Datum pg_event_trigger_ddl_commands(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a way for event triggers to examine the DDL commands that have been collected during the current event trigger context. It returns a set of records containing detailed metadata about each command, including object identifiers, command types, schema information, and object identities.

The function operates as a set-returning function (SRF) that iterates through the collected command list in the current event trigger state. For different types of commands (SCT_Simple, SCT_AlterTable, SCT_AlterOpFamily, etc.), it extracts and formats appropriate information including object addresses, command names, object types, and schema details.

The function includes special handling for different command types:
- Standard object commands (SCT_Simple, SCT_AlterTable, etc.): Returns full object identity information
- ALTER DEFAULT PRIVILEGES commands: Uses stringify_adefprivs_objtype for object type formatting
- GRANT/REVOKE commands: Uses stringify_grant_objtype for object type formatting

## Parameters / Member Variables
- Returns a set of records with the following columns:
  - `classid`: OID of the system catalog containing the object
  - `objid`: OID of the object within its catalog
  - `objsubid`: Sub-object identifier (for columns, etc.)
  - `command_tag`: The SQL command name (e.g., 'CREATE TABLE')
  - `object_type`: Human-readable object type description
  - `schema_name`: Schema containing the object (NULL for schema-less objects)
  - `identity`: Full identity string of the object
  - `in_extension`: Boolean indicating if command was part of extension creation
  - `command`: Pointer to the internal command structure

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md)
  - [getObjectIdentity](../g/getObjectIdentity.md)
  - [getObjectTypeDescription](../g/getObjectTypeDescription.md)
  - [is_objectclass_supported](../i/is_objectclass_supported.md)
  - [get_object_attnum_namespace](../g/get_object_attnum_namespace.md)
  - [get_catalog_object_by_oid](../g/get_catalog_object_by_oid.md)
  - [get_object_attnum_oid](../g/get_object_attnum_oid.md)
  - [heap_getattr](../h/heap_getattr.md)
  - [get_namespace_name_or_temp](../g/get_namespace_name_or_temp.md)
  - [CreateCommandName](../C/CreateCommandName.md)
  - [stringify_adefprivs_objtype](../s/stringify_adefprivs_objtype.md)
  - [stringify_grant_objtype](../s/stringify_grant_objtype.md)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md)
- Called from: 
  - Available as SQL function pg_event_trigger_ddl_commands()

## Notes and Other Information
- Can only be called within event trigger functions, otherwise raises an error
- Skips commands with invalid object IDs (e.g., IF NOT EXISTS commands for existing objects)
- Handles cases where objects may have been dropped in the same command by checking for valid object identity
- Uses different formatting strategies for different command types (ALTER DEFAULT PRIVILEGES, GRANT/REVOKE vs. regular object commands)
- Part of PostgreSQL's event trigger introspection system, allowing event triggers to examine and act upon DDL command metadata
- Returns comprehensive information that enables event triggers to implement complex DDL auditing, replication, or validation logic

## Simplified Source

```c
// Simplified version of pg_event_trigger_ddl_commands
Datum pg_event_trigger_ddl_commands(PG_FUNCTION_ARGS) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    ListCell *lc;

    // Validate context: must be called within event trigger
    if (!currentEventTriggerState)
        ereport(ERROR, (errcode(ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED),
                errmsg("pg_event_trigger_ddl_commands() can only be called in an event trigger function")));

    // Initialize result tuplestore
    InitMaterializedSRF(fcinfo, 0);

    // Iterate through collected DDL commands
    foreach(lc, currentEventTriggerState->commandList) {
        CollectedCommand *cmd = lfirst(lc);
        Datum values[9];
        bool nulls[9] = {0};
        ObjectAddress addr;
        int i = 0;

        // Skip invalid commands (IF NOT EXISTS with existing objects)
        if (cmd->type == SCT_Simple && !OidIsValid(cmd->d.simple.address.objectId))
            continue;

        switch (cmd->type) {
            case SCT_Simple:
            case SCT_AlterTable:
            case SCT_AlterOpFamily:
            case SCT_CreateOpClass:
            case SCT_AlterTSConfig:
                {
                    // Extract object address based on command type
                    if (cmd->type == SCT_Simple)
                        addr = cmd->d.simple.address;
                    else if (cmd->type == SCT_AlterTable)
                        ObjectAddressSet(addr, cmd->d.alterTable.classId, cmd->d.alterTable.objectId);
                    else if (cmd->type == SCT_AlterOpFamily)
                        addr = cmd->d.opfam.address;
                    else if (cmd->type == SCT_CreateOpClass)
                        addr = cmd->d.createopc.address;
                    else if (cmd->type == SCT_AlterTSConfig)
                        addr = cmd->d.atscfg.address;

                    // Get object identity and type
                    char *identity = getObjectIdentity(&addr, true);
                    if (identity == NULL)
                        continue;  // Object was dropped, skip

                    char *type = getObjectTypeDescription(&addr, true);
                    char *schema = NULL;

                    // Get schema name if object class supports it
                    if (is_objectclass_supported(addr.classId)) {
                        AttrNumber nspAttnum = get_object_attnum_namespace(addr.classId);
                        if (nspAttnum != InvalidAttrNumber) {
                            // Look up schema from catalog
                            Relation catalog = table_open(addr.classId, AccessShareLock);
                            HeapTuple objtup = get_catalog_object_by_oid(catalog,
                                                get_object_attnum_oid(addr.classId), addr.objectId);
                            if (HeapTupleIsValid(objtup)) {
                                bool isnull;
                                Oid schema_oid = heap_getattr(objtup, nspAttnum,
                                                            RelationGetDescr(catalog), &isnull);
                                if (!isnull)
                                    schema = get_namespace_name_or_temp(schema_oid);
                            }
                            table_close(catalog, AccessShareLock);
                        }
                    }

                    // Populate result row
                    values[i++] = ObjectIdGetDatum(addr.classId);
                    values[i++] = ObjectIdGetDatum(addr.objectId);
                    values[i++] = Int32GetDatum(addr.objectSubId);
                    values[i++] = CStringGetTextDatum(CreateCommandName(cmd->parsetree));
                    values[i++] = CStringGetTextDatum(type);
                    if (schema)
                        values[i++] = CStringGetTextDatum(schema);
                    else
                        nulls[i++] = true;
                    values[i++] = CStringGetTextDatum(identity);
                    values[i++] = BoolGetDatum(cmd->in_extension);
                    values[i++] = PointerGetDatum(cmd);
                }
                break;

            case SCT_AlterDefaultPrivileges:
                // Handle ALTER DEFAULT PRIVILEGES commands (no specific object)
                nulls[i++] = true;  // classid
                nulls[i++] = true;  // objid
                nulls[i++] = true;  // objsubid
                values[i++] = CStringGetTextDatum(CreateCommandName(cmd->parsetree));
                values[i++] = CStringGetTextDatum(stringify_adefprivs_objtype(cmd->d.defprivs.objtype));
                nulls[i++] = true;  // schema
                nulls[i++] = true;  // identity
                values[i++] = BoolGetDatum(cmd->in_extension);
                values[i++] = PointerGetDatum(cmd);
                break;

            case SCT_Grant:
                // Handle GRANT/REVOKE commands
                nulls[i++] = true;  // classid
                nulls[i++] = true;  // objid
                nulls[i++] = true;  // objsubid
                values[i++] = CStringGetTextDatum(cmd->d.grant.istmt->is_grant ? "GRANT" : "REVOKE");
                values[i++] = CStringGetTextDatum(stringify_grant_objtype(cmd->d.grant.istmt->objtype));
                nulls[i++] = true;  // schema
                nulls[i++] = true;  // identity
                values[i++] = BoolGetDatum(cmd->in_extension);
                values[i++] = PointerGetDatum(cmd);
                break;
        }

        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }

    PG_RETURN_VOID();
}
```

Key simplifications made:
- Removed excessive comments and kept essential ones
- Consolidated variable declarations
- Simplified schema lookup logic flow
- Clearly separated different command type handling
- Reduced nested conditional complexity
- Focused on core functionality while preserving all essential logic