# pg_event_trigger_ddl_commands

## Location
src/backend/commands/event_trigger.c: 1925 - 2120

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
  - getObjectTypeDescription
  - [is_objectclass_supported](../i/is_objectclass_supported.md)
  - [get_object_attnum_namespace](../g/get_object_attnum_namespace.md)
  - [get_catalog_object_by_oid](../g/get_catalog_object_by_oid.md)
  - [get_object_attnum_oid](../g/get_object_attnum_oid.md)
  - [heap_getattr](../h/heap_getattr.md)
  - [get_namespace_name_or_temp](../g/get_namespace_name_or_temp.md)
  - CreateCommandName
  - [stringify_adefprivs_objtype](../s/stringify_adefprivs_objtype.md)
  - [stringify_grant_objtype](../s/stringify_grant_objtype.md)
  - tuplestore_putvalues
- Called from: 
  - Available as SQL function pg_event_trigger_ddl_commands()

## Notes and Other Information
- Can only be called within event trigger functions, otherwise raises an error
- Skips commands with invalid object IDs (e.g., IF NOT EXISTS commands for existing objects)
- Handles cases where objects may have been dropped in the same command by checking for valid object identity
- Uses different formatting strategies for different command types (ALTER DEFAULT PRIVILEGES, GRANT/REVOKE vs. regular object commands)
- Part of PostgreSQL's event trigger introspection system, allowing event triggers to examine and act upon DDL command metadata
- Returns comprehensive information that enables event triggers to implement complex DDL auditing, replication, or validation logic