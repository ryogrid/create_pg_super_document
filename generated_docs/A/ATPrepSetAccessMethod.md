# ATPrepSetAccessMethod

## Location
src/backend/commands/tablecmds.c: 14895 - 14928

## Overview
This is the preparation phase function for the ALTER TABLE SET ACCESS METHOD command that validates the access method and determines if a change is needed.

## Definition
```c
static void ATPrepSetAccessMethod(AlteredTableInfo *tab, Relation rel, const char *amname)
```

## Detailed Description
This function handles the preparation phase of changing a table's access method through ALTER TABLE SET ACCESS METHOD. It validates that the specified access method exists and is different from the current one. The function supports setting a specific access method by name, using DEFAULT (which resolves to the system's default_table_access_method), or for partitioned tables, setting to InvalidOid to reset the catalogued access method.

The function first resolves the access method name to its OID using get_table_am_oid. For partitioned tables when DEFAULT is specified (amname is NULL), it sets the OID to InvalidOid. For regular tables with NULL amname, it uses the system default. If the resolved OID matches the table's current access method, no change is needed and the function returns early. Otherwise, it marks the table for rewriting and saves the new access method information in the AlteredTableInfo structure for the execution phase.

## Parameters / Member Variables
- `tab`: AlteredTableInfo structure that tracks changes being made to the table during ALTER TABLE processing
- `rel`: The relation (table) whose access method is being changed
- `amname`: The name of the new access method, or NULL for DEFAULT

## Dependencies
- Functions called/Symbols referenced:
  - [get_table_am_oid](../g/get_table_am_oid.md)
  - default_table_access_method (global variable)
  - AT_REWRITE_ACCESS_METHOD
- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md)

## Notes and Other Information
- This is a static function only accessible within tablecmds.c as part of the ALTER TABLE infrastructure
- Part of PostgreSQL's three-phase ALTER TABLE processing (preparation, validation, execution)
- Sets the rewrite flag when access method change is needed, indicating the table will need to be rewritten
- Handles special case for partitioned tables where DEFAULT means clearing the access method
- The actual access method change happens in the execution phase, not during preparation
- Located in src/backend/commands/tablecmds.c:14895-14928