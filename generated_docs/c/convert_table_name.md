# convert_table_name

## Location
src/backend/utils/adt/acl.c: 2049 - 2063

## Overview
Converts a table name expressed as a text string to its corresponding object identifier (OID) by performing name resolution and lookup.

## Definition
```c
static Oid convert_table_name(text *tablename)
```

## Detailed Description
This static helper function serves as a utility for the has_table_privilege family of functions. It takes a table name provided as a PostgreSQL text type and converts it to the corresponding relation OID. The function handles both simple table names and qualified names (schema.table format) by parsing the input text into a qualified name list and creating a RangeVar structure for relation lookup.

The function performs name resolution without acquiring locks on the target relation, as it might be used in contexts where the caller doesn't have permissions on the relation. This approach ensures that privilege checking functions can operate even when the user lacks access to the table being queried.

## Parameters / Member Variables
-  (text*): A PostgreSQL text value containing the table name, which can be either a simple name or a schema-qualified name (e.g., "mytable" or "myschema.mytable")

## Dependencies
- Functions called/Symbols referenced:
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md): Creates a RangeVar structure from a qualified name list
  - textToQualifiedNameList: Parses text input into a qualified name list
  - RangeVarGetRelid: Resolves RangeVar to relation OID without locking
  - [RangeVar](../R/RangeVar.md): Structure type representing a relation reference
- Called from (representative examples):
  - [has_table_privilege_name_name](../h/has_table_privilege_name_name.md): Table privilege check with role name and table name
  - [has_table_privilege_name](../h/has_table_privilege_name.md): Table privilege check with current user and table name
  - [has_table_privilege_id_name](../h/has_table_privilege_id_name.md): Table privilege check with role ID and table name
  - [has_sequence_privilege_name_name](../h/has_sequence_privilege_name_name.md): Sequence privilege check functions
  - has_column_privilege functions: Column-level privilege checking functions

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (acl.c)
- The function deliberately avoids locking the relation since it might be called for relations the user doesn't have permission to access
- Part of PostgreSQL's privilege inquiry system infrastructure
- Handles schema-qualified names by parsing them into namespace and relation components
- Used extensively throughout the privilege checking system for name-to-OID conversion
- Located in src/backend/utils/adt/acl.c:2049-2063