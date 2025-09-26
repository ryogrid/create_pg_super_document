# ImportForeignSchemaStmt

## Location
[src/include/nodes/parsenodes.h:2944-2953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2944-L2953)

## Overview
ImportForeignSchemaStmt is a parse node structure that represents an IMPORT FOREIGN SCHEMA SQL statement, which imports table definitions from a remote schema through a foreign data wrapper into a local PostgreSQL schema.

## Definition
```c
typedef struct ImportForeignSchemaStmt
{
    NodeTag                     type;
    char                       *server_name;     /* FDW server name */
    char                       *remote_schema;   /* remote schema name to query */
    char                       *local_schema;    /* local schema to create objects in */
    ImportForeignSchemaType     list_type;       /* type of table list */
    List                       *table_list;      /* List of RangeVar */
    List                       *options;         /* list of options to pass to FDW */
} ImportForeignSchemaStmt;
```

## Detailed Description
ImportForeignSchemaStmt is a parse tree node that stores the parsed representation of an IMPORT FOREIGN SCHEMA statement. This structure contains all the information needed to import foreign table definitions from a remote data source into a local PostgreSQL schema. The statement allows selective importing of tables through various filtering options and passes through foreign data wrapper-specific options to control the import process.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an ImportForeignSchemaStmt node
- `server_name`: String containing the name of the foreign server to import from
- `remote_schema`: String containing the name of the remote schema to import tables from
- `local_schema`: String containing the name of the local schema where foreign tables will be created
- `list_type`: ImportForeignSchemaType enum specifying how to interpret the table_list (ALL, LIMIT TO, or EXCEPT)
- `table_list`: List of RangeVar nodes specifying which tables to include or exclude from the import
- `options`: List of generic options (DefElem nodes) to pass to the foreign data wrapper

## Dependencies
- Functions called/Symbols referenced:
  - [ImportForeignSchemaType](ImportForeignSchemaType.md)
  - NodeTag
  - [List](../L/List.md)
  - [RangeVar](../R/RangeVar.md) (referenced indirectly through table_list)
- Called from (representative examples):
  - [ImportForeignSchema](ImportForeignSchema.md) (src/backend/commands/foreigncmds.c:1495)
  - [IsImportableForeignTable](IsImportableForeignTable.md) (src/backend/foreign/foreign.c:483)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1617)

## Notes and Other Information
- This structure is part of the PostgreSQL parser node hierarchy and inherits from the Node structure via NodeTag
- The ImportForeignSchemaType enum has values: FDW_IMPORT_SCHEMA_ALL, FDW_IMPORT_SCHEMA_LIMIT_TO, and FDW_IMPORT_SCHEMA_EXCEPT
- The statement creates foreign table definitions in the local schema that reference tables in the remote schema
- Foreign data wrappers must implement the ImportForeignSchema API to support this functionality
- The options list allows passing FDW-specific parameters to control import behavior
- This is defined in src/include/nodes/parsenodes.h:2944-2953