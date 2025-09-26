# IsImportableForeignTable

## Location
[src/backend/foreign/foreign.c:482-521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L482-L521)

## Overview
Determines whether a given table name should be imported during IMPORT FOREIGN SCHEMA operations based on the statement's filter options.

## Definition
```c
bool IsImportableForeignTable(const char *tablename, ImportForeignSchemaStmt *stmt)
```

## Detailed Description
This function implements the filtering logic for the IMPORT FOREIGN SCHEMA command, which allows selective importing of foreign tables from a remote schema. The function evaluates the table name against the import filter criteria specified in the ImportForeignSchemaStmt structure.

The function supports three filtering modes:
1. ALL: Import all tables (always returns true)
2. LIMIT TO: Import only tables explicitly listed (whitelist approach)  
3. EXCEPT: Import all tables except those explicitly listed (blacklist approach)

The function performs case-sensitive string comparison using strcmp() to match table names against the filter lists.

## Parameters / Member Variables
- `tablename`: The name of the table to check for import eligibility
- `stmt`: Pointer to ImportForeignSchemaStmt containing the import filter specification

## Dependencies
- Functions called/Symbols referenced:
  - strcmp
  - lfirst
  - FDW_IMPORT_SCHEMA_ALL
  - FDW_IMPORT_SCHEMA_LIMIT_TO  
  - FDW_IMPORT_SCHEMA_EXCEPT
  - [RangeVar](../R/RangeVar.md)
  - [ImportForeignSchemaStmt](ImportForeignSchemaStmt.md)
- Called from (representative examples):
  - [ImportForeignSchema](ImportForeignSchema.md)

## Notes and Other Information
- Used exclusively by IMPORT FOREIGN SCHEMA command processing
- Performs case-sensitive table name matching
- Returns boolean result indicating import eligibility
- Handles three distinct filtering strategies through list_type field
- Essential for selective schema import functionality in foreign data wrappers
- Default case returns false as safety measure (though shouldn't be reached)