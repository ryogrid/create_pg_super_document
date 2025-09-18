# ImportForeignSchemaType

## Location
src/include/nodes/parsenodes.h: 2942 - 2943

## Overview
ImportForeignSchemaType is an enumeration that defines the different filtering options for importing foreign tables in PostgreSQL's IMPORT FOREIGN SCHEMA statement.

## Definition


## Detailed Description
This enumeration specifies how table filtering should be applied when importing foreign schema definitions through PostgreSQL's Foreign Data Wrapper (FDW) system. The IMPORT FOREIGN SCHEMA statement allows importing table definitions from a remote schema, and this enum controls whether to import all tables, only specific tables, or all tables except specific ones.

The enum provides three distinct import strategies: unrestricted import of all available relations, selective import of only specified tables, and bulk import with specific exclusions. This flexibility allows database administrators to precisely control which foreign tables are imported into the local schema.

## Parameters / Member Variables
- : Import all relations from the foreign schema without any filtering. This corresponds to the basic "IMPORT FOREIGN SCHEMA schema_name" syntax without LIMIT TO or EXCEPT clauses.

- : Import only the specifically listed tables from the foreign schema. This corresponds to "IMPORT FOREIGN SCHEMA schema_name LIMIT TO (table1, table2, ...)" syntax, providing a whitelist approach to table selection.

- : Import all tables from the foreign schema except for the specifically listed ones. This corresponds to "IMPORT FOREIGN SCHEMA schema_name EXCEPT (table1, table2, ...)" syntax, providing a blacklist approach to table selection.

## Dependencies
- Functions called/Symbols referenced: None (this is an enum definition)
- Called from (representative examples):
  -  structure in src/include/nodes/parsenodes.h:2950

## Notes and Other Information
- This enum is defined in src/include/nodes/parsenodes.h:2937-2942
- The enum is used as the  field in the  structure to specify what type of table filtering should be applied
- The FDW prefix indicates this is part of the Foreign Data Wrapper system functionality
- Each enum value corresponds to different SQL syntax patterns in IMPORT FOREIGN SCHEMA statements
- The filtering is applied at the schema import level, allowing selective import of table definitions from remote databases
- This enum works in conjunction with the  field in ImportForeignSchemaStmt to specify which tables to include or exclude
- The import operation creates local foreign table definitions that reference tables in remote databases through FDW