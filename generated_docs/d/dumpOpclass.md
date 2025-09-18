# dumpOpclass

## Location
src/bin/pg_dump/pg_dump.c: 13342 - 13622

## Overview
Writes out a complete operator class definition including its operators and functions, generating CREATE OPERATOR CLASS and DROP OPERATOR CLASS statements for pg_dump output.

## Definition


## Detailed Description
This function generates a comprehensive CREATE OPERATOR CLASS statement by querying the PostgreSQL system catalogs to retrieve all associated operators and functions. It constructs the complete operator class definition including the data type, access method, optional operator family, storage type, operator entries with strategy numbers, and function entries with procedure numbers.

The function handles complex relationships between operator classes and their components by joining multiple system catalog tables (pg_opclass, pg_amop, pg_amproc, pg_opfamily, pg_depend) to ensure only relevant operators and functions tied to the specific operator class are included. It properly formats operator and function references, handles cross-type comparisons, and includes sorting operator families when specified.

## Parameters / Member Variables
- : Archive handle containing dump options and database connection
- : OpclassInfo structure containing operator class metadata including OID, name, namespace, and role information

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer/destroyPQExpBuffer (for SQL statement building)
  - appendPQExpBuffer/appendPQExpBufferStr/resetPQExpBuffer (for statement construction)
  - ExecuteSqlQueryForSingleRow/ExecuteSqlQuery (for catalog queries)
  - PQfnumber/PQgetvalue/PQntuples/PQclear (for result processing)
  - pg_strdup/free (for memory management)
  - fmtId/fmtQualifiedDumpable (for identifier formatting)
  - binary_upgrade_extension_member (for binary upgrade support)
  - ArchiveEntry (to register dump entry)
  - dumpComment (to handle operator class comments)
- Called from (representative examples):
  - dumpDumpableObject (as part of general object dumping)
  - fmtQualifiedDumpable

## Notes and Other Information
- Skips execution in data-only dump mode
- Handles DEFAULT operator classes with special formatting
- Retrieves and formats STORAGE clause when key type differs from input type
- Processes OPERATOR entries with strategy numbers and optional sort operator families
- Processes FUNCTION entries with procedure numbers and explicit type specifications for cross-type comparisons
- Includes fallback STORAGE clause to avoid generating invalid SQL when no operators or functions exist
- Supports binary upgrade scenarios with proper extension member handling
- Generates both creation and deletion statements for complete dump/restore cycle
- Handles operator class comments as separate dump components
- Uses dependency relationships to ensure only relevant operators and functions are included
- Part of PostgreSQL's pg_dump utility for comprehensive schema export