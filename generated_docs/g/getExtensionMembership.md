# getExtensionMembership

## Location
src/bin/pg_dump/pg_dump.c: 18271 - 18363

## Overview
Obtains extension membership data from the PostgreSQL catalogs to identify objects that belong to extensions, enabling pg_dump to correctly determine whether they need to be dumped individually or will be recreated by CREATE EXTENSION commands.

## Definition
```c
void getExtensionMembership(Archive *fout, ExtensionInfo extinfo[], int numExtensions)
```

## Detailed Description
This function queries the pg_depend catalog to find all objects that are members of extensions. Extension member objects are typically not dumped individually since they will be recreated by the CREATE EXTENSION command. However, in binary upgrade mode, these members still need to be dumped individually.

The function executes a SQL query to retrieve dependency information where:
- refclassid = 'pg_extension'::regclass (references extension objects)
- deptype = 'e' (extension dependency type)

Results are ordered by referenced object ID to optimize processing when multiple objects belong to the same extension. For each dependency found, it calls recordExtensionMembership() to mark the object as an extension member.

## Parameters / Member Variables
- `fout`: Archive context for the dump operation
- `extinfo[]`: Array of ExtensionInfo structures containing extension metadata
- `numExtensions`: Number of extensions in the extinfo array

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - atooid
  - [findExtensionByOid](../f/findExtensionByOid.md)
  - [recordExtensionMembership](../r/recordExtensionMembership.md)
  - pg_log_warning
  - [PQclear](../P/PQclear.md)
  - destroyPQExpBuffer
- Called from:
  - [getSchemaData](getSchemaData.md) (in src/bin/pg_dump/common.c:140)

## Notes and Other Information
- Early termination if numExtensions is 0 for efficiency
- Uses ordered results to minimize extension lookups when processing multiple objects from the same extension
- Handles cases where referenced extensions cannot be found with warning messages
- Critical for proper extension handling in both normal and binary upgrade dump modes
- The query uses a redundant refclassid constraint that may improve search performance