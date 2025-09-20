# collectSecLabels

## Location
[src/bin/pg_dump/pg_dump.c:15631-15716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15631-L15716)

## Overview
Queries the database to collect all security labels from pg_seclabel catalog and constructs a sorted lookup table for efficient access during the dump process.

## Definition

```c
struct lookup table containing OIDs in numeric form */
	i_label = PQfnumber(res, "label");
```
## Detailed Description
This function is responsible for gathering all security label information from the database at the start of the dump process. It executes a SQL query against the pg_seclabel system catalog to retrieve all security labels, then processes and stores them in a sorted array for efficient lookup during the actual dumping phase.

The function performs several important tasks: it retrieves label text, provider names, and object identifiers; validates that labels correspond to dumpable objects; handles special cases for composite types where column labels need to be associated with the type object; and sets appropriate component flags on dumpable objects to indicate they have associated security labels.

The resulting array is sorted by (classoid, objoid, objsubid) to enable efficient binary search lookups by the findSecLabels function. This preprocessing step ensures that security label operations during the dump are performant even for databases with many labeled objects.

## Parameters / Member Variables
- : Archive structure containing database connection and dump configuration

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer: Creates buffer for SQL query construction
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md): Adds SQL query text to buffer
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md): Executes the security label query against database
  - [PQfnumber](../P/PQfnumber.md): Gets column numbers from query result
  - [PQntuples](../P/PQntuples.md): Gets number of result rows
  - pg_malloc: Allocates memory for security labels array
  - atooid: Converts string to OID
  - [findObjectByCatalogId](../f/findObjectByCatalogId.md): Locates dumpable object by catalog ID
  - [findTypeByOid](../f/findTypeByOid.md): Locates type information by OID
  - [pg_strdup](../p/pg_strdup.md): Duplicates strings for storage
  - [PQclear](../P/PQclear.md): Frees query result memory
  - destroyPQExpBuffer: Cleans up query buffer
- Called from:
  - [main](../m/main.md): Called during pg_dump initialization phase
  - fmtQualifiedDumpable: Referenced for qualified name formatting

## Notes and Other Information
- Creates and populates the global seclabels array and nseclabels counter
- Only stores labels for objects that will actually be dumped
- Handles special case of composite type column labels by setting flags on the type object instead of the table
- Sets DUMP_COMPONENT_SECLABEL flag on objects with security labels to ensure they get proper dump treatment
- Memory allocated for labels persists for the duration of the dump process
- [Query](../Q/Query.md) results are ordered to enable efficient binary search in findSecLabels
- Skips labels for objects that don't exist in the dumpable object list
- Part of the initialization phase that runs before any actual dumping begins