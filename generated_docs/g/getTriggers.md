# getTriggers

## Location
[src/bin/pg_dump/pg_dump.c:8225-8420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8225-L8420)

## Overview
Retrieves comprehensive trigger information for all dumpable tables from the PostgreSQL system catalog, handling version-specific logic and partitioned table triggers.

## Definition

```c
void
getTriggers(Archive *fout, TableInfo tblinfo[], int numTables)
```
## Detailed Description
This function performs a sophisticated query against the pg_trigger system catalog to collect information about triggers on tables that need to be dumped. It implements version-specific logic to handle differences in PostgreSQL's trigger system across major versions, particularly around partitioned tables and inherited triggers.

The function uses an optimized approach where it builds a constraint list of table OIDs to avoid selecting all triggers system-wide. This is both a security measure (avoiding functions on tables without locks) and a performance optimization. It handles several complex scenarios:

- **Version 15+**: Uses tgparentid to identify partition triggers and checks for enabled state differences
- **Version 13-14**: Uses tgisinternal flag and tgparentid for partition trigger handling
- **Version 11-12**: Uses pg_depend to match partition triggers since tgparentid doesn't exist
- **Earlier versions**: Simple trigger collection without partition support

The function also ensures that partition triggers are included when their enabled state differs from their parent trigger, allowing for proper restoration of trigger state variations across partition hierarchies.

## Parameters / Member Variables
- : Archive pointer containing database connection information and version details
- : Array of TableInfo structures representing tables to process
- : Number of elements in the tblinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [TableInfo](../T/TableInfo.md), TriggerInfo (struct types)
  - [createPQExpBuffer](../c/createPQExpBuffer.md), appendPQExpBufferChar, appendPQExpBuffer (query building)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (query execution)
  - [PQntuples](../P/PQntuples.md), PQfnumber, PQgetvalue (libpq result processing)
  - [pg_malloc](../p/pg_malloc.md) (memory allocation)
  - atooid (OID conversion)
  - [AssignDumpId](../A/AssignDumpId.md) (dump ID assignment)
  - [pg_strdup](../p/pg_strdup.md) (string duplication)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md) (cleanup)
  - DUMP_COMPONENT_DEFINITION (dump component flag)
  - DO_TRIGGER (object type enum)
  - PGRES_TUPLES_OK (result status)

- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) (primary caller during schema data collection)

## Notes and Other Information
- The function does not return trigger data directly; instead, it populates the triggers and numTriggers fields in the corresponding TableInfo structures
- Implements sophisticated version-specific SQL queries to handle PostgreSQL's evolving partition trigger system
- Uses pg_get_triggerdef with pretty=false to ensure forward-compatible dump output
- Handles both regular triggers and partition-specific triggers with different enabled states
- Includes optimization to process only tables that actually have triggers (hastriggers flag)
- The function assumes tblinfo array is sorted by OID for efficient table lookup
- Partition triggers are included even if marked as internal when their enabled state differs from the parent
- Memory allocation creates a single array for all triggers, with per-table pointers into this array