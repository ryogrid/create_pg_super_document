# dumpTSConfig

## Location
[src/bin/pg_dump/pg_dump.c:14789-14908](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L14789-L14908)

## Overview
Writes out a single text search configuration definition to the PostgreSQL dump output, generating the necessary CREATE TEXT SEARCH CONFIGURATION statement along with all token-to-dictionary mappings.

## Definition

```c
static void
dumpTSConfig(Archive *fout, const TSConfigInfo *cfginfo)
```
## Detailed Description
The  function is responsible for dumping text search configuration objects during a pg_dump operation. It generates the CREATE TEXT SEARCH CONFIGURATION statement with the associated parser, then queries the database to retrieve all token-to-dictionary mappings and generates corresponding ALTER TEXT SEARCH CONFIGURATION statements to recreate the complete configuration.

The function performs two main database queries: first to get the parser information from pg_ts_parser and pg_namespace, and second to retrieve all mappings from pg_ts_config_map. It handles multiple dictionaries per token type and formats the output as separate ALTER statements for each token type.

## Parameters / Member Variables
- : Archive structure containing dump configuration and output methods
- : TSConfigInfo structure containing configuration metadata including parser OID and ownership information

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQclear](../P/PQclear.md)
  - strcmp
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - free
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (via switch statement for DO_TSCONFIG objects)

## Notes and Other Information
- Only executes during schema dumps (skipped when dopt->dataOnly is true)
- Queries the database to resolve parser references to fully qualified parser names
- Retrieves token-to-dictionary mappings from pg_ts_config_map ordered by configuration, token type, and sequence number
- Generates separate ALTER statements for each token type with all associated dictionaries
- Token names are quoted using fmtId(), while dictionary names are already properly formatted by regdictionary cast
- Supports binary upgrade mode with appropriate extension member handling
- Includes owner information in the archive entry for proper ownership restoration
- Part of PostgreSQL's text search infrastructure dumping functionality
- Configurations define how different token types are processed by mapping them to appropriate dictionaries
- Uses qualified names to handle schema-qualified configuration and parser names properly