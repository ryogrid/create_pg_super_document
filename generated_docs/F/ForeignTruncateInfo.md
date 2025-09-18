# ForeignTruncateInfo

## Location
[src/backend/commands/tablecmds.c:342-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L342-L346)

## Overview
A hash table entry structure used to organize foreign tables by their foreign server during TRUNCATE operations, enabling bulk truncation of all foreign tables belonging to each server.

## Definition
```c
typedef struct ForeignTruncateInfo
{
    Oid         serverid;
    List       *rels;
} ForeignTruncateInfo;
```

## Detailed Description
This structure serves as a hash table entry in the `ExecuteTruncateGuts` function to group foreign tables by their foreign server OID during TRUNCATE operations. When truncating multiple foreign tables, PostgreSQL organizes them by server to allow foreign data wrappers to perform bulk truncation operations efficiently. The hash table uses the server OID as the lookup key, and each entry contains a list of all foreign tables belonging to that server that are involved in the truncation operation.

This design allows each foreign data wrapper to receive all its relevant tables in a single call to `ExecForeignTruncate`, enabling optimizations such as batching truncation operations or performing them in a transaction on the foreign server.

## Parameters / Member Variables
- `serverid`: The OID of the foreign server that hosts the foreign tables in this entry
- `rels`: A list of Relation pointers representing all foreign tables belonging to this server that are being truncated

## Dependencies
- Functions called/Symbols referenced:
  - Oid (data type)
  - [List](../L/List.md) (data type from PostgreSQL list utilities)
- Called from (representative examples):
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md) (src/backend/commands/tablecmds.c:2093, 2102, 2189)

## Notes and Other Information
- Used specifically in hash tables created during TRUNCATE operations with the key being the foreign server OID
- Enables efficient bulk operations by grouping foreign tables by their hosting server
- The hash table is created with `HASH_ELEM | HASH_BLOBS | HASH_CONTEXT` flags using server OID as the key
- Each foreign data wrapper receives its complete list of tables in a single `ExecForeignTruncate` callback
- The structure is temporary and exists only for the duration of the TRUNCATE operation
- Part of PostgreSQL's foreign table infrastructure for optimizing cross-server operations