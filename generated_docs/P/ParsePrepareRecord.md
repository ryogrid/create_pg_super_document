# ParsePrepareRecord

## Location
[src/backend/access/rmgrdesc/xactdesc.c:239-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/xactdesc.c#L239-L281)

## Overview
ParsePrepareRecord parses the WAL format of a two-phase commit prepare record into a structured format for easier processing by both backend and frontend code.

## Definition
```c
void ParsePrepareRecord(uint8 info, xl_xact_prepare *xlrec, xl_xact_parsed_prepare *parsed)
```

## Detailed Description
This function processes prepared transaction records as part of PostgreSQL's two-phase commit protocol. Unlike commit/abort records which have variable-length optional sections, prepare records have a fixed structure with all transaction metadata included. The function extracts comprehensive information including transaction timing, origin details, database information, subtransactions, relation file locators (for both commit and abort scenarios), statistics items, and invalidation messages. The data is stored in aligned buffers to ensure proper memory access patterns.

## Parameters / Member Variables
- `info`: Info flags (currently unused for prepare records)
- `xlrec`: Pointer to the raw WAL prepare record data structure
- `parsed`: Output structure to store the parsed prepare record information

## Dependencies
- Functions called/Symbols referenced:
  - memset
  - strncpy
  - MAXALIGN
  - [xl_xact_prepare](../x/xl_xact_prepare.md)
  - xl_xact_parsed_prepare
  - TransactionId
  - [RelFileLocator](../R/RelFileLocator.md)
  - xl_xact_stats_item
  - SharedInvalidationMessage
- Called from (representative examples):
  - [xact_desc_prepare](../x/xact_desc_prepare.md)
  - [xact_decode](../x/xact_decode.md)

## Notes and Other Information
- Specific to two-phase commit (2PC) transactions in distributed environments
- Uses MAXALIGN for proper memory alignment throughout the parsing process
- Contains both commit and abort relation lists and statistics for flexibility during 2PC resolution
- Fixed structure unlike the variable-length commit/abort records
- Critical for distributed transactions and logical replication of prepared transactions
- The GID (Global Identifier) is null-terminated and copied with strncpy
- All array pointers point directly into the WAL buffer after alignment calculations