# dumpSequence

## Location
[src/bin/pg_dump/pg_dump.c:17576-17842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L17576-L17842)

## Overview
Writes the SQL declaration (not data) of one user-defined sequence to the dump output, handling both regular sequences and identity sequences.

## Definition

```c
static void
dumpSequence(Archive *fout, const TableInfo *tbinfo)
```
## Detailed Description
The  function generates SQL CREATE SEQUENCE statements for PostgreSQL sequences. It extracts sequence metadata from either  (PostgreSQL 10+) or the sequence relation itself (older versions) and constructs appropriate DDL statements. The function handles various sequence types (smallint, integer, bigint), calculates default min/max values based on sequence type and increment direction, and supports both standalone sequences and identity sequences. For identity sequences, it generates ALTER TABLE...ADD GENERATED statements instead of CREATE SEQUENCE. The function also handles sequence ownership relationships, comments, and security labels.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*tbinfo`: TableInfo structure containing sequence metadata including OID, name, ownership information, and identity sequence status
## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [findTableByOid](../f/findTableByOid.md)
  - [binary_upgrade_set_pg_class_oids](../b/binary_upgrade_set_pg_class_oids.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/resetPQExpBuffer/destroyPQExpBuffer
- Called from (representative examples):
  - [dumpTable](dumpTable.md)

## Notes and Other Information
- Supports PostgreSQL version compatibility by using different metadata sources (pg_sequence vs sequence relation)
- Handles three sequence data types: smallint, integer, and bigint with appropriate default limits
- Identity sequences are treated specially and integrated into ALTER TABLE statements
- Sequence ownership (OWNED BY) is handled as a separate archive entry to ensure proper dependency ordering
- Binary upgrade mode preserves OIDs for pg_class entries
- Comments and security labels are dumped as separate components if enabled