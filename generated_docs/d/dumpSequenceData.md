# dumpSequenceData

## Location
[src/bin/pg_dump/pg_dump.c:17843-17892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L17843-L17892)

## Overview
Writes the current data (value state) of one user-defined sequence using SQL setval() function calls.

## Definition

```c
static void
dumpSequenceData(Archive *fout, const TableDataInfo *tdinfo)
```
## Detailed Description
The  function generates SQL statements to restore the current state of a sequence, specifically its last value and whether it has been called. It queries the sequence to retrieve  and  from the sequence relation, then creates a  call that will restore these values when the dump is loaded. This ensures that sequences maintain their proper state across dump/restore operations, preventing duplicate key violations or other issues that could arise from sequences starting over from their initial values.

## Parameters / Member Variables
- : Archive structure containing dump options and output methods
- : TableDataInfo structure containing sequence data metadata and reference to the underlying TableInfo

## Dependencies
- Functions called/Symbols referenced:
  - fmtQualifiedDumpable
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - createPQExpBuffer/resetPQExpBuffer/destroyPQExpBuffer
  - appendStringLiteralAH
  - [createDumpId](../c/createDumpId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - ngettext
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Generates setval() calls with three parameters: sequence name, last value, and is_called flag
- The is_called flag determines whether the next call to nextval() will increment the sequence or return the current last_value
- Creates a separate archive entry in the SECTION_DATA section, ensuring sequence data is restored after sequence definitions
- Uses proper SQL literal escaping through appendStringLiteralAH for sequence names
- Depends on the sequence definition being restored first (handled via dependency tracking)