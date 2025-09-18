# dumpConversion

## Location
src/bin/pg_dump/pg_dump.c: 14099 - 14194

## Overview
Writes out a single conversion definition, generating CREATE CONVERSION SQL statements for character encoding transformations between different encodings.

## Definition


## Detailed Description
The  function generates SQL commands to recreate encoding conversion objects during database dumps. It queries the pg_conversion catalog to retrieve conversion properties including source encoding, target encoding, conversion function, and default status. The function constructs CREATE CONVERSION statements with proper encoding names obtained via  system function.

The function handles both regular and default conversions - default conversions are automatically selected when converting between specific encoding pairs. The conversion function (typically a C function) performs the actual character encoding transformation.

## Parameters / Member Variables
- : Archive structure containing dump options and output methods
- : ConvInfo structure containing conversion metadata including OID, name, namespace, and owner

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - fmtQualifiedDumpable
  - [fmtId](../f/fmtId.md)
  - appendStringLiteralAH
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Only operates in schema dump mode (skipped when dopt->dataOnly is true)
- Uses PostgreSQL built-in functions  to resolve encoding names
- Supports both regular and DEFAULT conversions (automatic selection for encoding pairs)
- The conversion procedure (conproc) is output as regproc which includes proper quoting
- Generates proper DROP statements for clean restoration
- Includes comment dumping if enabled in dump options
- Supports binary upgrade scenarios