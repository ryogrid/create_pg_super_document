# dumpCollation

## Location
src/bin/pg_dump/pg_dump.c: 13842 - 14098

## Overview
Writes out a single collation definition, generating CREATE COLLATION SQL statements with proper provider-specific locale and rules configuration.

## Definition


## Detailed Description
The  function generates SQL commands to recreate a collation during database dumps. It handles multiple collation providers (libc, ICU, builtin, default) and adapts the output based on PostgreSQL version differences. The function queries the pg_collation catalog to retrieve collation properties including provider, determinism, locale settings, and ICU rules. It constructs CREATE COLLATION statements with appropriate provider-specific parameters:

- **libc provider**: Uses  and  or unified LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL=
- **ICU provider**: Uses LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL= and optional  for customization  
- **builtin provider**: Uses unified LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL= parameter
- **default provider**: Special case for pg_catalog collations

The function includes version compatibility handling for PostgreSQL 10.0+, 12.0+, 15.0+, 16.0+, and 17.0+ to manage evolving collation catalog schema changes.

## Parameters / Member Variables
- : Archive structure containing dump options and output methods
- : CollInfo structure containing collation metadata including OID, name, namespace, and owner

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - fmtQualifiedDumpable
  - [fmtId](../f/fmtId.md)
  - appendStringLiteralAH
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - pg_log_warning
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Only operates in schema dump mode (skipped when dopt->dataOnly is true)
- Includes extensive version compatibility logic for different PostgreSQL releases
- Validates collation properties and warns about invalid configurations
- Handles binary upgrade scenarios with collation version preservation
- Supports deterministic/non-deterministic collation settings (PostgreSQL 12+)
- Manages ICU collation rules for advanced locale customization (PostgreSQL 16+)
- Generates proper DROP statements for clean restoration