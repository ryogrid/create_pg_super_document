# dumpDatabase

## Location
src/bin/pg_dump/pg_dump.c: 3055 - 3520

## Overview
Dumps the complete database definition including creation statement, properties, comments, security labels, and ACLs to the archive output.

## Definition


## Detailed Description
This comprehensive function handles dumping all aspects of a database definition for pg_dump. It queries the current database's metadata from pg_database and related system catalogs, then generates appropriate CREATE DATABASE and ALTER DATABASE statements. The function handles version-specific features like locale providers, ICU locales, collation versions, and encoding settings. It creates separate archive entries for the database creation, comments, security labels, ACLs, and properties. For binary upgrades, it preserves OIDs, frozen transaction IDs, and large object metadata to ensure exact reproduction of the source database.

## Parameters / Member Variables
- : Pointer to Archive structure representing the dump output context and containing dump options

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [GetConnection](../G/GetConnection.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - atooid
  - [getRoleName](../g/getRoleName.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [fmtId](../f/fmtId.md)
  - appendStringLiteralAH
  - [createDumpId](../c/createDumpId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [buildShSecLabelQuery](../b/buildShSecLabelQuery.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [emitShSecLabels](../e/emitShSecLabels.md)
  - [dumpACL](dumpACL.md)
  - [dumpDatabaseConfig](dumpDatabaseConfig.md)
  - destroyPQExpBuffer
  - [PQclear](../P/PQclear.md)
  - free
- Types referenced:
  - [Archive](../A/Archive.md)
  - DumpOptions
  - PQExpBuffer
  - PGconn
  - PGresult
  - [CatalogId](../C/CatalogId.md)
  - DumpId
  - DumpableAcl
  - SECTION_PRE_DATA
  - SECTION_NONE
  - InvalidDumpId
- Called from:
  - [main](../m/main.md)

## Notes and Other Information
- Handles version-specific SQL generation for PostgreSQL versions 9.3+ (datminmxid), 15.0+ (ICU locale), 16.0+ (ICU rules), and 17.0+ (locale provider)
- Creates separate archive entries for database creation, properties, comments, security labels, and ACLs to handle transaction restrictions
- For binary upgrades, preserves exact OID, frozen XIDs, and collation versions
- Handles special template database considerations that prevent normal DROP operations
- Manages large object metadata preservation during binary upgrades
- Supports various locale providers (builtin, libc, ICU) with appropriate parameter handling
- Database properties are separated from creation to avoid transaction block restrictions with CREATE DATABASE
- Includes comprehensive error handling for unrecognized locale providers and missing metadata