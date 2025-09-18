# dumpOpfamily

## Location
src/bin/pg_dump/pg_dump.c: 13623 - 13841

## Overview
Writes out a single operator family definition along with any loose operator members that aren't bound to a specific opclass within the opfamily.

## Definition


## Detailed Description
The  function is responsible for generating SQL commands to recreate an operator family during database dumps. It constructs CREATE OPERATOR FAMILY and ALTER OPERATOR FAMILY statements to properly restore the operator family and its associated operators and support functions. The function queries the PostgreSQL catalog to fetch:

1. Operator members (pg_amop) tied directly to the opfamily
2. Support function members (pg_amproc) tied directly to the opfamily  
3. Access method information from pg_opfamily

The function generates both the CREATE command for the basic operator family definition and an ALTER command to add any loose operators and functions that are directly dependent on the family but not bound to specific operator classes.

## Parameters / Member Variables
- : Archive structure containing dump options and output methods
- : OpfamilyInfo structure containing operator family metadata including OID, name, namespace, and owner

## Dependencies
- Functions called/Symbols referenced:
  - ExecuteSqlQuery
  - ExecuteSqlQueryForSingleRow
  - fmtQualifiedDumpable
  - fmtId
  - ArchiveEntry
  - dumpComment
  - binary_upgrade_extension_member
- Called from (representative examples):
  - dumpDumpableObject

## Notes and Other Information
- Only operates in schema dump mode (skipped when dopt->dataOnly is true)
- Handles both operators with optional ORDER BY clauses for sort families
- Generates proper DROP statements for clean restoration
- Supports binary upgrade scenarios
- Includes comment dumping if enabled in dump options
- Uses PQExpBuffer for efficient SQL string construction