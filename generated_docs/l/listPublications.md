# listPublications

## Location
src/bin/psql/describe.c: 6217 - 6292

## Overview
Lists PostgreSQL logical replication publications, displaying their properties such as name, owner, and replication settings.

## Definition


## Detailed Description
The  function implements the  psql meta-command functionality to display information about logical replication publications. It constructs and executes a SQL query against the  system catalog to retrieve publication details. The function supports optional pattern matching to filter results and adapts its output columns based on the PostgreSQL server version to show version-appropriate features.

The function performs several key operations:
1. Version checking to ensure the server supports publications (PostgreSQL 10.0+)
2. Dynamic SQL query construction with version-specific columns
3. Pattern validation and filtering
4. Result formatting and display using psql's standard table output format

## Parameters / Member Variables
- : Optional regular expression pattern to filter publications by name. If NULL, all publications are listed.

## Dependencies
- Functions called/Symbols referenced:
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - lengthof
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c for \dRp command processing)

## Notes and Other Information
- Requires PostgreSQL 10.0 or later (publications were introduced in version 10)
- Dynamically adjusts column display based on server version:
  - PostgreSQL 11+: Includes 'Truncates' column (pubtruncate)
  - PostgreSQL 13+: Includes 'Via root' column (pubviaroot)
- Uses psql's standard query result formatting with internationalization support
- Returns boolean indicating success/failure of the operation
- Part of psql's describe.c module which handles various \d commands