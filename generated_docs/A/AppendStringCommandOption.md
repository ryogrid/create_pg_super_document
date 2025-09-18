# AppendStringCommandOption

## Location
src/bin/pg_basebackup/streamutil.c: 833 - 855

## Overview
A utility function that appends a command option with an associated string value to a PostgreSQL server command buffer, with proper SQL string escaping for safety.

## Definition


## Detailed Description
This function extends the functionality of AppendPlainCommandOption by adding support for string values that require proper SQL escaping. It first calls AppendPlainCommandOption to append the option name, then if a non-NULL option_value is provided, it escapes the string using PQescapeStringConn and appends it in single quotes to the command buffer. This ensures that special characters in the option value are properly handled and don't cause SQL injection vulnerabilities or parsing errors.

## Parameters / Member Variables
- : PQExpBuffer to append the option to
- : Boolean flag indicating whether to use new or legacy option syntax
- : Name of the command option to append
- : String value for the option (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [AppendPlainCommandOption](AppendPlainCommandOption.md)
  - [PQescapeStringConn](../P/PQescapeStringConn.md)
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) (multiple calls in pg_basebackup.c)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)

## Notes and Other Information
- The function safely handles NULL option values by checking before processing
- Uses PostgreSQL's built-in string escaping mechanism (PQescapeStringConn) to prevent SQL injection
- Memory management is handled properly with palloc/pfree for the escaped string buffer
- Part of the pg_basebackup utility's command construction infrastructure