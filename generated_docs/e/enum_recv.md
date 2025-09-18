# enum_recv

## Location
src/backend/utils/adt/enum.c: 179 - 220

## Overview
Converts binary protocol representation of enum values to internal OID format for PostgreSQL's binary I/O operations.

## Definition


## Detailed Description
This function implements the binary input conversion for PostgreSQL enum types, handling enum values received through the binary protocol (such as in COPY BINARY operations or prepared statement parameters). It extracts the enum label from the binary message buffer and converts it to the corresponding internal OID representation.

The function performs similar validation to enum_in but works with binary protocol data instead of C strings. It extracts the text representation from the message buffer, validates the string length, looks up the enum value in the system catalog, and ensures the value is safe to use (not uncommitted). The binary protocol allows for more efficient data transfer while maintaining the same safety guarantees as text input.

## Parameters / Member Variables
-  (PG_GETARG_POINTER(0)): StringInfo buffer containing the binary protocol data
-  (PG_GETARG_OID(1)): The OID of the enum type that this value should belong to

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgtext
  - NAMEDATALEN
  - SearchSysCache2
  - CStringGetDatum
  - check_safe_enum_use
  - Form_pg_enum
  - PG_RETURN_OID
- Called from (representative examples):
  - No direct references found (called via function manager for binary protocol)

## Notes and Other Information
- This is part of PostgreSQL's binary I/O support system, complementing enum_in for text input
- Uses pq_getmsgtext to extract string data from the binary protocol buffer
- Performs the same safety validations as enum_in, including length checks and uncommitted value detection
- Properly manages memory by calling pfree on the extracted name string
- The binary protocol provides more efficient data transfer compared to text representation
- Essential for COPY BINARY operations and prepared statements with binary parameter formats
- Maintains the same error reporting patterns as the text input function for consistency