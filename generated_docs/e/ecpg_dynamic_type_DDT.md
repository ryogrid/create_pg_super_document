# ecpg_dynamic_type_DDT

## Location
src/interfaces/ecpg/ecpglib/descriptor.c: 64 - 83

## Overview
Maps PostgreSQL date/time type OIDs to SQL3 descriptor data type (DDT) constants for ECPG dynamic descriptor handling.

## Definition


## Detailed Description
This function is an internal utility within the ECPG (Embedded SQL in C for PostgreSQL) library that translates PostgreSQL's internal date and time type OIDs into standardized SQL3 descriptor data type constants. It's specifically used in dynamic descriptor operations to provide type information that conforms to the SQL3 standard for embedded SQL implementations.

The function performs a straightforward mapping using a switch statement, handling the five main PostgreSQL date/time types and returning an illegal type indicator for any unrecognized OID.

## Parameters / Member Variables
- : PostgreSQL type OID (Object Identifier) representing a data type to be mapped

## Dependencies
- Functions called/Symbols referenced:
  - SQL3_DDT_DATE
  - SQL3_DDT_TIME  
  - SQL3_DDT_TIMESTAMP
  - SQL3_DDT_TIMESTAMP_WITH_TIME_ZONE
  - SQL3_DDT_TIME_WITH_TIME_ZONE
  - SQL3_DDT_ILLEGAL
- Called from (representative examples):
  - [ECPGget_desc](../E/ECPGget_desc.md)

## Notes and Other Information
- Static function, only accessible within descriptor.c
- Handles only date/time types; other PostgreSQL types return SQL3_DDT_ILLEGAL
- Part of ECPG's SQL3 compliance layer for dynamic descriptor management
- Used specifically in descriptor header retrieval operations