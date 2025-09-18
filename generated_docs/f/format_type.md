# format_type

## Location
src/backend/utils/adt/format_type.c: 60 - 111

## Overview
SQL function that converts a PostgreSQL type OID and typemod into a human-readable type name formatted in canonical SQL format.

## Definition


## Detailed Description
The  function is a PostgreSQL SQL function that takes a type OID from  and an optional typemod from  and returns a formatted type name string. It serves as the primary interface for converting internal PostgreSQL type representations into readable SQL type names.

The function handles two distinct cases for the typemod parameter:
- When typemod is NULL: Produces a "prettier" representation suitable for contexts where exact typemod interpretation doesn't matter (e.g., function arguments)
- When typemod is provided: Ensures the output can be parsed back to recreate the original typemod value, important for pg_dump compatibility

For standard types, it returns canonical SQL format names. For custom types, it returns the  with appropriate quoting if the name contains special characters or matches SQL keywords.

## Parameters / Member Variables
-  (type_oid): OID from pg_type table identifying the PostgreSQL data type
-  (typemod): Optional type modifier from pg_attribute.atttypmod, can be NULL

## Dependencies
- Functions called/Symbols referenced:
  -  - Core formatting logic implementation
  -  - Converts C string to PostgreSQL text type
  -  - PostgreSQL function return macro
  -  - Flag constant
  -  - Flag constant

- Called from (representative examples):
  -  - Expression initialization in executor
  -  - JSON expression transformations
  -  - JSON formatting utilities
  -  - JSON output validation

## Notes and Other Information
- The function is not strict, meaning it must explicitly handle NULL arguments
- The design choice to encode meaning in NULL vs -1 typemod is acknowledged as somewhat inelegant but maintained for backward compatibility
- Critical for pg_dump functionality to ensure DDL statements can recreate original table structures
- Extensively used in JSON processing and expression handling throughout PostgreSQL
- Example:  with NULL typemod returns "character", while typemod -1 returns "bpchar"