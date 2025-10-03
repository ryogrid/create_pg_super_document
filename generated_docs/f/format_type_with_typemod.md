# format_type_with_typemod

## Location
[src/backend/utils/adt/format_type.c:362-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/format_type.c#L362-L370)

## Overview
Backend function that formats PostgreSQL type names while explicitly including type modifier information in the output.

## Definition

```c
char *
format_type_with_typemod(Oid type_oid, int32 typemod)
```
## Detailed Description
 is a specialized formatting function that ensures type modifier information is included in the formatted type name output. Unlike other formatting functions that may ignore or suppress typemod information, this function explicitly requests that the typemod be processed and displayed using the  flag.

This function is particularly important for contexts where precise type information must be preserved, such as when generating DDL statements, comparing column definitions, or displaying detailed schema information. The typemod parameter contains important constraints like precision, scale, length limits, or other type-specific modifiers that affect the data type's behavior.

The function is extensively used throughout PostgreSQL's DDL processing, rule system, and schema comparison operations where exact type compatibility must be determined or displayed.

## Parameters / Member Variables
- `type_oid`: PostgreSQL type OID from pg_type.oid that must be valid
- `typemod`: Type modifier value that will be included in the formatted output
## Dependencies
- Functions called/Symbols referenced:
  -  - Core formatting implementation with typemod flag
  -  - Flag constant to ensure typemod inclusion

- Called from (representative examples):
  -  - Attribute mapping for table operations
  -  - Table inheritance attribute merging
  -  - Inherited attribute processing
  -  - ALTER TABLE column type preparation
  -  - View column validation
  -  - Common Table Expression analysis
  -  - Rule system result validation
  -  - [Variable](../V/Variable.md) expression formatting in rule utilities
  -  - Rule expression decompilation
  -  - Type coercion expression formatting

## Notes and Other Information
- Always returns a palloc'd string that must be freed by the caller
- Will throw an error for invalid type OIDs (no graceful fallback)
- Critical for maintaining type precision in DDL generation and schema operations
- Used extensively in pg_dump and rule system for accurate type representation
- Ensures that type constraints are preserved when displaying or comparing schema elements
- Essential for operations involving table inheritance, view definitions, and rule processing
- The typemod parameter directly affects the output format (e.g., varchar(50), numeric(10,2))

## Simplified Source

```c
char *
format_type_with_typemod(Oid type_oid, int32 typemod)
{
    // Delegate to extended formatter with explicit typemod flag
    return format_type_extended(type_oid, typemod, FORMAT_TYPE_TYPEMOD_GIVEN);
}
```