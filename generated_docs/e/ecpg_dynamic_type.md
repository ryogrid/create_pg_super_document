# ecpg_dynamic_type

## Location
src/interfaces/ecpg/ecpglib/typename.c: 73 - 106

## Overview
This function maps PostgreSQL object identifiers (OIDs) to SQL3 standard data type constants for dynamic SQL operations in the ECPG preprocessor.

## Definition
```c
int ecpg_dynamic_type(Oid type)
```

## Detailed Description
The `ecpg_dynamic_type` function translates PostgreSQL's internal object identifiers (OIDs) for data types into corresponding SQL3 standard type constants. This mapping is essential for dynamic SQL operations where the actual data types are determined at runtime rather than compile time. The function supports the most commonly used PostgreSQL data types and returns the appropriate SQL3 type identifier that can be used in dynamic SQL contexts.

The function handles basic data types like integers, floating-point numbers, characters, and temporal types, mapping them to their SQL3 equivalents. For unsupported or unknown types, the function returns 0, indicating an unmapped type.

## Parameters / Member Variables
- `type`: A PostgreSQL object identifier (Oid) representing the internal type identifier for a PostgreSQL data type that needs to be mapped to its SQL3 equivalent.

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL type identifier type)
  - BOOLOID, INT2OID, INT4OID, TEXTOID, FLOAT4OID, FLOAT8OID, BPCHAROID, VARCHAROID, DATEOID, TIMEOID, TIMESTAMPOID, NUMERICOID (PostgreSQL OID constants)
  - SQL3_BOOLEAN, SQL3_SMALLINT, SQL3_INTEGER, SQL3_CHARACTER, SQL3_REAL, SQL3_DOUBLE_PRECISION, SQL3_CHARACTER_VARYING, SQL3_DATE_TIME_TIMESTAMP, SQL3_NUMERIC (SQL3 type constants)
- Called from (representative examples):
  - ECPGget_desc (in descriptor.c:390, 396)
  - not_an_array_in_ecpg (in execute.c:282, 283)

## Notes and Other Information
- The function provides a bridge between PostgreSQL's internal type system and SQL3 standard type identifiers.
- Temporal types (DATE, TIME, TIMESTAMP) are all mapped to the same SQL3 constant `SQL3_DATE_TIME_TIMESTAMP`.
- Both TEXTOID and BPCHAROID are mapped to `SQL3_CHARACTER`, indicating they are treated similarly for dynamic SQL purposes.
- The function returns 0 for unknown or unsupported types, which can be used by calling code to detect unmapped types.
- This function is located in `src/interfaces/ecpg/ecpglib/typename.c` at lines 73-106.
- The function is particularly important for SQLDA (SQL Descriptor Area) operations where type information must be provided dynamically.