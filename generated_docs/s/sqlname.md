# sqlname

## Location
[src/interfaces/ecpg/include/sqlda-native.h:18-23](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/include/sqlda-native.h#L18-L23)

## Overview
The  struct represents a named identifier in PostgreSQL's ECPG (Embedded SQL in C for PostgreSQL) interface, providing a fixed-length storage structure for database object names.

## Definition

```c
struct sqlname
{
	short		length;
	char		data[NAMEDATALEN];
};
```
## Detailed Description
The  struct is a fundamental data structure used in PostgreSQL's ECPG native SQLDA (SQL Descriptor Area) implementation. It encapsulates database object names (such as column names, table names, etc.) in a format that includes both the actual name data and its length. This structure ensures consistent handling of identifiers across the ECPG interface while respecting PostgreSQL's naming constraints defined by .

The struct follows PostgreSQL's standard approach of storing names with explicit length information, which is essential for proper memory management and string handling in the embedded SQL context.

## Parameters / Member Variables
- `length`: A short integer indicating the actual length of the name stored in the data field
- `data[NAMEDATALEN]`: A fixed-size character array that stores the actual name data, with size determined by PostgreSQL's  constant

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN
- Called from (representative examples):
  - [ecpg_build_compat_sqlda](../e/ecpg_build_compat_sqlda.md)
  - [ecpg_build_native_sqlda](../e/ecpg_build_native_sqlda.md)
  - [sqlvar_compat](sqlvar_compat.md)
  - [sqlvar_struct](sqlvar_struct.md)

## Notes and Other Information
- This structure is part of PostgreSQL's ECPG native interface and is located in 
- The  constant defines the maximum length for PostgreSQL identifiers (typically 64 bytes including null terminator)
- Used extensively in SQLDA operations for describing database schema information to embedded SQL applications
- The structure is referenced in both compatibility and native ECPG implementations
- Critical for test cases involving database schema description and SQLDA manipulation