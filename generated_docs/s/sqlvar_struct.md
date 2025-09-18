# sqlvar_struct

## Location
src/interfaces/ecpg/include/sqlda-native.h: 24 - 32

## Overview
The  represents a single variable descriptor in PostgreSQL's ECPG native SQLDA, containing type information, data pointer, and metadata for database columns or parameters.

## Definition


## Detailed Description
The  is a core component of PostgreSQL's ECPG native SQLDA (SQL Descriptor Area) implementation. Each instance describes a single database column or SQL variable, providing complete metadata including data type, length, actual data pointer, null indicator, and name information. This structure enables dynamic SQL operations where the structure of result sets or parameter lists is not known at compile time.

The struct follows the standard SQLDA pattern used in embedded SQL implementations, allowing applications to introspect and manipulate database data dynamically. It's particularly important for applications that need to handle variable schemas or implement generic database access patterns.

## Parameters / Member Variables
- : A short integer indicating the SQL data type of the variable (e.g., integer, varchar, etc.)
- : A short integer specifying the length or precision of the data type
- : A pointer to the actual data buffer where the column/parameter value is stored
- : A pointer to a short integer serving as a null indicator (typically 0 for non-null, -1 for null)
- : An embedded  struct containing the name and length information for this variable

## Dependencies
- Functions called/Symbols referenced:
  - sqlname
- Called from (representative examples):
  - sqlda_native_empty_size
  - ecpg_build_native_sqlda
  - sqlda_struct

## Notes and Other Information
- This structure is part of PostgreSQL's ECPG native interface and is located in 
- Used extensively in SQLDA operations for describing individual columns in result sets and parameters
- The structure is a building block for the larger  which contains arrays of these variable descriptors
- Critical for dynamic SQL operations where schema information is determined at runtime
- The null indicator pattern () follows standard SQL embedded programming conventions
- Referenced in both test cases and production ECPG library code for SQLDA manipulation