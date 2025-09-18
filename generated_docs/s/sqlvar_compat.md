# sqlvar_compat

## Location
[src/interfaces/ecpg/include/sqlda-compat.h:8-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/include/sqlda-compat.h#L8-L36)

## Overview
The  structure represents a compatibility layer for SQL variable descriptors in the ECPG (Embedded SQL in C for PostgreSQL) interface, providing detailed metadata about SQL variables including type information, data pointers, and extended attributes.

## Definition


## Detailed Description
The  structure serves as a comprehensive descriptor for SQL variables within the ECPG compatibility layer. This structure extends basic variable information with advanced features like support for large data fields (beyond 32K), extended type information, and ownership details. It maintains backward compatibility while providing enhanced functionality for modern PostgreSQL applications.

The structure includes both primary data fields and indicator variable fields, allowing for NULL value handling and extended metadata storage. The  field specifically addresses limitations of older interfaces that were restricted to 32K data sizes.

## Parameters / Member Variables
- : SQL data type identifier for the variable
- : Length of the data in bytes
- : Pointer to the actual data storage
- : Pointer to the indicator variable (for NULL handling)
- : Name of the SQL variable
- : Reserved field for future formatting extensions
- : Data type of the indicator variable
- : Length of indicator data in bytes (limited to <32K for compatibility)
- : Pointer to indicator data (legacy, <32K limit)
- : Extended identifier for complex types
- : String name of the extended type
- : Length of the extended type name string
- : Length of the owner name string
- : Source type identifier for distinct types based on built-ins
- : Name of the type owner
- : Extended identifier of the source type
- : Enhanced data pointer supporting fields beyond 32K limit
- : Internal flags for implementation-specific use
- : Reserved pointer for future extensions

## Dependencies
- Functions called/Symbols referenced:
  - [sqlname](sqlname.md) (internal member reference)
- Called from (representative examples):
  - [sqlda_compat_empty_size](sqlda_compat_empty_size.md) (in src/interfaces/ecpg/ecpglib/sqlda.c:52)
  - [ecpg_build_compat_sqlda](../e/ecpg_build_compat_sqlda.md) (in src/interfaces/ecpg/ecpglib/sqlda.c:208, 220)
  - [sqlda_compat](sqlda_compat.md) (in src/interfaces/ecpg/include/sqlda-compat.h:40)
  - [sqlvar_t](sqlvar_t.md) (in src/interfaces/ecpg/include/sqlda.h:7)

## Notes and Other Information
- The structure includes both legacy fields (sqlilen, sqlidata) and modern equivalents (sqlilongdata) to maintain backward compatibility
- The 32K limitation in legacy fields is addressed by the sqlilongdata field for handling larger data sets
- This is part of the ECPG compatibility layer, ensuring existing applications continue to work with newer PostgreSQL versions
- The structure supports extended type information including ownership and source type details for complex data types
- Internal flags and reserved fields provide extensibility for future enhancements without breaking ABI compatibility