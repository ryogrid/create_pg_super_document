# sqlvar_t

## Location
[src/interfaces/ecpg/test/expected/compat_oracle-char_array.c:30-30](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/compat_oracle-char_array.c#L30-L30)

## Overview
sqlvar_t is a typedef representing a variable structure used in PostgreSQL's Embedded SQL (ECPG) interface, providing a unified way to handle database variables across different compatibility modes.

## Definition

```c
typedef struct sqlvar_struct sqlvar_t;
```
## Detailed Description
sqlvar_t is a type alias that maps to different underlying structures depending on the compilation context. When ECPG is compiled with Informix compatibility mode (_ECPG_INFORMIX_H defined), it maps to , providing enhanced compatibility with Informix-style SQLDA (SQL Descriptor Area) handling. In standard PostgreSQL native mode, it maps to .

The compat version (struct sqlvar_compat) provides a comprehensive interface for handling database variables with extended type information, indicator variables, and support for data beyond 32K limits. This structure is designed to maintain backward compatibility while providing enhanced functionality for embedded SQL applications.

## Parameters / Member Variables
When used in Informix compatibility mode (struct sqlvar_compat):
- : Variable data type identifier
- : Length of the data in bytes
- : Pointer to the actual data
- : Pointer to the indicator variable
- : Variable name as a string
- : Reserved field for future use
- : Indicator variable type
- : Indicator variable length in bytes
- : Indicator data pointer
- : Extended identifier type
- : Extended type name string
- : Length of the extended type name
- : Length of the owner name
- : Source type for distinct built-in types
- : Owner name string
- : Extended identifier of the source type
- : Support for data fields exceeding 32K limit
- : Internal use flags
- : Reserved pointer for future extensions

## Dependencies
- Functions called/Symbols referenced:
  - [sqlvar_compat](sqlvar_compat.md) (when in Informix compatibility mode)
  - [sqlvar_struct](sqlvar_struct.md) (when in native mode)
- Called from (representative examples):
  - [main](../m/main.md) functions in ECPG test cases
  - ECPG runtime functions for SQL descriptor area management

## Notes and Other Information
- The actual structure definition depends on compilation flags (_ECPG_INFORMIX_H)
- Primarily used in ECPG (Embedded C for PostgreSQL) applications
- The compatibility version provides extensive metadata support for complex database operations
- The sqlilongdata member specifically addresses limitations of earlier implementations that were restricted to 32K data sizes
- Used extensively in test cases for both Informix compatibility and Oracle compatibility modes