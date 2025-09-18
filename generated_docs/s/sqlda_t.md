# sqlda_t

## Location
[src/interfaces/ecpg/test/expected/compat_oracle-char_array.c:31-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/compat_oracle-char_array.c#L31-L50)

## Overview
sqlda_t is a typedef representing a SQL Descriptor Area structure used in PostgreSQL's Embedded SQL (ECPG) interface for managing collections of database variables and their metadata.

## Definition
```c
typedef struct sqlda_compat sqlda_t;
```

## Detailed Description
sqlda_t is a type alias that provides a unified interface for SQL Descriptor Area (SQLDA) handling in embedded SQL applications. Like sqlvar_t, it maps to different underlying structures depending on the compilation context. When compiled with Informix compatibility mode (_ECPG_INFORMIX_H defined), it maps to `struct sqlda_compat`, while in native PostgreSQL mode it maps to `struct sqlda_struct`.

The SQLDA structure is fundamental to dynamic SQL operations, allowing programs to work with result sets and parameter lists where the number and types of columns/parameters are not known at compile time. The compatibility version provides enhanced functionality for managing multiple variables, descriptor chaining, and maintaining compatibility with legacy Informix applications.

## Parameters / Member Variables
When used in Informix compatibility mode (struct sqlda_compat):
- `sqld`: Number of variables described in the SQLDA
- `sqlvar`: Pointer to an array of sqlvar_compat structures containing variable descriptions
- `desc_name[19]`: Descriptor name (18 characters plus null terminator)
- `desc_occ`: Size of the SQLDA structure
- `desc_next`: Pointer to the next SQLDA structure for chaining multiple descriptors
- `reserved`: Reserved pointer for future extensions

## Dependencies
- Functions called/Symbols referenced:
  - [sqlda_compat](sqlda_compat.md) (when in Informix compatibility mode)
  - [sqlda_struct](sqlda_struct.md) (when in native mode)  
  - [sqlvar_compat](sqlvar_compat.md) (referenced through sqlvar member)
- Called from (representative examples):
  - [dump_sqlda](../d/dump_sqlda.md) functions in ECPG test cases
  - [main](../m/main.md) functions in various ECPG compatibility tests
  - ECPG runtime functions for dynamic SQL processing

## Notes and Other Information
- Essential for dynamic SQL operations where column information is determined at runtime
- The desc_next member enables chaining multiple SQLDA structures for complex queries
- Widely used in ECPG test suites for both Informix and Oracle compatibility testing
- The structure size is tracked in desc_occ to support proper memory management
- Critical component for applications migrating from Informix to PostgreSQL
- The 19-character desc_name field follows traditional SQLDA naming conventions from legacy database systems