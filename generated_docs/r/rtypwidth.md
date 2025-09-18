# rtypwidth

## Location
[src/interfaces/ecpg/compatlib/informix.c:1011-1018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L1011-L1018)

## Overview
rtypwidth is a stub function in PostgreSQL's ECPG Informix compatibility library that returns the display width for a given SQL type.

## Definition
```c
int rtypwidth(int sqltype, int sqllen)
```

## Detailed Description
This function is part of the ECPG (Embedded SQL in C for PostgreSQL) Informix compatibility layer. It's designed to return the display width (in characters) for a particular SQL data type and length combination. However, in the current implementation, it's a stub function that simply returns 0 regardless of the input parameters. The function parameters are explicitly cast to void to suppress compiler warnings about unused parameters.

This stub implementation suggests that either:
1. The functionality is not fully implemented in PostgreSQL's Informix compatibility layer
2. The function is provided for API compatibility but not actually used in typical operations
3. It may be intended for future implementation

The function is similar to rtypmsize but focuses on display width rather than memory size requirements.

## Parameters / Member Variables
- `sqltype`: An integer representing the SQL data type identifier
- `sqllen`: An integer representing the length parameter for the SQL type

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - Referenced in ECPG_INFORMIX_EXTRA_CHARS macro at src/interfaces/ecpg/include/ecpg_informix.h:51

## Notes and Other Information
- This is a stub function that always returns 0
- Part of the ECPG Informix compatibility library
- Parameters are explicitly unused to avoid compiler warnings
- Focuses on display width rather than memory size (unlike rtypmsize)
- May be intended for future implementation or provided for API compatibility