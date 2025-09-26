# ECPGgeneric_varchar

## Location
src/interfaces/ecpg/ecpglib/ecpglib_extern.h: 38 - 44

## Overview
A generic variable-length character string structure used by ECPG (Embedded SQL in C) to handle VARCHAR data types with dynamic length.

## Definition


## Detailed Description
ECPGgeneric_varchar is a fundamental data structure in PostgreSQL's ECPG library that represents variable-length character strings. It uses a flexible array member design pattern where the actual character data follows immediately after the length field in memory. This structure allows ECPG to efficiently handle VARCHAR columns from SQL queries by storing both the actual length of the string and the character data in a single, contiguous memory allocation.

The structure is designed to be memory-efficient and compatible with C's memory layout requirements, making it suitable for interfacing between SQL VARCHAR types and C programs.

## Parameters / Member Variables
- : An integer storing the actual length of the character string (not including null terminator)
- : A flexible array member containing the actual character data

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array declaration)
- Called from (representative examples):
  - ecpg_get_data (src/interfaces/ecpg/ecpglib/data.c:692-693)
  - get_char_item (src/interfaces/ecpg/ecpglib/descriptor.c:205-206)
  - ecpg_store_input (src/interfaces/ecpg/ecpglib/execute.c:835-836)
  - ECPGset_noind_null (src/interfaces/ecpg/ecpglib/misc.c:323-324)
  - ECPGis_noind_null (src/interfaces/ecpg/ecpglib/misc.c:397)

## Notes and Other Information
- This structure uses the flexible array member feature introduced in C99, which allows for variable-sized structures
- The actual memory allocation must account for both the fixed size of the len field and the variable size of the character array
- Used throughout the ECPG library for handling VARCHAR data types in SQL operations
- The structure is part of the external interface for ECPG, as indicated by its location in ecpglib_extern.h