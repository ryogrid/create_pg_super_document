# sqlda_compat

## Location
src/interfaces/ecpg/include/sqlda-compat.h: 37 - 47

## Overview
The  structure represents a SQL Descriptor Area (SQLDA) in the ECPG compatibility layer, serving as a container for multiple SQL variable descriptors and providing metadata about the descriptor collection itself.

## Definition


## Detailed Description
The  structure implements a SQL Descriptor Area (SQLDA) for the ECPG compatibility layer. It acts as a container that holds an array of  structures, each describing individual SQL variables or columns. This structure supports dynamic SQL operations where the number and types of variables are determined at runtime.

The structure supports chaining multiple SQLDA instances through the  pointer, allowing for complex query results that exceed the capacity of a single descriptor area. The descriptor includes metadata such as naming and sizing information to facilitate proper memory management and identification.

## Parameters / Member Variables
- : Number of variables/columns described in this SQLDA
- : Pointer to array of  structures containing variable descriptors
- : Fixed-size array containing the name identifier for this descriptor (19 characters)
- : Size of the SQLDA structure, used for memory management and validation
- : Pointer to the next SQLDA structure in a chained list, enabling handling of large result sets
- : Reserved pointer for future extensions and enhancements

## Dependencies
- Functions called/Symbols referenced:
  - sqlvar_compat (member type reference)
  - sqlda_compat (self-reference for chaining)
- Called from (representative examples):
  - ECPGdescribe (in src/interfaces/ecpg/ecpglib/descriptor.c:926, 927, 936, 937)
  - var_list (in src/interfaces/ecpg/ecpglib/ecpglib_extern.h:226, 228)
  - ecpg_build_params (in src/interfaces/ecpg/ecpglib/execute.c:1284)
  - ecpg_process_output (in src/interfaces/ecpg/ecpglib/execute.c:1729, 1730, 1731)
  - sqlda_compat_empty_size (in src/interfaces/ecpg/ecpglib/sqlda.c:52)
  - sqlda_native_total_size (in src/interfaces/ecpg/ecpglib/sqlda.c:204)
  - ecpg_build_compat_sqlda (in src/interfaces/ecpg/ecpglib/sqlda.c:207, 215)
  - ecpg_set_compat_sqlda (in src/interfaces/ecpg/ecpglib/sqlda.c:255, 257)
  - sqlda_t (in src/interfaces/ecpg/include/sqlda.h:8)

## Notes and Other Information
- Part of the ECPG (Embedded SQL in C for PostgreSQL) compatibility interface
- Provides backward compatibility for applications using older SQLDA interfaces
- The fixed 19-character descriptor name follows traditional SQL standards for descriptor identification
- Chaining capability through  allows handling of complex queries with large result sets
- Used extensively in dynamic SQL operations where query structure is determined at runtime
- The structure size tracking () enables safe memory operations and proper cleanup
- Reserved field ensures future extensibility without breaking existing code compatibility