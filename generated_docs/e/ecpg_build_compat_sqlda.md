# ecpg_build_compat_sqlda

## Location
src/interfaces/ecpg/ecpglib/sqlda.c: 205 - 254

## Overview
Builds a compatibility SQLDA (SQL Descriptor Area) structure from a PostgreSQL result set, allocating metadata for all fields while leaving space for field values in a specified row.

## Definition


## Detailed Description
This function constructs a  structure that contains metadata about the columns in a PostgreSQL query result. The SQLDA (SQL Descriptor Area) is a data structure used in embedded SQL programming to describe the format and characteristics of dynamic SQL statements. This function specifically builds the compatibility version of SQLDA, which maintains backward compatibility with older ECPG (Embedded C for PostgreSQL) applications.

The function allocates a single memory block that contains the main SQLDA structure, an array of SQLVAR structures (one per column), and space for column names. It populates the metadata fields including SQL types, column names, type identifiers, and type lengths for each field in the result set.

## Parameters / Member Variables
- : Line number in the source code where this function is called (used for debugging and error reporting)
- : PostgreSQL result set (PGresult*) containing the query results and metadata
- : Row number for which space should be allocated (though this parameter appears to be used primarily for size calculation)
- : Compatibility mode enumeration that determines how SQL types are mapped

## Dependencies
- Functions called/Symbols referenced:
  - sqlda_compat_total_size
  - ecpg_alloc
  - PQnfields
  - ecpg_log
  - sqlda_dynamic_type
  - PQftype
  - PQfname
  - PQfsize
- Called from (representative examples):
  - ECPGdescribe
  - ecpg_process_output

## Notes and Other Information
- The function allocates memory using  which includes line number tracking for debugging
- The  field is set to the total allocated size as a "cheat" to keep track of the full allocation
- The  field is currently reserved for future use and left empty
- Memory layout is carefully designed with the main structure, followed by an array of sqlvar structures, followed by column name strings
- The function returns NULL if memory allocation fails
- All allocated memory is zero-initialized before population