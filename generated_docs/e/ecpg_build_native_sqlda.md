# ecpg_build_native_sqlda

## Location
src/interfaces/ecpg/ecpglib/sqlda.c: 412 - 443

## Overview
Builds a native SQLDA (SQL Descriptor Area) structure from a PostgreSQL result set, creating a standard SQLDA format with embedded column names.

## Definition
```c
struct sqlda_struct *ecpg_build_native_sqlda(int line, PGresult *res, int row, enum COMPAT_MODE compat)
```

## Detailed Description
This function constructs a `struct sqlda_struct` which represents the native (standard) format of SQLDA used in embedded SQL programming. Unlike the compatibility version, this native SQLDA follows the standard SQLDA format more closely, with a fixed header containing identification fields and metadata about the structure itself.

The function allocates memory for the complete SQLDA structure including the header and an array of SQLVAR structures. It initializes the standard SQLDA identification fields (`sqldaid`) and sets up the structure dimensions (`sqld`, `sqln`, `sqldabc`). For each column in the result set, it populates the SQL type and column name information in the embedded format where names are stored within each SQLVAR structure.

## Parameters / Member Variables
- `line`: Line number in the source code where this function is called (used for debugging and error reporting)
- `res`: PostgreSQL result set (PGresult*) containing the query results and metadata
- `row`: Row number for which space should be allocated (used for size calculation)
- `compat`: Compatibility mode enumeration that determines how SQL types are mapped

## Dependencies
- Functions called/Symbols referenced:
  - sqlda_native_total_size
  - ecpg_alloc
  - PQnfields
  - ecpg_log
  - sqlda_dynamic_type
  - PQftype
  - PQfname
- Called from (representative examples):
  - ECPGdescribe
  - ecpg_process_output

## Notes and Other Information
- The function sets the standard SQLDA identifier "SQLDA  " (8 characters) in the `sqldaid` field
- Both `sqld` (actual number of columns) and `sqln` (maximum number of columns) are set to the same value from `PQnfields()`
- The `sqldabc` field contains the byte count of the SQLDA structure including the variable-length SQLVAR array
- Column names are stored in a length-prefixed format within each SQLVAR structure (length field + data field)
- This native format is more standard-compliant compared to the compatibility version
- Memory allocation uses `ecpg_alloc()` with line number tracking for debugging purposes
- Returns NULL if memory allocation fails
- All allocated memory is zero-initialized before population