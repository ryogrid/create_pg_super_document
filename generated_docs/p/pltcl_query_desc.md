# pltcl_query_desc

## Location
src/pl/tcl/pltcl.c: 168 - 176

## Overview
A structure that caches information about prepared and saved SQL execution plans for PL/Tcl procedures, optimizing repeated query execution.

## Definition
```c
typedef struct pltcl_query_desc
{
    char        qname[20];
    SPIPlanPtr  plan;
    int         nargs;
    Oid        *argtypes;
    FmgrInfo   *arginfuncs;
    Oid        *argtypioparams;
} pltcl_query_desc;
```

## Detailed Description
The `pltcl_query_desc` structure serves as a cache for prepared SQL statements within PL/Tcl procedures. This structure enables efficient execution of repeated SQL queries by storing the compiled execution plan along with metadata about parameter types and conversion functions.

When PL/Tcl procedures execute SQL statements multiple times with the same structure but different parameter values, this caching mechanism avoids the overhead of repeatedly parsing and planning the same queries. The structure maintains all necessary information for parameter binding and type conversion between Tcl and PostgreSQL formats.

The structure is typically stored in hash tables within interpreter descriptors, allowing quick lookup of prepared statements by name. This design supports the SPI (Server Programming Interface) pattern commonly used in PostgreSQL procedural languages.

## Parameters / Member Variables
- `qname`: Fixed-size character array (20 bytes) containing the query name identifier used as a key for hash table lookups
- `plan`: Pointer to the SPI execution plan, containing the compiled and optimized query representation
- `nargs`: Number of parameters/arguments that the prepared statement accepts
- `argtypes`: Array of OIDs representing the PostgreSQL data types of each parameter
- `arginfuncs`: Array of cached input functions for converting Tcl values to the corresponding PostgreSQL parameter types
- `argtypioparams`: Array of additional parameters required by the input functions for type conversion

## Dependencies
- Functions called/Symbols referenced:
  - SPIPlanPtr (at line 171) - PostgreSQL SPI plan pointer type
  - Oid (PostgreSQL object identifier type)
  - FmgrInfo (PostgreSQL function manager structure)
- Called from (representative examples):
  - pltcl_SPI_prepare (referenced at lines 2553, 2587)
  - pltcl_SPI_execute_plan (referenced at line 2684)
  - OPT_NULLS (referenced at line 2757)

## Notes and Other Information
- The 20-character limit for `qname` suggests query names should be concise identifiers
- Arrays `argtypes`, `arginfuncs`, and `argtypioparams` all have `nargs` entries and must be kept synchronized
- This structure enables significant performance improvements for PL/Tcl procedures that execute the same SQL statements repeatedly
- The cached input functions (`arginfuncs`) eliminate the need to look up conversion functions on each query execution
- Typically managed within the `query_hash` table of `pltcl_interp_desc` structures
- Located in src/pl/tcl/pltcl.c at lines 168-176