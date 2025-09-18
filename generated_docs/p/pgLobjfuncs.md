# pgLobjfuncs

## Location
src/interfaces/libpq/libpq-int.h: 284 - 298

## Overview
pgLobjfuncs is a structure that stores the Object IDs (OIDs) of PostgreSQL backend functions needed for large object operations, enabling libpq to perform function calls for large object manipulation.

## Definition
```c
typedef struct pgLobjfuncs
{
    Oid         fn_lo_open;      /* OID of backend function lo_open       */
    Oid         fn_lo_close;     /* OID of backend function lo_close      */
    Oid         fn_lo_creat;     /* OID of backend function lo_creat      */
    Oid         fn_lo_create;    /* OID of backend function lo_create     */
    Oid         fn_lo_unlink;    /* OID of backend function lo_unlink     */
    Oid         fn_lo_lseek;     /* OID of backend function lo_lseek      */
    Oid         fn_lo_lseek64;   /* OID of backend function lo_lseek64    */
    Oid         fn_lo_tell;      /* OID of backend function lo_tell       */
    Oid         fn_lo_tell64;    /* OID of backend function lo_tell64     */
    Oid         fn_lo_truncate;  /* OID of backend function lo_truncate   */
    Oid         fn_lo_truncate64;/* OID of function lo_truncate64         */
    Oid         fn_lo_read;      /* OID of backend function LOread        */
    Oid         fn_lo_write;     /* OID of backend function LOwrite       */
} PGlobjfuncs;
```

## Detailed Description
The pgLobjfuncs structure serves as a function lookup table for PostgreSQL large object operations. Since large object functions are implemented as user-defined functions in the backend with dynamically assigned OIDs, libpq must query the system catalogs to discover these function OIDs before it can perform large object operations. This structure is allocated and populated during the first large object operation on a connection, storing the OIDs for efficient reuse. Each field corresponds to a specific large object function, enabling libpq to make fast function calls using PQfn() instead of issuing SQL commands.

## Parameters / Member Variables
- `fn_lo_open`: OID of the lo_open function for opening large objects
- `fn_lo_close`: OID of the lo_close function for closing large objects  
- `fn_lo_creat`: OID of the lo_creat function for creating large objects (deprecated)
- `fn_lo_create`: OID of the lo_create function for creating large objects with OID
- `fn_lo_unlink`: OID of the lo_unlink function for deleting large objects
- `fn_lo_lseek`: OID of the lo_lseek function for 32-bit seeking within large objects
- `fn_lo_lseek64`: OID of the lo_lseek64 function for 64-bit seeking within large objects
- `fn_lo_tell`: OID of the lo_tell function for 32-bit position reporting
- `fn_lo_tell64`: OID of the lo_tell64 function for 64-bit position reporting
- `fn_lo_truncate`: OID of the lo_truncate function for 32-bit truncation
- `fn_lo_truncate64`: OID of the lo_truncate64 function for 64-bit truncation
- `fn_lo_read`: OID of the LOread function for reading data from large objects
- `fn_lo_write`: OID of the LOwrite function for writing data to large objects

## Dependencies
- Functions called/Symbols referenced: None (data structure only)
- Used by:
  - All large object API functions in fe-lobj.c (lo_open, lo_close, lo_read, lo_write, etc.)
  - Stored in pg_conn structure as lobjfuncs field (libpq-int.h:518)
  - Memory management in pqDropServerData (fe-connect.c:618-619)

## Notes and Other Information
- This structure is allocated only when large object operations are first used on a connection
- The structure is typedef-ed as both pgLobjfuncs and PGlobjfuncs
- Function OIDs are discovered by querying pg_proc system catalog during initialization
- If any required function OID cannot be found, the entire structure is freed and large object operations fail
- Memory is managed as part of the connection cleanup process
- 64-bit variants (lseek64, tell64, truncate64) may not be available in older PostgreSQL versions