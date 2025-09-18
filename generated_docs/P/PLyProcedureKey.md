# PLyProcedureKey

## Location
src/pl/plpython/plpy_procedure.h: 53 - 57

## Overview
PLyProcedureKey is a simple structure that serves as the hash key for caching PLyProcedure instances in PostgreSQL's PL/Python extension, uniquely identifying procedures based on function OID and optionally the relation OID for triggers.

## Definition
```c
typedef struct PLyProcedureKey
{
    Oid     fn_oid;     /* function OID */
    Oid     fn_rel;     /* triggered-on relation or InvalidOid */
} PLyProcedureKey;
```

## Detailed Description
PLyProcedureKey provides a compact and efficient way to uniquely identify cached PL/Python procedures in the procedure cache hash table. The key combines the function's OID with an optional relation OID to handle the special case of trigger functions, which may have different compiled forms depending on the table they operate on. For regular functions, fn_rel is set to InvalidOid, while for trigger functions, it contains the OID of the relation the trigger is defined on. This dual-key approach ensures that trigger functions are cached separately for each table while regular functions share a single cache entry regardless of calling context.

## Parameters / Member Variables
- `fn_oid`: The object identifier (OID) of the function in PostgreSQL's system catalog, uniquely identifying the function definition
- `fn_rel`: The OID of the relation (table) that the trigger operates on, or InvalidOid for non-trigger functions

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - init_procedure_caches (initializes the procedure cache hash table using this key type)
  - PLy_procedure_get (uses this key to look up cached procedures)
  - PLyProcedureEntry (contains this key as part of cache entry structure)

## Notes and Other Information
- This structure is designed to be used as a hash table key, so it must have consistent memory layout and comparison semantics
- The fn_rel field enables proper isolation of trigger function contexts across different tables
- Key comparison is typically done using memcmp or similar byte-wise comparison functions
- The structure size is kept minimal (two Oid fields) for efficient hashing and comparison operations
- InvalidOid is used as a sentinel value to indicate non-trigger functions