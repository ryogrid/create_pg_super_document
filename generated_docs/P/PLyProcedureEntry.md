# PLyProcedureEntry

## Location
src/pl/plpython/plpy_procedure.h: 60 - 64

## Overview
PLyProcedureEntry is a hash table entry structure that combines a PLyProcedureKey with a pointer to the corresponding PLyProcedure, serving as the fundamental unit stored in PostgreSQL's PL/Python procedure cache.

## Definition
```c
typedef struct PLyProcedureEntry
{
    PLyProcedureKey key;    /* hash key */
    PLyProcedure *proc;
} PLyProcedureEntry;
```

## Detailed Description
PLyProcedureEntry represents a single entry in the PL/Python procedure cache hash table, linking a unique procedure identifier (PLyProcedureKey) with its compiled procedure data (PLyProcedure). This structure enables efficient lookup and storage of compiled Python procedures, avoiding the need to recompile procedures on every invocation. The entry acts as the bridge between the cache's key-based lookup mechanism and the actual procedure data, facilitating O(1) average-case access to cached procedures based on function OID and optionally relation OID for triggers.

## Parameters / Member Variables
- `key`: PLyProcedureKey structure containing the hash key that uniquely identifies this cache entry (function OID and relation OID)
- `proc`: Pointer to the PLyProcedure structure containing the complete compiled procedure data and execution context

## Dependencies
- Functions called/Symbols referenced:
  - [PLyProcedureKey](PLyProcedureKey.md) (embedded structure for cache key)
  - [PLyProcedure](PLyProcedure.md) (referenced structure containing procedure data)
- Called from (representative examples):
  - [init_procedure_caches](../i/init_procedure_caches.md) (initializes hash table to store these entries)
  - [PLy_procedure_get](PLy_procedure_get.md) (retrieves and manipulates these entries during cache lookups)

## Notes and Other Information
- This structure is the basic unit of the procedure cache hash table implementation
- The design separates the key from the data, following standard hash table entry patterns
- Memory management must ensure both the entry and the referenced PLyProcedure are properly allocated and freed
- The structure enables efficient cache invalidation by allowing direct access to both key and procedure data
- Cache entries are typically allocated in the same memory context as their associated PLyProcedure structures