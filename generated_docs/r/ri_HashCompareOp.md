# ri_HashCompareOp

## Location
[src/backend/utils/adt/ri_triggers.c:2908-3000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2908-L3000)

## Overview
Retrieves or creates cached comparison operator information for efficient type-aware equality operations in referential integrity checking.

## Definition
```c
static RI_CompareHashEntry *ri_HashCompareOp(Oid eq_opr, Oid typeid)
```

## Detailed Description
This function implements a caching mechanism for comparison operator metadata used in referential integrity constraint checking. It maintains a hash table of comparison entries that store pre-resolved function information for equality operators and any necessary type casting functions.

When a new operator-type combination is encountered, the function:
1. Looks up the operator's implementation function and caches it
2. Determines if type coercion is needed between the FK and PK types  
3. Resolves and caches any required cast functions
4. Handles special cases like polymorphic types (ANYARRAY, ANYENUM) and binary coercion

The cached information is stored in TopMemoryContext to persist for the lifetime of the backend process, providing significant performance benefits for repeated constraint checks.

## Parameters / Member Variables
- `eq_opr`: Object ID of the equality operator to cache information for
- `typeid`: Object ID of the data type that will be used with this operator

## Dependencies
- Functions called/Symbols referenced:
  - [ri_InitHashTables](ri_InitHashTables.md) (initializes hash table on first call)
  - [hash_search](../h/hash_search.md) (finds or creates hash table entries)
  - [get_opcode](../g/get_opcode.md) (gets the function implementing the operator)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (caches function manager information)
  - [op_input_types](../o/op_input_types.md) (determines operator input type requirements)
  - [find_coercion_pathway](../f/find_coercion_pathway.md) (locates type coercion functions)
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md) (checks for binary coercion compatibility)
- Called from (representative examples):
  - [ri_AttributesEqual](ri_AttributesEqual.md) (when performing attribute equality comparisons)

## Notes and Other Information
- Returns a pointer to the cached RI_CompareHashEntry containing operator and cast function information
- Initializes hash table on first use and persists entries for the backend lifetime  
- Handles type coercion scenarios where FK and PK types don't exactly match
- Supports implicit coercion pathways and relabel-type conversions
- Does not currently support array coercion or CoerceViaIO pathways
- Uses TopMemoryContext for persistent caching across multiple constraint checks
- Includes error handling for unsupported type conversion scenarios