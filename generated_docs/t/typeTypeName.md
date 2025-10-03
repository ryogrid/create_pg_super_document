# typeTypeName

## Location
[src/backend/parser/parse_type.c:619-629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L619-L629)

## Overview
Returns a copy of the name of a PostgreSQL data type from its type structure.

## Definition

```c
char *
typeTypeName(Type t)
```
## Detailed Description
The  function extracts the type name from a PostgreSQL type structure and returns a dynamically allocated copy of it. The function accesses the  field from the  system catalog entry and uses  to create a copy that can outlive the syscache entry. This is important because syscache entries may be invalidated or freed, so returning a copy ensures the caller has a stable reference to the type name.

The type name returned is the internal PostgreSQL name for the type (e.g., "int4", "text", "varchar"), not necessarily the SQL standard name that users might see.

## Parameters / Member Variables
- `t`: A Type structure (HeapTuple) representing a row from the pg_type system catalog
## Dependencies
- Functions called/Symbols referenced:
  - Type (typedef for HeapTuple)
  - Form_pg_type (structure representing pg_type catalog row)
  - GETSTRUCT (macro to extract structure from HeapTuple)
  - [pstrdup](../p/pstrdup.md) (function to duplicate a string in current memory context)
  - NameStr (macro to convert Name type to C string)
- Called from (representative examples):
  - [coerce_type](../c/coerce_type.md) (in parse_coerce.c:352)

## Notes and Other Information
- The returned string is allocated in the current memory context and must be freed by the caller if needed
- Uses pstrdup to ensure the result can outlive the syscache entry, preventing potential use-after-free issues
- The typname field in pg_type is of type Name, which is a fixed-length PostgreSQL internal type
- This function is part of the parser subsystem's type handling utilities

## Simplified Source

```c
char *typeTypeName(Type t) {
    // Extract the type structure from the heap tuple
    Form_pg_type typ = (Form_pg_type) GETSTRUCT(t);

    // Return a copy of the type name that can outlive the syscache entry
    // pstrdup ensures the result survives syscache invalidation
    return pstrdup(NameStr(typ->typname));
}
```

**Simplification Notes:**
- Added explanatory comments about memory management necessity
- Function is already concise, so only added documentation
- Core logic: extract type structure, get name, and create a safe copy
- Preserved the essential purpose: provide a stable copy of the type name