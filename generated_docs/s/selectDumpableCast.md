# selectDumpableCast

## Location
[src/bin/pg_dump/pg_dump.c:1976-2000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1976-L2000)

## Overview
This function determines whether a type cast should be included in a PostgreSQL database dump by distinguishing between user-defined casts and built-in system casts.

## Definition
```c
static void selectDumpableCast(CastInfo *cast, Archive *fout)
```

## Detailed Description
The selectDumpableCast function implements the policy logic for deciding whether to include type casts in database dumps. Since casts do not belong to any particular namespace and lack identifiable owners, the function uses OID ranges to differentiate between system-provided and user-defined casts.

The function first checks if the cast is part of an extension, which takes precedence over all other considerations. For non-extension casts, it examines the cast's OID:

1. **Built-in casts**: Casts with OIDs in the reserved initdb range (≤ g_last_builtin_oid) are marked as not dumpable since they are part of the system catalog
2. **User-defined casts**: Casts with higher OIDs are considered user-created and will be dumped only if the dump includes everything

The function notes that built-in casts do not currently support ACLs, so ACL-only dumps would not include them anyway.

## Parameters / Member Variables
- `cast`: Pointer to CastInfo structure containing information about the cast being evaluated
- `fout`: Pointer to Archive structure containing dump context and options

## Dependencies
- Functions called/Symbols referenced:
  - [checkExtensionMembership](../c/checkExtensionMembership.md)
  - [CastInfo](../C/CastInfo.md) (structure)
  - DUMP_COMPONENT_ALL (constant)
  - DUMP_COMPONENT_NONE (constant)
  - g_last_builtin_oid (global variable)
- Called from (representative examples):
  - [getCasts](../g/getCasts.md)

## Notes and Other Information
- Casts are unique among database objects in that they have no namespace or identifiable owner
- The OID-based approach is necessary due to the lack of other distinguishing characteristics
- Extension membership always overrides other dump policies
- Built-in casts currently do not support ACLs, simplifying the dump logic
- The function is static and only used internally within pg_dump.c