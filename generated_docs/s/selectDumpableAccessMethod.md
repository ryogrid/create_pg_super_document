# selectDumpableAccessMethod

## Location
[src/bin/pg_dump/pg_dump.c:2034-2068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L2034-L2068)

## Overview
This function determines whether an access method should be included in a PostgreSQL database dump, with special handling for version compatibility and built-in vs user-defined access methods.

## Definition
```c
static void selectDumpableAccessMethod(AccessMethodInfo *method, Archive *fout)
```

## Detailed Description
The selectDumpableAccessMethod function implements the policy logic for deciding whether to include access methods in database dumps. Access methods are a PostgreSQL 9.6+ feature that allows defining custom table and index access strategies.

The function operates with the following logic:

1. **Version check**: For PostgreSQL versions before 9.6, access methods are not supported, so they are never dumped
2. **Extension membership**: If the access method is part of an extension, that overrides all other considerations
3. **Built-in vs user-defined methods**:
   - Built-in access methods (OID ≤ g_last_builtin_oid) are not dumped since they are part of the system catalog
   - User-defined access methods (higher OIDs) are dumped only if include_everything is enabled

The function notes that built-in access methods do not currently support ACLs, which simplifies the dump logic compared to other object types.

## Parameters / Member Variables
- `method`: Pointer to AccessMethodInfo structure containing information about the access method being evaluated
- `fout`: Pointer to Archive structure containing dump context and options, including remote server version

## Dependencies
- Functions called/Symbols referenced:
  - [checkExtensionMembership](../c/checkExtensionMembership.md)
  - [AccessMethodInfo](../A/AccessMethodInfo.md) (structure)
  - DUMP_COMPONENT_NONE (constant)
  - DUMP_COMPONENT_ALL (constant)
  - g_last_builtin_oid (global variable)
- Called from (representative examples):
  - [getAccessMethods](../g/getAccessMethods.md)

## Notes and Other Information
- Access methods were introduced in PostgreSQL 9.6, hence the version check
- Like casts and procedural languages, access methods do not belong to any particular namespace
- Built-in access methods currently do not support ACLs, simplifying the dump decision
- Extension membership always takes precedence over other dump policies
- The function references getAccessMethods() for additional context about version 9.6 support
- The function is static and only used internally within pg_dump.c