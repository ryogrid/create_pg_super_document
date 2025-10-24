# getRoleName

## Location
[src/bin/pg_dump/pg_dump.c:9946-9981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L9946-L9981)

## Overview
Looks up the name of a PostgreSQL role given its OID using binary search on a pre-populated sorted array.

## Definition
```c
static const char *getRoleName(const char *roleoid_str)
```

## Detailed Description
This function performs efficient role name lookup by converting a string OID to a numeric OID and then using binary search to find the corresponding role name in a sorted array (`rolenames`). The function assumes that role names have been pre-loaded and sorted by OID in the global `rolenames` array during pg_dump initialization. It uses a standard binary search algorithm for O(log n) lookup performance. The function is designed to always succeed in current usage contexts and will terminate the program with an error if a role OID cannot be found, indicating a serious inconsistency in the dump process.

## Parameters / Member Variables
- `roleoid_str`: String representation of the role OID to look up

## Dependencies
- Functions called/Symbols referenced:
  - atooid
  - [pg_fatal](../p/pg_fatal.md) (on error)
- Global variables accessed:
  - `rolenames`: Array of RoleNameItem structures sorted by OID
  - `nrolenames`: Number of entries in the rolenames array
- Called from (representative examples):
  - fmtQualifiedDumpable
  - [dumpDatabase](../d/dumpDatabase.md)
  - [getNamespaces](getNamespaces.md)
  - [getTypes](getTypes.md)
  - [getForeignDataWrappers](getForeignDataWrappers.md)
  - [getForeignServers](getForeignServers.md)
  - [getDefaultACLs](getDefaultACLs.md)

## Notes and Other Information
- This is a static function local to pg_dump.c
- The function expects the global `rolenames` array to be pre-populated and sorted by OID
- Uses binary search for efficient lookup in large role collections
- Terminates the program on lookup failure rather than returning an error code
- The returned string pointer points to memory owned by the rolenames array
- Role names are cached to avoid repeated database queries during dump operations
- The function is critical for resolving role ownership information throughout the dump process

## Simplified Source

```c
static const char *getRoleName(const char *roleoid_str) {
    Oid roleoid = atooid(roleoid_str);

    // Binary search through pre-loaded role names array
    if (nrolenames > 0) {
        RoleNameItem *low = &rolenames[0];
        RoleNameItem *high = &rolenames[nrolenames - 1];

        while (low <= high) {
            RoleNameItem *middle = low + (high - low) / 2;

            if (roleoid < middle->roleoid)
                high = middle - 1;
            else if (roleoid > middle->roleoid)
                low = middle + 1;
            else
                return middle->rolename; // Found match
        }
    }

    // Role not found - this should not happen in normal operation
    pg_fatal("role with OID %u does not exist", roleoid);
    return NULL;
}
```