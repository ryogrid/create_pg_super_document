# get_am_oid

## Location
[src/backend/commands/amcmds.c:183-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/amcmds.c#L183-L191)

## Overview
Looks up an access method by name and returns its OID without enforcing any type constraints.

## Definition

```c
Oid
get_am_oid(const char *amname, bool missing_ok)
```
## Detailed Description
get_am_oid provides a type-agnostic wrapper for access method lookup operations. Unlike its specialized counterparts (get_index_am_oid and get_table_am_oid), this function does not enforce any access method type constraints, making it suitable for generic access method resolution where the type is not relevant or will be validated elsewhere. It serves as the most flexible interface in the access method lookup family.

## Parameters / Member Variables
- `*amname`: Name of the access method to look up
- `missing_ok`: If false, throws error when access method not found; if true, returns InvalidOid
## Dependencies
- Functions called/Symbols referenced:
  - [get_am_type_oid](get_am_type_oid.md): Internal worker function for access method lookup (called with type '\0' for no type checking)
- Called from (representative examples):
  - [get_object_address_unqualified](get_object_address_unqualified.md): Generic object address resolution
  - DEFREM_H: Referenced in command definition headers

## Notes and Other Information
- Provides type-agnostic interface for access method lookups
- Uses null character ('\0') to bypass type validation in get_am_type_oid
- Most flexible of the access method lookup functions
- Suitable for cases where access method type is irrelevant or validated elsewhere
- Primarily used in generic object management contexts
- Location: src/backend/commands/amcmds.c:183-191

## Simplified Source

```c
Oid get_am_oid(const char *amname, bool missing_ok) {
    // Look up access method OID by name without type constraints
    return get_am_type_oid(amname, '\0', missing_ok);
}
```