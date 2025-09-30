# get_index_am_oid

## Location
[src/backend/commands/amcmds.c:163-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/amcmds.c#L163-L172)

## Overview
Looks up an access method by name and verifies it corresponds to an index access method, returning its OID.

## Definition

```c
Oid
get_index_am_oid(const char *amname, bool missing_ok)
```
## Detailed Description
get_index_am_oid is a specialized wrapper function that provides type-safe lookup of index access methods. It leverages the internal get_am_type_oid function with the AMTYPE_INDEX constraint to ensure that only valid index access methods are returned. This function is commonly used throughout the system when creating or manipulating index-related objects that require validation of the access method type.

## Parameters / Member Variables
- : Name of the index access method to look up
- : If false, throws error when access method not found; if true, returns InvalidOid

## Dependencies
- Functions called/Symbols referenced:
  - [get_am_type_oid](get_am_type_oid.md): Internal worker function for access method lookup
  - AMTYPE_INDEX: Constant defining the index access method type
- Called from (representative examples):
  - [get_object_address_opcf](get_object_address_opcf.md): Object address resolution for operator classes/families
  - [DefineOpFamily](../D/DefineOpFamily.md): Operator family definition processing
  - [transformIndexConstraint](../t/transformIndexConstraint.md): Index constraint transformation in parser

## Notes and Other Information
- Provides type-safe interface specifically for index access methods
- Thin wrapper around get_am_type_oid with AMTYPE_INDEX constraint
- Used extensively in index creation and manipulation operations
- Ensures that only index-compatible access methods are accepted
- Location: src/backend/commands/amcmds.c:163-172

## Simplified Source

```c
Oid
get_index_am_oid(const char *amname, bool missing_ok)
{
    // Delegate to generic access method lookup with index type constraint
    return get_am_type_oid(amname, AMTYPE_INDEX, missing_ok);
}
```