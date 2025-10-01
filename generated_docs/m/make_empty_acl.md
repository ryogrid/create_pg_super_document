# make_empty_acl

## Location
[src/backend/utils/adt/acl.c:448-456](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L448-L456)

## Overview
Creates and returns an empty Access Control List (ACL) with zero entries for initialization purposes.

## Definition

```c
Acl *
make_empty_acl(void)
```
## Detailed Description
The  function is a utility function that creates a completely empty ACL structure. It serves as a foundational building block in PostgreSQL's access control system, providing a clean slate ACL that can be populated with specific access rights later. The function internally delegates to  to allocate memory for an ACL structure with zero entries, ensuring proper initialization of the ACL header while maintaining no actual access control entries.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  - Internal function to allocate ACL memory with specified entry count
  -  - ACL structure type definition
- Called from (representative examples):
  -  - Used when setting default ACL permissions
  - Referenced in  type definitions

## Notes and Other Information
- This function is typically used as a starting point when building ACLs programmatically
- The returned ACL has zero entries but is properly initialized and can be extended with  or similar functions
- Memory allocated by this function should be managed according to PostgreSQL's memory context system
- Essential for scenarios where an empty ACL needs to be explicitly created rather than using NULL

## Simplified Source

```c
Acl *
make_empty_acl(void)
{
    // Allocate ACL structure with zero entries
    return allocacl(0);
}
```