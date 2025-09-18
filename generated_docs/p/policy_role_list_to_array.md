# policy_role_list_to_array

## Location
[src/backend/commands/policy.c:137-192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L137-L192)

## Overview
A helper function that converts a list of RoleSpec structures into an array of role OID Datums for use in row-level security policy storage and validation.

## Definition
```c
static Datum *policy_role_list_to_array(List *roles, int *num_roles)
```

## Detailed Description
This function transforms a linked list of role specifications (RoleSpec nodes) into a compact array of PostgreSQL Datum values containing role OIDs. It handles several important cases:

1. **Empty role list**: When no roles are specified, it defaults to PUBLIC, meaning the policy applies to all users
2. **PUBLIC role handling**: When PUBLIC is specified, it validates that no other roles are listed (since PUBLIC encompasses all roles) and issues a warning if other roles are ignored
3. **Role resolution**: Converts each RoleSpec to its corresponding system OID using get_rolespec_oid()

The function allocates memory for the result array and populates it with ObjectIdGetDatum-wrapped role OIDs that can be stored in system catalogs.

## Parameters / Member Variables
- `roles`: Linked list of RoleSpec structures representing the roles for the policy
- `num_roles`: Output parameter that receives the number of roles in the resulting array

## Dependencies
- Functions called/Symbols referenced:
  - list_length (list utility function)
  - [palloc](palloc.md) (PostgreSQL memory allocation)
  - lfirst (list cell access macro)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)
  - ereport (error/warning reporting)
  - get_rolespec_oid (role specification to OID resolution)
  - ACL_ID_PUBLIC (public role constant)
  - ROLESPEC_PUBLIC (public role type constant)

- Called from:
  - [CreatePolicy](../C/CreatePolicy.md) (during policy creation to convert role specifications)
  - [AlterPolicy](../A/AlterPolicy.md) (during policy modification to convert role specifications)

## Notes and Other Information
- This is a static function, only accessible within the policy.c module
- Returns a dynamically allocated array that the caller is responsible for managing
- Handles the special case where NIL (empty list) maps to a single PUBLIC role entry
- Issues WARNING-level messages when PUBLIC is specified alongside other roles, but continues execution
- The PUBLIC role optimization reduces storage and improves performance by representing "all users" with a single entry
- Memory allocation uses palloc(), which is automatically freed at transaction end in PostgreSQL
- The function modifies the `num_roles` parameter to reflect the actual array size, which may differ from the input list length when PUBLIC optimizations are applied