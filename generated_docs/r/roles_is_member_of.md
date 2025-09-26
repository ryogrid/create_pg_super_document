# roles_is_member_of

## Location
[src/backend/utils/adt/acl.c:5019-5150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L5019-L5150)

## Overview
Gets a list of all roles that a specified role is a member of, with configurable recursion types and caching for performance optimization.

## Definition
```c
static List *
roles_is_member_of(Oid roleid, enum RoleRecurseType type,
                   Oid admin_of, Oid *admin_role)
```

## Detailed Description
This function computes the transitive closure of role membership for a given role, supporting different types of recursion based on grant inheritance and set options. It implements a breadth-first traversal to ensure closer relationships appear earlier in the result list.

The function includes several key optimizations:
- Caching mechanism to avoid recomputing membership lists for repeated queries
- Integration with roles_list_append() and Bloom filters for efficient duplicate detection
- Special handling for the pg_database_owner implicit membership

The recursion type controls which grants are followed:
- ROLERECURSE_MEMBERS: follows all grants
- ROLERECURSE_PRIVS: only inheritable grants
- ROLERECURSE_SETROLE: only grants with set_option

## Parameters / Member Variables
- `roleid`: The OID of the role whose memberships to compute
- `type`: Enum specifying the recursion type (MEMBERS, PRIVS, or SETROLE)
- `admin_of`: OID of role to check admin privileges for (InvalidOid if not needed)
- `admin_role`: Output parameter set to the first role with ADMIN OPTION on admin_of

## Dependencies
- Functions called/Symbols referenced:
  - RoleRecurseType (enum)
  - bloom_filter (data structure)
  - roles_list_append
  - SearchSysCacheList1
  - Form_pg_auth_members
  - bloom_free
  - list_make1_oid
  - list_copy
  - list_free
  - PointerIsValid
  - ReleaseSysCacheList
- Called from:
  - has_privs_of_role
  - member_can_set_role
  - is_member_of_role
  - is_member_of_role_nosuper
  - is_admin_of_role
  - select_best_admin
  - select_best_grantor

## Notes and Other Information
- Results are cached globally and only valid until the next call to this function
- Returns results in breadth-first order for select_best_grantor optimization
- Handles pg_database_owner implicit membership for database owners
- Uses TopMemoryContext for cached results to ensure persistence
- Includes safety checks to prevent infinite loops in the membership graph
- Special handling for non-database backends (e.g., WAL senders)