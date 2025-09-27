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
  - [bloom_filter](../b/bloom_filter.md) (data structure)
  - [roles_list_append](roles_list_append.md)
  - SearchSysCacheList1
  - Form_pg_auth_members
  - [bloom_free](../b/bloom_free.md)
  - list_make1_oid
  - [list_copy](../l/list_copy.md)
  - [list_free](../l/list_free.md)
  - PointerIsValid
  - ReleaseSysCacheList
- Called from:
  - [has_privs_of_role](../h/has_privs_of_role.md)
  - [member_can_set_role](../m/member_can_set_role.md)
  - [is_member_of_role](../i/is_member_of_role.md)
  - [is_member_of_role_nosuper](../i/is_member_of_role_nosuper.md)
  - [is_admin_of_role](../i/is_admin_of_role.md)
  - [select_best_admin](../s/select_best_admin.md)
  - [select_best_grantor](../s/select_best_grantor.md)

## Notes and Other Information
- Results are cached globally and only valid until the next call to this function
- Returns results in breadth-first order for select_best_grantor optimization
- Handles pg_database_owner implicit membership for database owners
- Uses TopMemoryContext for cached results to ensure persistence
- Includes safety checks to prevent infinite loops in the membership graph
- Special handling for non-database backends (e.g., WAL senders)

## Simplified Source

```c
// Simplified version of roles_is_member_of
static List *roles_is_member_of(Oid roleid, enum RoleRecurseType type,
                               Oid admin_of, Oid *admin_role) {
    Oid dba;
    List *roles_list;
    ListCell *l;
    List *new_cached_roles;
    MemoryContext oldctx;
    bloom_filter *bf = NULL;

    // Validate admin_of and admin_role parameters consistency
    Assert(OidIsValid(admin_of) == PointerIsValid(admin_role));
    if (admin_role != NULL)
        *admin_role = InvalidOid;

    // Return cached result if available and admin option not needed
    if (cached_role[type] == roleid && !OidIsValid(admin_of) &&
        OidIsValid(cached_role[type]))
        return cached_roles[type];

    // Get database owner (special handling for non-database backends)
    if (!OidIsValid(MyDatabaseId))
        dba = InvalidOid;
    else {
        HeapTuple dbtup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
        if (!HeapTupleIsValid(dbtup))
            elog(ERROR, "cache lookup failed for database %u", MyDatabaseId);
        dba = ((Form_pg_database) GETSTRUCT(dbtup))->datdba;
        ReleaseSysCache(dbtup);
    }

    // Initialize roles list with the starting role
    roles_list = list_make1_oid(roleid);

    // Breadth-first traversal to find all role memberships
    foreach(l, roles_list) {
        Oid memberid = lfirst_oid(l);
        CatCList *memlist;
        int i;

        // Find all roles that memberid is directly a member of
        memlist = SearchSysCacheList1(AUTHMEMMEMROLE, ObjectIdGetDatum(memberid));

        for (i = 0; i < memlist->n_members; i++) {
            HeapTuple tup = &memlist->members[i]->tuple;
            Form_pg_auth_members form = (Form_pg_auth_members) GETSTRUCT(tup);
            Oid otherid = form->roleid;

            // Check for admin option on the target role
            if (otherid == admin_of && form->admin_option &&
                OidIsValid(admin_of) && !OidIsValid(*admin_role))
                *admin_role = memberid;

            // Apply recursion type filters
            if (type == ROLERECURSE_PRIVS && !form->inherit_option)
                continue;  // Skip non-inheritable grants
            if (type == ROLERECURSE_SETROLE && !form->set_option)
                continue;  // Skip non-SET grants

            // Add role to list (with duplicate detection via Bloom filter)
            roles_list = roles_list_append(roles_list, &bf, otherid);
        }
        ReleaseSysCacheList(memlist);

        // Handle pg_database_owner implicit membership
        if (memberid == dba && OidIsValid(dba))
            roles_list = roles_list_append(roles_list, &bf, ROLE_PG_DATABASE_OWNER);
    }

    // Clean up Bloom filter
    if (bf)
        bloom_free(bf);

    // Copy result to persistent memory context
    oldctx = MemoryContextSwitchTo(TopMemoryContext);
    new_cached_roles = list_copy(roles_list);
    MemoryContextSwitchTo(oldctx);
    list_free(roles_list);

    // Update cache
    cached_role[type] = InvalidOid;  // Paranoia
    list_free(cached_roles[type]);
    cached_roles[type] = new_cached_roles;
    cached_role[type] = roleid;

    return cached_roles[type];
}
```

Key simplifications made:
- Added clear comments explaining each major section
- Explained the breadth-first traversal algorithm
- Clarified the different recursion type filters
- Explained the admin option detection logic
- Simplified variable declarations and grouping
- Documented the caching mechanism and memory management
- Preserved all performance optimizations and safety checks
- Maintained the complex but essential duplicate detection and role inheritance logic