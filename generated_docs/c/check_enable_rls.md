# check_enable_rls

## Location
[src/backend/utils/misc/rls.c:52-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/rls.c#L52-L141)

## Overview
Determines whether Row Level Security (RLS) should be applied to a query based on the relation, row_security setting, and current role.

## Definition

```c
int
check_enable_rls(Oid relid, Oid checkAsUser, bool noError)
```
## Detailed Description
This function evaluates whether Row Level Security (RLS) should be enabled for a given relation in the current query context. It returns one of three values:
- : RLS is not applicable to the relation at all
- : RLS is not applied for this query, but environmental changes may affect this decision  
- : RLS should be implemented and the plan cache needs invalidation if the environment changes

The function considers several factors:
1. Built-in relations (with OID < FirstNormalObjectId) never have RLS
2. Relations without  flag don't use RLS
3. Users with BYPASSRLS privilege (including superusers) bypass RLS
4. Table owners generally bypass RLS unless FORCE ROW LEVEL SECURITY is set
5. The  GUC setting can force an error instead of applying RLS

## Parameters / Member Variables
- `relid`: OID of the relation to check RLS status for
- `checkAsUser`: OID of user to check permissions as (use InvalidOid for current user)
- `noError`: If true, returns RLS_ENABLED instead of throwing error when user attempts unauthorized RLS bypass
## Dependencies
- Functions called/Symbols referenced:
  - [has_bypassrls_privilege](../h/has_bypassrls_privilege.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [InNoForceRLSOperation](../I/InNoForceRLSOperation.md)
  - [get_rel_name](../g/get_rel_name.md)
  - Form_pg_class
  - FirstNormalObjectId
- Called from (representative examples):
  - [row_security_active](../r/row_security_active.md)
  - [row_security_active_name](../r/row_security_active_name.md)
  - [get_row_security_policies](../g/get_row_security_policies.md)
  - [DoCopy](../D/DoCopy.md)
  - [ExecBuildSlotValueDescription](../E/ExecBuildSlotValueDescription.md)

## Notes and Other Information
- The function handles checking permissions as another role via checkAsUser parameter, useful for views and security definer functions
- The noError parameter allows callers to test RLS status without triggering errors, useful in error handling contexts
- The RLS_NONE_ENV return value indicates environment-dependent decisions that may affect plan caching
- Special handling exists for referential integrity checks through InNoForceRLSOperation context

## Simplified Source

```c
int check_enable_rls(Oid relid, Oid checkAsUser, bool noError)
{
    Oid user_id = OidIsValid(checkAsUser) ? checkAsUser : GetUserId();
    HeapTuple tuple;
    Form_pg_class classform;
    bool relrowsecurity;
    bool relforcerowsecurity;
    bool amowner;

    // Built-in relations don't use RLS
    if (relid < FirstNormalObjectId)
        return RLS_NONE;

    // Get relation's RLS flags from system catalog
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        return RLS_NONE;

    classform = (Form_pg_class) GETSTRUCT(tuple);
    relrowsecurity = classform->relrowsecurity;
    relforcerowsecurity = classform->relforcerowsecurity;
    ReleaseSysCache(tuple);

    // No RLS if not enabled on relation
    if (!relrowsecurity)
        return RLS_NONE;

    // Users with BYPASSRLS privilege bypass RLS
    if (has_bypassrls_privilege(user_id))
        return RLS_NONE_ENV;

    // Table owners bypass RLS unless FORCE is set
    amowner = object_ownercheck(RelationRelationId, relid, user_id);
    if (amowner) {
        if (!relforcerowsecurity || InNoForceRLSOperation())
            return RLS_NONE_ENV;
    }

    // Check if user has disabled row_security GUC
    if (!row_security && !noError)
        ereport(ERROR, /* RLS policy would affect query */);

    return RLS_ENABLED;
}
```