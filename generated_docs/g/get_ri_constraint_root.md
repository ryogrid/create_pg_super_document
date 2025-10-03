# get_ri_constraint_root

## Location
[src/backend/utils/adt/ri_triggers.c:2194-2227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2194-L2227)

## Overview
Returns the OID of the root parent constraint in a constraint inheritance hierarchy for partitioned foreign keys.

## Definition

```c
static Oid
get_ri_constraint_root(Oid constrOid)
```
## Detailed Description
This function traverses up the constraint inheritance hierarchy to find the root constraint. In PostgreSQL's partitioned table system, foreign key constraints can be inherited from parent to child partitions, creating a hierarchy of related constraints. This function:

1. Starts with the given constraint OID
2. Looks up the constraint in pg_constraint system catalog
3. Checks the conparentid field to find the parent constraint
4. Continues traversing up the hierarchy until it finds a constraint with no parent
5. Returns the OID of the root constraint

The function uses a simple loop to walk up the constraint hierarchy, making system catalog lookups at each level.

## Parameters / Member Variables
- `constrOid`: OID of the constraint to find the root for
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - OidIsValid
- Called from (representative examples):
  - [ri_LoadConstraintInfo](../r/ri_LoadConstraintInfo.md)

## Notes and Other Information
- Essential for handling partitioned foreign key constraints where child constraints inherit from parent constraints
- Uses a straightforward iterative approach rather than recursion to avoid stack overflow with deep hierarchies
- Performs error checking to ensure constraint lookups succeed
- Returns the same OID if the input constraint is already a root constraint (has no parent)
- Located in src/backend/utils/adt/ri_triggers.c:2194-2227

## Simplified Source

```c
static Oid
get_ri_constraint_root(Oid constrOid)
{
    for (;;)
    {
        HeapTuple tuple;
        Oid constrParentOid;

        // Look up constraint in system catalog
        tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constrOid));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for constraint %u", constrOid);

        // Get parent constraint OID
        constrParentOid = ((Form_pg_constraint) GETSTRUCT(tuple))->conparentid;
        ReleaseSysCache(tuple);

        // If no parent, we've reached the root
        if (!OidIsValid(constrParentOid))
            break;

        // Move up to parent constraint
        constrOid = constrParentOid;
    }

    return constrOid;
}
```