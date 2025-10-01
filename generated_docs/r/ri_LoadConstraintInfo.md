# ri_LoadConstraintInfo

## Location
[src/backend/utils/adt/ri_triggers.c:2112-2193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2112-L2193)

## Overview
Fetches or creates the RI_ConstraintInfo struct for a foreign key constraint by loading constraint data from the system catalog and caching it in a hash table.

## Definition

```c
structFkConstraintRow(tup,
							   &riinfo->nkeys,
							   riinfo->fk_attnums,
							   riinfo->pk_attnums,
							   riinfo->pf_eq_oprs,
							   riinfo->pp_eq_oprs,
							   riinfo->ff_eq_oprs,
							   &riinfo->ndelsetcols,
							   riinfo->confdelsetcols);
```
## Detailed Description
This function implements a caching mechanism for foreign key constraint information. It first checks if the constraint info is already cached in ri_constraint_cache hash table. If not found or invalid, it:

1. Fetches the constraint row from pg_constraint system catalog
2. Validates that it's a foreign key constraint
3. Extracts constraint metadata (names, relation OIDs, action types, etc.)
4. Uses DeconstructFkConstraintRow to parse column mappings and operators
5. Adds the entry to a valid constraint list for efficient invalidation
6. Marks the entry as valid and returns it

The function handles constraint inheritance by determining the root constraint ID for partitioned foreign keys.

## Parameters / Member Variables
- : OID of the foreign key constraint to load

## Dependencies
- Functions called/Symbols referenced:
  - [ri_InitHashTables](ri_InitHashTables.md)
  - [hash_search](../h/hash_search.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - [get_ri_constraint_root](../g/get_ri_constraint_root.md)
  - GetSysCacheHashValue1
  - [DeconstructFkConstraintRow](../D/DeconstructFkConstraintRow.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [dclist_push_tail](../d/dclist_push_tail.md)
- Called from (representative examples):
  - [ri_FetchConstraintInfo](ri_FetchConstraintInfo.md)

## Notes and Other Information
- Uses a hash table (ri_constraint_cache) for efficient constraint info caching
- Initializes the hash table on first call via ri_InitHashTables
- Maintains a doubly-linked list (ri_constraint_cache_valid_list) of valid entries for invalidation processing
- Stores both constraint OID and root constraint OID hash values for efficient lookups
- Handles partitioned foreign key constraints by tracking constraint hierarchy
- Returns a const pointer to prevent modification of cached data
- Located in src/backend/utils/adt/ri_triggers.c:2112-2193

## Simplified Source

```c
static const RI_ConstraintInfo *
ri_LoadConstraintInfo(Oid constraintOid)
{
    RI_ConstraintInfo *riinfo;
    bool found;
    HeapTuple tup;
    Form_pg_constraint conForm;

    // Initialize hash table on first call
    if (!ri_constraint_cache)
        ri_InitHashTables();

    // Find or create hash entry
    riinfo = (RI_ConstraintInfo *) hash_search(ri_constraint_cache,
                                              &constraintOid,
                                              HASH_ENTER, &found);
    if (!found)
        riinfo->valid = false;
    else if (riinfo->valid)
        return riinfo;  // Already cached and valid

    // Fetch constraint data from pg_constraint
    tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintOid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for constraint %u", constraintOid);
    conForm = (Form_pg_constraint) GETSTRUCT(tup);

    if (conForm->contype != CONSTRAINT_FOREIGN)
        elog(ERROR, "constraint %u is not a foreign key constraint",
             constraintOid);

    // Fill in constraint info
    Assert(riinfo->constraint_id == constraintOid);
    if (OidIsValid(conForm->conparentid))
        riinfo->constraint_root_id = get_ri_constraint_root(conForm->conparentid);
    else
        riinfo->constraint_root_id = constraintOid;

    // Store hash values and basic info
    riinfo->oidHashValue = GetSysCacheHashValue1(CONSTROID,
                                                ObjectIdGetDatum(constraintOid));
    riinfo->rootHashValue = GetSysCacheHashValue1(CONSTROID,
                                                 ObjectIdGetDatum(riinfo->constraint_root_id));
    memcpy(&riinfo->conname, &conForm->conname, sizeof(NameData));
    riinfo->pk_relid = conForm->confrelid;
    riinfo->fk_relid = conForm->conrelid;
    riinfo->confupdtype = conForm->confupdtype;
    riinfo->confdeltype = conForm->confdeltype;
    riinfo->confmatchtype = conForm->confmatchtype;

    // Extract column mappings and operators
    DeconstructFkConstraintRow(tup,
                              &riinfo->nkeys,
                              riinfo->fk_attnums,
                              riinfo->pk_attnums,
                              riinfo->pf_eq_oprs,
                              riinfo->pp_eq_oprs,
                              riinfo->ff_eq_oprs,
                              &riinfo->ndelsetcols,
                              riinfo->confdelsetcols);

    ReleaseSysCache(tup);

    // Add to valid constraint list for efficient invalidation
    dclist_push_tail(&ri_constraint_cache_valid_list, &riinfo->valid_link);

    riinfo->valid = true;
    return riinfo;
}
```