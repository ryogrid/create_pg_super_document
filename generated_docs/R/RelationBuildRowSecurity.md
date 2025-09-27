# RelationBuildRowSecurity

## Location
[src/backend/commands/policy.c:193-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L193-L331)

## Overview
Loads row-level security policies from the system catalog (pg_policy) and builds the in-memory row security descriptor structure that gets cached in the relation's relcache entry.

## Definition
```c
void RelationBuildRowSecurity(Relation relation)
```

## Detailed Description
This function is responsible for constructing the complete row security policy information for a relation by scanning the pg_policy system catalog and building an in-memory representation. The function performs several key operations:

1. **Memory Context Management**: Creates a dedicated memory context ("row security descriptor") for all policy-related data, enabling efficient cleanup during relcache flushes
2. **Policy Discovery**: Scans pg_policy using the (polrelid, polname) index to consistently retrieve policies in name order
3. **Policy Parsing**: For each policy found, extracts and parses:
   - Command type (SELECT, INSERT, UPDATE, DELETE, or ALL)
   - Permissive vs restrictive policy type
   - Policy name
   - Applicable roles (converted from Datum array)
   - USING clause (qual expression)
   - WITH CHECK clause (with_check_qual expression)
4. **Expression Analysis**: Determines if policies contain sublinks for optimization purposes
5. **Cache Integration**: Attaches the completed descriptor to the relation's relcache entry

The function ensures proper memory management by carefully switching memory contexts when allocating pass-by-reference data and reparenting the final context under CacheMemoryContext for persistence.

## Parameters / Member Variables
- `relation`: Relation structure for which to build row security policy information

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (memory context creation)
  - MemoryContextCopyAndSetIdentifier (context identification)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (zero-initialized allocation)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md) (string duplication in context)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (context switching)
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md) (context reparenting)
  - [table_open](../t/table_open.md) (catalog access)
  - [table_close](../t/table_close.md) (catalog cleanup)
  - [ScanKeyInit](../S/ScanKeyInit.md) (scan key initialization)
  - [systable_beginscan](../s/systable_beginscan.md) (system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (scan iteration)
  - [systable_endscan](../s/systable_endscan.md) (scan cleanup)
  - [heap_getattr](../h/heap_getattr.md) (tuple attribute extraction)
  - DatumGetArrayTypePCopy (array datum processing)
  - TextDatumGetCString (text datum conversion)
  - [stringToNode](../s/stringToNode.md) (expression parsing)
  - [checkExprHasSubLink](../c/checkExprHasSubLink.md) (sublink detection)
  - [lcons](../l/lcons.md) (list construction)
  - RelationGetRelid (relation OID extraction)
  - RelationGetRelationName (relation name extraction)

- Called from:
  - [RelationBuildDesc](RelationBuildDesc.md) (during relation cache building)
  - Critical system index operations

## Notes and Other Information
- This is a public function, accessible from other PostgreSQL modules
- Assumes the caller has verified that pg_class.relrowsecurity is true for the relation
- Uses the PolicyPolrelidPolnameIndexId index for efficient policy lookup
- Policies are stored in reverse order in the descriptor list for historical reasons
- The function handles both USING and WITH CHECK clauses, which may be null
- Memory context management ensures that policy data persists across transaction boundaries while being cleanly freed during relcache invalidation
- Expression parsing converts stored text representations back into executable expression trees
- The hassublinks flag optimization helps the planner make informed decisions about policy evaluation costs
- Proper error handling is included for unexpected null values in required policy fields

## Simplified Source

```c
// Simplified version of RelationBuildRowSecurity
void RelationBuildRowSecurity(Relation relation) {
    MemoryContext rscxt;
    MemoryContext oldcxt = CurrentMemoryContext;
    RowSecurityDesc *rsdesc;
    Relation catalog;
    ScanKeyData skey;
    SysScanDesc sscan;
    HeapTuple tuple;

    // Step 1: Create dedicated memory context for row security policies
    rscxt = AllocSetContextCreate(CurrentMemoryContext,
                                  "row security descriptor",
                                  ALLOCSET_SMALL_SIZES);
    MemoryContextCopyAndSetIdentifier(rscxt, RelationGetRelationName(relation));

    // Step 2: Initialize row security descriptor
    rsdesc = MemoryContextAllocZero(rscxt, sizeof(RowSecurityDesc));
    rsdesc->rscxt = rscxt;

    // Step 3: Open pg_policy catalog and prepare scan
    catalog = table_open(PolicyRelationId, AccessShareLock);
    ScanKeyInit(&skey,
                Anum_pg_policy_polrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(relation)));

    // Step 4: Scan policies for this relation (ordered by policy name)
    sscan = systable_beginscan(catalog, PolicyPolrelidPolnameIndexId, true,
                               NULL, 1, &skey);

    while (HeapTupleIsValid(tuple = systable_getnext(sscan))) {
        Form_pg_policy policy_form = (Form_pg_policy) GETSTRUCT(tuple);
        RowSecurityPolicy *policy;
        Datum datum;
        bool isnull;
        char *str_value;

        // Step 5: Create new policy structure
        policy = MemoryContextAllocZero(rscxt, sizeof(RowSecurityPolicy));

        // Step 6: Extract basic policy attributes
        policy->polcmd = policy_form->polcmd;            // Command type
        policy->permissive = policy_form->polpermissive; // Policy type
        policy->policy_name = MemoryContextStrdup(rscxt, NameStr(policy_form->polname));

        // Step 7: Extract policy roles array
        datum = heap_getattr(tuple, Anum_pg_policy_polroles,
                             RelationGetDescr(catalog), &isnull);
        if (isnull)
            elog(ERROR, "unexpected null value in pg_policy.polroles");

        MemoryContextSwitchTo(rscxt);
        policy->roles = DatumGetArrayTypePCopy(datum);
        MemoryContextSwitchTo(oldcxt);

        // Step 8: Extract and parse USING clause (qual expression)
        datum = heap_getattr(tuple, Anum_pg_policy_polqual,
                             RelationGetDescr(catalog), &isnull);
        if (!isnull) {
            str_value = TextDatumGetCString(datum);
            MemoryContextSwitchTo(rscxt);
            policy->qual = (Expr *) stringToNode(str_value);
            MemoryContextSwitchTo(oldcxt);
            pfree(str_value);
        } else {
            policy->qual = NULL;
        }

        // Step 9: Extract and parse WITH CHECK clause
        datum = heap_getattr(tuple, Anum_pg_policy_polwithcheck,
                             RelationGetDescr(catalog), &isnull);
        if (!isnull) {
            str_value = TextDatumGetCString(datum);
            MemoryContextSwitchTo(rscxt);
            policy->with_check_qual = (Expr *) stringToNode(str_value);
            MemoryContextSwitchTo(oldcxt);
            pfree(str_value);
        } else {
            policy->with_check_qual = NULL;
        }

        // Step 10: Check for sublinks in expressions (optimization flag)
        policy->hassublinks = checkExprHasSubLink((Node *) policy->qual) ||
                             checkExprHasSubLink((Node *) policy->with_check_qual);

        // Step 11: Add policy to descriptor list (in reverse order)
        MemoryContextSwitchTo(rscxt);
        rsdesc->policies = lcons(policy, rsdesc->policies);
        MemoryContextSwitchTo(oldcxt);
    }

    // Step 12: Clean up scan and catalog
    systable_endscan(sscan);
    table_close(catalog, AccessShareLock);

    // Step 13: Make descriptor persistent and attach to relation cache
    MemoryContextSetParent(rscxt, CacheMemoryContext);
    relation->rd_rsdesc = rsdesc;
}
```

Key simplifications made:
- Added step-by-step comments to clarify the main workflow
- Preserved all essential logic and error handling
- Maintained proper memory context management patterns
- Kept the core algorithm structure intact
- Focused on the main execution path without removing critical functionality
- Simplified variable declarations while maintaining clarity