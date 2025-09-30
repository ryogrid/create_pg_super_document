# load_domaintype_info

## Location
[src/backend/utils/cache/typcache.c:994-1229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L994-L1229)

## Overview
A helper function that loads and caches domain constraint information for PostgreSQL domain types, including CHECK constraints and NOT NULL constraints from the type hierarchy.

## Definition
```c
static void load_domaintype_info(TypeCacheEntry *typentry)
```

## Detailed Description
This function is responsible for loading domain constraint information into the PostgreSQL type cache system. It scans the pg_constraint system catalog to find all constraints that apply to a domain type, including constraints inherited from parent domains in the type hierarchy.

The function performs several key operations:
1. Releases any existing stale constraint information
2. Crawls up the domain type hierarchy to collect constraints from all ancestor domains
3. Processes CHECK constraints by parsing and planning the constraint expressions
4. Handles NOT NULL constraints if specified in the domain definition
5. Creates a DomainConstraintCache structure in a dedicated memory context
6. Sorts constraints deterministically to ensure consistent application order
7. Attaches the constraint cache to the type cache entry

The function optimizes for the common case of no constraints by deferring memory allocation until constraints are actually found.

## Parameters / Member Variables
- `typentry`: A pointer to the TypeCacheEntry that will be populated with domain constraint information. This entry represents the domain type being processed.

## Dependencies
- Functions called/Symbols referenced:
  - [decr_dcc_refcount](../d/decr_dcc_refcount.md)
  - [table_open](../t/table_open.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [fastgetattr](../f/fastgetattr.md)
  - TextDatumGetCString
  - AllocSetContextCreate
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [stringToNode](../s/stringToNode.md)
  - [expression_planner](../e/expression_planner.md)
  - makeNode
  - qsort
  - [dcs_cmp](../d/dcs_cmp.md)
  - [lcons](lcons.md)
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md)
- Called from (representative examples):
  - [lookup_type_cache](lookup_type_cache.md)
  - [UpdateDomainConstraintRef](../U/UpdateDomainConstraintRef.md)

## Notes and Other Information
- This is a static function, only accessible within typcache.c
- Uses a dedicated memory context ('Domain constraints') for constraint data to enable proper cleanup
- Implements reference counting for DomainConstraintCache objects to support sharing
- [Constraint](../C/Constraint.md) expressions are pre-planned for efficiency during runtime evaluation
- Constraints from parent domains are applied before child domain constraints (using lcons)
- The function assumes it's called in a short-lived context and may leak temporary data
- Sets the TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS flag to mark the type cache entry as having valid domain data
- Handles both CHECK constraints (with expressions) and NOT NULL constraints (boolean flags)

## Simplified Source

```c
// Simplified version of load_domaintype_info
static void
load_domaintype_info(TypeCacheEntry *typentry)
{
    Oid typeOid = typentry->type_id;
    DomainConstraintCache *domain_cache = NULL;
    bool has_not_null = false;
    DomainConstraintState **constraint_array = NULL;
    int constraint_count = 0;
    Relation constraint_rel;
    MemoryContext old_context;

    // Clean up any existing constraint info
    if (typentry->domainData) {
        domain_cache = typentry->domainData;
        typentry->domainData = NULL;
        decr_dcc_refcount(domain_cache);
    }

    // Open constraint catalog for scanning
    constraint_rel = table_open(ConstraintRelationId, AccessShareLock);

    // Walk up the domain type hierarchy
    for (;;) {
        HeapTuple type_tuple;
        Form_pg_type type_form;
        ScanKeyData scan_key[1];
        SysScanDesc scan;
        HeapTuple constraint_tuple;
        int local_constraints = 0;

        // Get the current type's definition
        type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
        if (!HeapTupleIsValid(type_tuple))
            elog(ERROR, "cache lookup failed for type %u", typeOid);
        type_form = (Form_pg_type) GETSTRUCT(type_tuple);

        // Stop if we've reached a non-domain type
        if (type_form->typtype != TYPTYPE_DOMAIN) {
            ReleaseSysCache(type_tuple);
            break;
        }

        // Check for NOT NULL constraint
        if (type_form->typnotnull)
            has_not_null = true;

        // Scan for CHECK constraints on this domain
        ScanKeyInit(&scan_key[0], Anum_pg_constraint_contypid,
                   BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(typeOid));
        scan = systable_beginscan(constraint_rel, ConstraintTypidIndexId, true, NULL, 1, scan_key);

        // Process each CHECK constraint
        while (HeapTupleIsValid(constraint_tuple = systable_getnext(scan))) {
            Form_pg_constraint constraint_form = (Form_pg_constraint) GETSTRUCT(constraint_tuple);

            // Only process CHECK constraints
            if (constraint_form->contype != CONSTRAINT_CHECK)
                continue;

            // Get constraint expression
            Datum constraint_bin = fastgetattr(constraint_tuple, Anum_pg_constraint_conbin,
                                             constraint_rel->rd_att, &isNull);
            if (isNull)
                elog(ERROR, "domain constraint has NULL conbin");

            char *constraint_string = TextDatumGetCString(constraint_bin);

            // Create constraint cache if needed
            if (domain_cache == NULL) {
                MemoryContext cache_context = AllocSetContextCreate(CurrentMemoryContext,
                                                                   "Domain constraints",
                                                                   ALLOCSET_SMALL_SIZES);
                domain_cache = (DomainConstraintCache *)
                    MemoryContextAlloc(cache_context, sizeof(DomainConstraintCache));
                domain_cache->constraints = NIL;
                domain_cache->dccContext = cache_context;
                domain_cache->dccRefCount = 0;
            }

            // Build constraint node in cache context
            old_context = MemoryContextSwitchTo(domain_cache->dccContext);
            Expr *check_expr = (Expr *) stringToNode(constraint_string);
            check_expr = expression_planner(check_expr);

            DomainConstraintState *constraint_state = makeNode(DomainConstraintState);
            constraint_state->constrainttype = DOM_CONSTRAINT_CHECK;
            constraint_state->name = pstrdup(NameStr(constraint_form->conname));
            constraint_state->check_expr = check_expr;
            constraint_state->check_exprstate = NULL;
            MemoryContextSwitchTo(old_context);

            // Add to sorting array
            // ... array management logic simplified ...
            local_constraints++;
        }

        systable_endscan(scan);

        // Sort and add constraints to main list
        if (local_constraints > 0) {
            // Sort for deterministic order
            if (local_constraints > 1)
                qsort(constraint_array, local_constraints, sizeof(DomainConstraintState *), dcs_cmp);

            // Add to constraint list (parent constraints first)
            old_context = MemoryContextSwitchTo(domain_cache->dccContext);
            while (local_constraints > 0)
                domain_cache->constraints = lcons(constraint_array[--local_constraints], domain_cache->constraints);
            MemoryContextSwitchTo(old_context);
        }

        // Move to parent domain
        typeOid = type_form->typbasetype;
        ReleaseSysCache(type_tuple);
    }

    table_close(constraint_rel, AccessShareLock);

    // Add NOT NULL constraint if needed
    if (has_not_null) {
        // Create cache if needed
        if (domain_cache == NULL) {
            // ... cache creation logic ...
        }

        // Add NOT NULL constraint node
        old_context = MemoryContextSwitchTo(domain_cache->dccContext);
        DomainConstraintState *not_null_constraint = makeNode(DomainConstraintState);
        not_null_constraint->constrainttype = DOM_CONSTRAINT_NOTNULL;
        not_null_constraint->name = pstrdup("NOT NULL");
        not_null_constraint->check_expr = NULL;
        not_null_constraint->check_exprstate = NULL;
        domain_cache->constraints = lcons(not_null_constraint, domain_cache->constraints);
        MemoryContextSwitchTo(old_context);
    }

    // Attach cache to type entry
    if (domain_cache) {
        MemoryContextSetParent(domain_cache->dccContext, CacheMemoryContext);
        typentry->domainData = domain_cache;
        domain_cache->dccRefCount++;
    }

    // Mark as complete
    typentry->flags |= TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS;
}
```

Key simplifications made:
- Used more descriptive variable names throughout
- Condensed array management logic with comments
- Simplified memory context switching explanations
- Added clear section comments for major phases
- Preserved all essential error handling and logic
- Reduced from ~240 lines to ~120 lines while maintaining core functionality