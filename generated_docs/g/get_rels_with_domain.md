# get_rels_with_domain

## Location
[src/backend/commands/typecmds.c:3321-3489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3321-L3489)

## Overview
Discovers and returns all relations and their specific attribute numbers that use a given domain type, including relations that use derived domain types based on the target domain.

## Definition
```c
static List *get_rels_with_domain(Oid domainOid, LOCKMODE lockmode)
```

## Detailed Description
This function performs a comprehensive search through the dependency catalog (pg_depend) to identify all relations containing columns of a specified domain type. It supports nested domains by recursively processing derived domain types and builds a list of RelToCheck structures containing the relation and all relevant attribute numbers.

The function handles several important aspects:
- Recursively processes sub-domains that are based on the target domain
- Detects and reports errors for container types (composite types, arrays, ranges) that contain the domain
- Filters relations to include only tables and materialized views with user-defined columns
- Acquires the specified lock on each relation to prevent concurrent schema changes
- Returns attributes sorted by column number for predictable output

Key limitations include potential race conditions during concurrent DDL operations and the inability to check domain values within container types.

## Parameters / Member Variables
- `domainOid`: Object identifier of the domain type to search for
- `lockmode`: Type of lock to acquire on relations (must not be NoLock)

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (prevent stack overflow in recursion)
  - [table_open](../t/table_open.md)/relation_open/relation_close (relation access)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext/systable_endscan (catalog scanning)
  - [get_typtype](get_typtype.md) (determine if dependent type is a domain)
  - [find_composite_type_dependencies](../f/find_composite_type_dependencies.md) (check for container type usage)
  - [list_concat](../l/list_concat.md) (combine results from recursive calls)
  - [format_type_be](../f/format_type_be.md) (format domain type name for error messages)
- Called from:
  - [validateDomainCheckConstraint](../v/validateDomainCheckConstraint.md) (when validating check constraints)
  - [validateDomainNotNullConstraint](../v/validateDomainNotNullConstraint.md) (when validating NOT NULL constraints)
  - [get_rels_with_domain](get_rels_with_domain.md) (recursive calls for sub-domains)

## Notes and Other Information
- Contains known concurrency issues due to inability to lock domains during operation
- Risk of deadlocks when holding multiple relation locks simultaneously
- Does not support checking domain values inside container types (composite, array, range)
- Uses weakest suitable lock (typically ShareLock) to minimize deadlock risk
- Results are deterministic due to sorting attributes by column number
- Part of the domain constraint validation infrastructure

## Simplified Source

```c
static List *
get_rels_with_domain(Oid domainOid, LOCKMODE lockmode)
{
    List *result = NIL;
    char *domainTypeName = format_type_be(domainOid);
    Relation depRel;
    ScanKeyData key[2];
    SysScanDesc depScan;
    HeapTuple depTup;

    Assert(lockmode != NoLock);
    check_stack_depth(); // Prevent stack overflow in recursion

    // Scan pg_depend to find things that depend on the domain
    depRel = table_open(DependRelationId, AccessShareLock);

    // Set up scan keys to find dependencies on this domain
    ScanKeyInit(&key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(TypeRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(domainOid));

    depScan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 2, key);

    while (HeapTupleIsValid(depTup = systable_getnext(depScan)))
    {
        Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
        RelToCheck *rtc = NULL;
        ListCell *rellist;

        // Handle directly dependent types (sub-domains or container types)
        if (pg_depend->classid == TypeRelationId)
        {
            if (get_typtype(pg_depend->objid) == TYPTYPE_DOMAIN)
            {
                // Sub-domain: recursively add dependent columns
                result = list_concat(result,
                                   get_rels_with_domain(pg_depend->objid, lockmode));
            }
            else
            {
                // Container type: check for dependencies and fail if found
                find_composite_type_dependencies(pg_depend->objid, NULL, domainTypeName);
            }
            continue;
        }

        // Skip non-relation dependencies or system columns
        if (pg_depend->classid != RelationRelationId || pg_depend->objsubid <= 0)
            continue;

        // Find existing RelToCheck entry for this relation
        foreach(rellist, result)
        {
            RelToCheck *rt = (RelToCheck *) lfirst(rellist);
            if (RelationGetRelid(rt->rel) == pg_depend->objid)
            {
                rtc = rt;
                break;
            }
        }

        if (rtc == NULL)
        {
            // First attribute found for this relation
            Relation rel = relation_open(pg_depend->objid, lockmode);

            // Check for composite type dependencies
            if (OidIsValid(rel->rd_rel->reltype))
                find_composite_type_dependencies(rel->rd_rel->reltype, NULL, domainTypeName);

            // Only process tables and materialized views
            if (rel->rd_rel->relkind != RELKIND_RELATION &&
                rel->rd_rel->relkind != RELKIND_MATVIEW)
            {
                relation_close(rel, lockmode);
                continue;
            }

            // Create new RelToCheck entry
            rtc = (RelToCheck *) palloc(sizeof(RelToCheck));
            rtc->rel = rel;
            rtc->natts = 0;
            rtc->atts = (int *) palloc(sizeof(int) * RelationGetNumberOfAttributes(rel));
            result = lappend(result, rtc);
        }

        // Validate column exists and has correct type
        if (pg_depend->objsubid > RelationGetNumberOfAttributes(rtc->rel))
            continue;

        Form_pg_attribute pg_att = TupleDescAttr(rtc->rel->rd_att, pg_depend->objsubid - 1);
        if (pg_att->attisdropped || pg_att->atttypid != domainOid)
            continue;

        // Add column to result, maintaining sort order
        int ptr = rtc->natts++;
        while (ptr > 0 && rtc->atts[ptr - 1] > pg_depend->objsubid)
        {
            rtc->atts[ptr] = rtc->atts[ptr - 1];
            ptr--;
        }
        rtc->atts[ptr] = pg_depend->objsubid;
    }

    systable_endscan(depScan);
    relation_close(depRel, AccessShareLock);

    return result;
}
```