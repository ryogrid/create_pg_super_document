# DOTypeNameCompare

## Location
[src/bin/pg_dump/pg_dump_sort.c:199-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L199-L470)

## Overview
A static comparison function that provides comprehensive sorting logic for DumpableObject instances, implementing a multi-level sorting hierarchy based on type priority, namespace, name, and object-specific natural keys.

## Definition
static int DOTypeNameCompare(const void *p1, const void *p2)

## Detailed Description
This function serves as the core comparison function for sorting PostgreSQL database objects in pg_dump. It implements a sophisticated multi-level sorting algorithm that ensures consistent, predictable ordering of database objects across dump operations. The sorting hierarchy follows this order:

1. **Type Priority**: Objects are first sorted by their type priority using dbObjectTypePriority array
2. **Namespace**: Objects within the same priority are sorted by namespace name (NULL namespaces sorted after non-NULL)
3. **Object Name**: Objects are then sorted alphabetically by their catalog column name
4. **Object Type**: Fine-grained sorting by specific object type within the same priority
5. **Natural Key Columns**: Object-specific sorting using natural key components from their catalog definitions
6. **OID**: Final fallback sorting by object ID for complete stability

The function handles many PostgreSQL object types with specialized sorting logic, including functions (by argument count and types), operators (by kind and operand types), operator classes and families (by access method), collations (by encoding), and various constraint types.

## Parameters / Member Variables
- : Pointer to first DumpableObject pointer to compare
- : Pointer to second DumpableObject pointer to compare

## Dependencies
- Functions called/Symbols referenced:
  - [pgTypeNameCompare](../p/pgTypeNameCompare.md) (for type name comparisons)
  - [accessMethodNameCompare](../a/accessMethodNameCompare.md) (for access method comparisons)
  - strcmp (standard string comparison)
  - oidcmp (OID comparison function)
- Called from (representative examples):
  - [sortDumpableObjectsByTypeName](../s/sortDumpableObjectsByTypeName.md) (via qsort callback)

## Notes and Other Information
- The function provides extensive object-specific sorting logic for functions, operators, operator classes/families, collations, attribute defaults, policies, rules, triggers, constraints, default ACLs, and publication objects
- Falls back to OID comparison in case of catalog corruption or when all other comparison levels are equal
- The sorting ensures stable, reproducible dump output that facilitates consistent database restoration and comparison
- Implements natural key sorting that translates surrogate key references to their natural key equivalents
- Located in src/bin/pg_dump/pg_dump_sort.c:199-470

## Simplified Source

```c
static int DOTypeNameCompare(const void *p1, const void *p2) {
    DumpableObject *obj1 = *(DumpableObject *const *) p1;
    DumpableObject *obj2 = *(DumpableObject *const *) p2;
    int cmpval;

    // Sort by object type priority first
    cmpval = dbObjectTypePriority[obj1->objType] - dbObjectTypePriority[obj2->objType];
    if (cmpval != 0)
        return cmpval;

    // Sort by namespace (NULL namespaces come after non-NULL)
    if (obj1->namespace && obj2->namespace) {
        cmpval = strcmp(obj1->namespace->dobj.name, obj2->namespace->dobj.name);
        if (cmpval != 0)
            return cmpval;
    } else if (obj1->namespace) {
        return -1;  // obj1 has namespace, obj2 doesn't
    } else if (obj2->namespace) {
        return 1;   // obj2 has namespace, obj1 doesn't
    }

    // Sort by object name
    cmpval = strcmp(obj1->name, obj2->name);
    if (cmpval != 0)
        return cmpval;

    // Sort by specific object type
    cmpval = obj1->objType - obj2->objType;
    if (cmpval != 0)
        return cmpval;

    // Object-specific natural key sorting
    if (obj1->objType == DO_FUNC || obj1->objType == DO_AGG) {
        // Functions: sort by argument count, then argument types
        FuncInfo *fobj1 = *(FuncInfo *const *) p1;
        FuncInfo *fobj2 = *(FuncInfo *const *) p2;

        cmpval = fobj1->nargs - fobj2->nargs;
        if (cmpval != 0)
            return cmpval;

        for (int i = 0; i < fobj1->nargs; i++) {
            cmpval = pgTypeNameCompare(fobj1->argtypes[i], fobj2->argtypes[i]);
            if (cmpval != 0)
                return cmpval;
        }
    } else if (obj1->objType == DO_OPERATOR) {
        // Operators: sort by kind, then operand types
        OprInfo *oobj1 = *(OprInfo *const *) p1;
        OprInfo *oobj2 = *(OprInfo *const *) p2;

        cmpval = (oobj2->oprkind - oobj1->oprkind);  // prefix, postfix, infix
        if (cmpval != 0)
            return cmpval;

        cmpval = pgTypeNameCompare(oobj1->oprleft, oobj2->oprleft);
        if (cmpval != 0)
            return cmpval;

        return pgTypeNameCompare(oobj1->oprright, oobj2->oprright);
    } else if (obj1->objType == DO_OPCLASS || obj1->objType == DO_OPFAMILY) {
        // Operator classes/families: sort by access method
        if (obj1->objType == DO_OPCLASS) {
            OpclassInfo *opcobj1 = *(OpclassInfo *const *) p1;
            OpclassInfo *opcobj2 = *(OpclassInfo *const *) p2;
            return accessMethodNameCompare(opcobj1->opcmethod, opcobj2->opcmethod);
        } else {
            OpfamilyInfo *opfobj1 = *(OpfamilyInfo *const *) p1;
            OpfamilyInfo *opfobj2 = *(OpfamilyInfo *const *) p2;
            return accessMethodNameCompare(opfobj1->opfmethod, opfobj2->opfmethod);
        }
    } else if (obj1->objType == DO_CONSTRAINT) {
        // Constraints: domain constraints before table constraints
        ConstraintInfo *robj1 = *(ConstraintInfo *const *) p1;
        ConstraintInfo *robj2 = *(ConstraintInfo *const *) p2;

        if (robj1->condomain && robj2->condomain) {
            return strcmp(robj1->condomain->dobj.name, robj2->condomain->dobj.name);
        } else if (robj1->condomain) {
            return PRIO_TYPE - PRIO_TABLE;  // Domain constraint first
        } else if (robj2->condomain) {
            return PRIO_TABLE - PRIO_TYPE;  // Table constraint second
        } else {
            return strcmp(robj1->contable->dobj.name, robj2->contable->dobj.name);
        }
    }
    // Additional object types (collations, policies, rules, etc.) handled similarly...

    // Final fallback: sort by OID for stability
    return oidcmp(obj1->catId.oid, obj2->catId.oid);
}
```