# DOTypeNameCompare

## Location
src/bin/pg_dump/pg_dump_sort.c: 199 - 470

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