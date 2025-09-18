# pgTypeNameCompare

## Location
src/bin/pg_dump/pg_dump_sort.c: 471 - 514

## Overview
A static utility function that compares two PostgreSQL data types by their namespace and type name, providing consistent ordering for type-based sorting operations in pg_dump.

## Definition
static int pgTypeNameCompare(Oid typid1, Oid typid2)

## Detailed Description
This function compares two PostgreSQL data types identified by their OIDs, implementing a hierarchical comparison that first sorts by namespace name and then by type name. The function is designed to handle catalog corruption gracefully and provides stable sorting for type-dependent database objects.

The comparison follows this hierarchy:
1. **Early Return**: If both OIDs are identical, return 0 immediately
2. **Type Lookup**: Retrieve TypeInfo objects for both OIDs using findTypeByOid
3. **Corruption Handling**: If either type is not found, assume catalog corruption and return 0 (equal)
4. **Namespace Comparison**: Compare namespace names using strcmp
5. **Type Name Comparison**: If namespaces are equal, compare type names using strcmp

The function includes special handling for unary operators where one operand OID may be InvalidOid, relying on prior oprkind sorting to ensure such cases are handled correctly.

## Parameters / Member Variables
- : OID of the first PostgreSQL data type to compare
- : OID of the second PostgreSQL data type to compare

## Dependencies
- Functions called/Symbols referenced:
  - [findTypeByOid](../f/findTypeByOid.md) (looks up TypeInfo by OID)
  - strcmp (standard string comparison)
- Called from (representative examples):
  - [DOTypeNameCompare](../D/DOTypeNameCompare.md) (for function argument type comparisons)
  - [DOTypeNameCompare](../D/DOTypeNameCompare.md) (for operator operand type comparisons)

## Notes and Other Information
- Returns 0 if types are identical or if catalog corruption is detected
- Includes assertions to catch unexpected catalog corruption scenarios during development
- Handles special cases for unary operators where one operand type may be InvalidOid
- The function assumes that types without namespaces represent catalog corruption
- Part of the comprehensive sorting system that ensures deterministic pg_dump output
- Located in src/bin/pg_dump/pg_dump_sort.c:471-514