# accessMethodNameCompare

## Location
[src/bin/pg_dump/pg_dump_sort.c:515-544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L515-L544)

## Overview
A static utility function that compares two PostgreSQL access methods by their names, providing consistent ordering for access method-dependent database objects in pg_dump.

## Definition
static int accessMethodNameCompare(Oid am1, Oid am2)

## Detailed Description
This function compares two PostgreSQL access methods identified by their OIDs, implementing a simple alphabetical comparison by access method name. The function is used to provide stable sorting for database objects that depend on access methods, such as operator classes and operator families.

The comparison logic is straightforward:
1. **Early Return**: If both OIDs are identical, return 0 immediately
2. **Access Method Lookup**: Retrieve AccessMethodInfo objects for both OIDs using findAccessMethodByOid
3. **Corruption Handling**: If either access method is not found, assume catalog corruption and return 0 (equal), consistent with pgTypeNameCompare behavior
4. **Name Comparison**: Compare access method names alphabetically using strcmp

The function follows the same error handling pattern as pgTypeNameCompare, treating missing access methods as catalog corruption and returning 0 to allow the calling sort function to proceed with alternative comparison criteria.

## Parameters / Member Variables
- : OID of the first PostgreSQL access method to compare
- : OID of the second PostgreSQL access method to compare

## Dependencies
- Functions called/Symbols referenced:
  - [findAccessMethodByOid](../f/findAccessMethodByOid.md) (looks up AccessMethodInfo by OID)
  - strcmp (standard string comparison)
- Called from (representative examples):
  - [DOTypeNameCompare](../D/DOTypeNameCompare.md) (for operator class access method comparisons)
  - [DOTypeNameCompare](../D/DOTypeNameCompare.md) (for operator family access method comparisons)

## Notes and Other Information
- Returns 0 if access methods are identical or if catalog corruption is detected
- Includes assertion to catch unexpected catalog corruption scenarios during development
- Follows the same error handling pattern as pgTypeNameCompare for consistency
- Used specifically for sorting operator classes and operator families by their associated access methods
- Part of the comprehensive sorting system ensuring deterministic pg_dump output order
- Located in src/bin/pg_dump/pg_dump_sort.c:515-544

## Simplified Source

```c
static int accessMethodNameCompare(Oid am1, Oid am2) {
    // Quick equality check
    if (am1 == am2)
        return 0;

    // Look up access method information for both OIDs
    AccessMethodInfo *amobj1 = findAccessMethodByOid(am1);
    AccessMethodInfo *amobj2 = findAccessMethodByOid(am2);

    // Handle catalog corruption - return equal if either method not found
    if (!amobj1 || !amobj2)
        return 0;

    // Compare access method names alphabetically
    return strcmp(amobj1->dobj.name, amobj2->dobj.name);
}
```