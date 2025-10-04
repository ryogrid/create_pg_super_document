# SetAttrMissing

## Location
[src/backend/catalog/heap.c:2069-2129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L2069-L2129)

## Overview
SetAttrMissing sets the missing value for an attribute using a string representation, specifically designed for binary upgrade operations to restore missing value information during PostgreSQL upgrades.

## Definition

```c
void
SetAttrMissing(Oid relid, char *attname, char *value)
```
## Detailed Description
This function is specifically designed for binary upgrade scenarios to restore missing value information for attributes. It takes a relation OID, attribute name, and string representation of a missing value, then updates the pg_attribute catalog to set atthasmissing to true and stores the parsed missing value in attmissingval. The function acquires an AccessExclusive lock on both the target relation and pg_attribute, validates that the relation is a plain table, looks up the attribute by name, and converts the string value to the appropriate array format using the attribute's type input function. This function ensures that missing value information is properly preserved during PostgreSQL binary upgrades.

## Parameters / Member Variables
- `relid`: OID of the relation containing the attribute
- `attname`: Name of the attribute to set the missing value for
- `value`: String representation of the missing value to be stored

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [SearchSysCacheAttName](SearchSysCacheAttName.md)
  - OidFunctionCall3
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
- Called from (representative examples):
  - [binary_upgrade_set_missing_value](../b/binary_upgrade_set_missing_value.md)

## Notes and Other Information
- Designed exclusively for binary upgrade operations, not for general use
- Acquires AccessExclusive lock on the target relation and holds it throughout the operation
- Only operates on plain tables (RELKIND_RELATION), silently returns for other relation types
- Uses attribute name lookup via SearchSysCacheAttName instead of attribute number
- Converts string value to proper array format using F_ARRAY_IN function with type information
- Sets both atthasmissing flag and attmissingval field in pg_attribute
- Part of PostgreSQL's binary upgrade infrastructure for preserving missing value optimizations
- Ensures missing value information is not lost during major version upgrades
- Uses the attribute's type input function to properly parse the string representation
- Maintains data integrity by validating attribute existence before proceeding

## Simplified Source

```c
void SetAttrMissing(Oid relid, char *attname, char *value) {
    Datum valuesAtt[Natts_pg_attribute] = {0};
    bool nullsAtt[Natts_pg_attribute] = {0};
    bool replacesAtt[Natts_pg_attribute] = {0};
    Datum missingval;
    Form_pg_attribute attStruct;
    Relation attrrel, tablerel;
    HeapTuple atttup, newtup;

    // Lock the target table with exclusive access
    tablerel = table_open(relid, AccessExclusiveLock);

    // Only operate on plain tables, skip other relation types
    if (tablerel->rd_rel->relkind != RELKIND_RELATION) {
        table_close(tablerel, AccessExclusiveLock);
        return;
    }

    // Open pg_attribute catalog and find the attribute by name
    attrrel = table_open(AttributeRelationId, RowExclusiveLock);
    atttup = SearchSysCacheAttName(relid, attname);
    if (!HeapTupleIsValid(atttup)) {
        elog(ERROR, "cache lookup failed for attribute %s of relation %u",
             attname, relid);
    }
    attStruct = (Form_pg_attribute) GETSTRUCT(atttup);

    // Parse the string value into proper array format
    missingval = OidFunctionCall3(F_ARRAY_IN,
                                  CStringGetDatum(value),
                                  ObjectIdGetDatum(attStruct->atttypid),
                                  Int32GetDatum(attStruct->atttypmod));

    // Update the attribute: set atthasmissing=true and store the missing value
    valuesAtt[Anum_pg_attribute_atthasmissing - 1] = BoolGetDatum(true);
    replacesAtt[Anum_pg_attribute_atthasmissing - 1] = true;
    valuesAtt[Anum_pg_attribute_attmissingval - 1] = missingval;
    replacesAtt[Anum_pg_attribute_attmissingval - 1] = true;

    // Create and store the updated tuple
    newtup = heap_modify_tuple(atttup, RelationGetDescr(attrrel),
                               valuesAtt, nullsAtt, replacesAtt);
    CatalogTupleUpdate(attrrel, &newtup->t_self, newtup);

    // Clean up
    ReleaseSysCache(atttup);
    table_close(attrrel, RowExclusiveLock);
    table_close(tablerel, AccessExclusiveLock);
}
```