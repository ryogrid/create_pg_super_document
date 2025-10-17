# StoreAttrMissingVal

## Location
[src/backend/catalog/heap.c:2013-2068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L2013-L2068)

## Overview
StoreAttrMissingVal sets the missing value for a single attribute by updating pg_attribute to mark the attribute as having a missing value and storing the default value in array format.

## Definition

```c
void StoreAttrMissingVal(Relation rel, AttrNumber attnum, Datum missingval)
```
## Detailed Description
This function stores a missing value for a specified attribute when adding a column with a default value to an existing table. It updates the pg_attribute system catalog by setting atthasmissing to true and storing the provided default value in attmissingval as a single-element array. The missing value mechanism allows PostgreSQL to avoid rewriting the entire table when adding columns with defaults, instead using the stored missing value for existing rows that don't have the new column. The function is specifically designed for plain tables (RELKIND_RELATION) and constructs the missing value array using the attribute's type information.

## Parameters / Member Variables
- `rel`: Relation object for the table containing the attribute
- `attnum`: Attribute number (column number) to set the missing value for
- `missingval`: The default value to store as the missing value

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](SearchSysCache2.md)
  - [construct_array](../c/construct_array.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
- Called from (representative examples):
  - [ATExecAddColumn](../A/ATExecAddColumn.md)

## Notes and Other Information
- Only supported for plain tables (RELKIND_RELATION), enforced by assertion
- Converts the missing value into a single-element array using construct_array
- Uses attribute type information (atttypid, attlen, attbyval, attalign) for array construction
- Sets both atthasmissing flag to true and stores the value in attmissingval
- Part of the missing value optimization that avoids table rewrites when adding columns with defaults
- Commonly used during ALTER TABLE ADD COLUMN operations with DEFAULT clauses
- The stored missing value will be used for existing rows that don't have the new column
- Updates pg_attribute catalog which triggers relcache rebuild
- Requires RowExclusiveLock on AttributeRelationId for catalog updates

## Simplified Source

```c
void
StoreAttrMissingVal(Relation rel, AttrNumber attnum, Datum missingval)
{
    Datum valuesAtt[Natts_pg_attribute] = {0};
    bool nullsAtt[Natts_pg_attribute] = {0};
    bool replacesAtt[Natts_pg_attribute] = {0};
    Relation attrrel;
    Form_pg_attribute attStruct;
    HeapTuple atttup, newtup;

    // Only supported for plain tables
    Assert(rel->rd_rel->relkind == RELKIND_RELATION);

    // Open pg_attribute catalog for modification
    attrrel = table_open(AttributeRelationId, RowExclusiveLock);

    // Find the attribute tuple in pg_attribute
    atttup = SearchSysCache2(ATTNUM,
                            ObjectIdGetDatum(RelationGetRelid(rel)),
                            Int16GetDatum(attnum));
    if (!HeapTupleIsValid(atttup))
        elog(ERROR, "cache lookup failed for attribute %d of relation %u",
             attnum, RelationGetRelid(rel));

    attStruct = (Form_pg_attribute) GETSTRUCT(atttup);

    // Convert missing value to single-element array
    missingval = PointerGetDatum(construct_array(&missingval, 1,
                                                 attStruct->atttypid,
                                                 attStruct->attlen,
                                                 attStruct->attbyval,
                                                 attStruct->attalign));

    // Update pg_attribute: set atthasmissing = true
    valuesAtt[Anum_pg_attribute_atthasmissing - 1] = BoolGetDatum(true);
    replacesAtt[Anum_pg_attribute_atthasmissing - 1] = true;

    // Update pg_attribute: store the missing value array
    valuesAtt[Anum_pg_attribute_attmissingval - 1] = missingval;
    replacesAtt[Anum_pg_attribute_attmissingval - 1] = true;

    // Create and update the tuple
    newtup = heap_modify_tuple(atttup, RelationGetDescr(attrrel),
                              valuesAtt, nullsAtt, replacesAtt);
    CatalogTupleUpdate(attrrel, &newtup->t_self, newtup);

    // Clean up
    ReleaseSysCache(atttup);
    table_close(attrrel, RowExclusiveLock);
}
```