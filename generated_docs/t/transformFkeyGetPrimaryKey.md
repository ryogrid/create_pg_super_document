# transformFkeyGetPrimaryKey

## Location
[src/backend/commands/tablecmds.c:11945-12043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L11945-L12043)

## Overview
transformFkeyGetPrimaryKey retrieves the names, attribute numbers, types, and index information for the primary key of a referenced table, used when the column list is omitted in a REFERENCES specification.

## Definition

```c
static int
transformFkeyGetPrimaryKey(Relation pkrel, Oid *indexOid,
						   List **attnamelist,
						   int16 *attnums, Oid *atttypids,
						   Oid *opclasses)
```
## Detailed Description
This function automatically determines the primary key columns of a referenced table when creating a foreign key constraint without explicitly specifying the referenced columns. It searches through all indexes on the table to find the primary key index, validates that the primary key is suitable for foreign key references, and extracts detailed information about the primary key columns.

The function performs these key operations:
1. Searches all indexes on the referenced table to find the primary key
2. Validates that the primary key is immediate (not deferrable) per SQL specification
3. Extracts the index OID and operator classes from the primary key index
4. Builds lists of attribute numbers, column names, and type OIDs for all primary key columns
5. Returns the count of primary key columns

The function ensures the primary key is valid and immediate, as deferrable primary keys cannot be used as foreign key targets according to SQL standards.

## Parameters / Member Variables
- : The relation containing the primary key to examine
- : Output parameter for the OID of the primary key index
- : Output parameter for list of primary key column names
- : Output array for attribute numbers of primary key columns
- : Output array for type OIDs of primary key columns
- : Output array for operator class OIDs of primary key columns

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - lfirst_oid
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [list_free](../l/list_free.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [attnumTypeId](../a/attnumTypeId.md)
  - [attnumAttName](../a/attnumAttName.md)
  - [makeString](../m/makeString.md)
  - [pstrdup](../p/pstrdup.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md) (when creating foreign keys without explicit column lists)

## Notes and Other Information
- Only works with immediate (non-deferrable) primary keys per SQL specification
- Assumes primary key indexes cannot contain expressional elements, only simple column references
- Returns the number of columns in the primary key
- All output parameters except pkrel must be provided by the caller
- Used specifically when the REFERENCES clause omits the column list, requiring automatic primary key detection
- Validates that the primary key index is both primary and valid before using it

## Simplified Source

```c
static int
transformFkeyGetPrimaryKey(Relation pkrel, Oid *indexOid,
                          List **attnamelist,
                          int16 *attnums, Oid *atttypids,
                          Oid *opclasses)
{
    List       *indexoidlist;
    ListCell   *indexoidscan;
    HeapTuple   indexTuple = NULL;
    Form_pg_index indexStruct = NULL;
    Datum       indclassDatum;
    oidvector  *indclass;
    int         i;

    // Find primary key index among all table indexes
    *indexOid = InvalidOid;
    indexoidlist = RelationGetIndexList(pkrel);

    foreach(indexoidscan, indexoidlist)
    {
        Oid indexoid = lfirst_oid(indexoidscan);

        indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
        if (!HeapTupleIsValid(indexTuple))
            elog(ERROR, "cache lookup failed for index %u", indexoid);

        indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);

        if (indexStruct->indisprimary && indexStruct->indisvalid)
        {
            // Reject deferrable primary keys per SQL spec
            if (!indexStruct->indimmediate)
                ereport(ERROR,
                       (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot use a deferrable primary key for referenced table \"%s\"",
                               RelationGetRelationName(pkrel))));

            *indexOid = indexoid;
            break;
        }
        ReleaseSysCache(indexTuple);
    }

    list_free(indexoidlist);

    // Ensure we found a primary key
    if (!OidIsValid(*indexOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("there is no primary key for referenced table \"%s\"",
                        RelationGetRelationName(pkrel))));

    // Extract index operator classes
    indclassDatum = SysCacheGetAttrNotNull(INDEXRELID, indexTuple,
                                          Anum_pg_index_indclass);
    indclass = (oidvector *) DatumGetPointer(indclassDatum);

    // Build lists of primary key column information
    *attnamelist = NIL;
    for (i = 0; i < indexStruct->indnkeyatts; i++)
    {
        int pkattno = indexStruct->indkey.values[i];

        attnums[i] = pkattno;
        atttypids[i] = attnumTypeId(pkrel, pkattno);
        opclasses[i] = indclass->values[i];
        *attnamelist = lappend(*attnamelist,
                              makeString(pstrdup(NameStr(*attnumAttName(pkrel, pkattno)))));
    }

    ReleaseSysCache(indexTuple);
    return i;  // Number of primary key columns
}
```