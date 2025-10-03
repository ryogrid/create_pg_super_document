# transformFkeyCheckAttrs

## Location
[src/backend/commands/tablecmds.c:12044-12182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L12044-L12182)

## Overview
Validates that the specified columns in a referenced table can support a foreign key constraint by finding a suitable unique index and returning its opclasses.

## Definition

```c
static Oid
transformFkeyCheckAttrs(Relation pkrel,
						int numattrs, int16 *attnums,
						Oid *opclasses)
```
## Detailed Description
This function validates that the specified attribute numbers (columns) in the primary key relation can be properly referenced by a foreign key constraint. It searches through all unique indexes on the referenced table to find one that matches the given columns exactly. The function ensures the foreign key constraint follows SQL standards by rejecting duplicate column references and deferrable unique constraints. Upon finding a suitable index, it populates the caller-provided opclasses array with the operator classes associated with the index columns.

The validation process includes:
- Checking for duplicate column references (forbidden by SQL standard)
- Finding a unique, non-partial, non-expression index that matches the specified columns
- Ensuring the index is not deferrable (per SQL specification)
- Extracting and returning the appropriate operator classes for type compatibility

## Parameters / Member Variables
- `pkrel`: The relation (table) being referenced by the foreign key
- `numattrs`: Number of attributes (columns) in the foreign key
- `*attnums`: Array of attribute numbers representing the referenced columns
- `*opclasses`: Output array to be populated with operator classes from the matching index
## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [heap_attisnull](../h/heap_attisnull.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md)

## Notes and Other Information
- Returns InvalidOid and raises ERROR if no suitable unique index is found
- Specifically rejects deferrable unique constraints per SQL specification
- The function ensures one-to-one column matching between foreign key and unique index
- Handles indexes with columns in any order relative to the foreign key specification
- Part of the foreign key constraint validation process in table alteration commands

## Simplified Source

```c
static Oid transformFkeyCheckAttrs(Relation pkrel, int numattrs, int16 *attnums,
                                  Oid *opclasses)
{
    Oid indexoid = InvalidOid;
    bool found = false;
    bool found_deferrable = false;
    List *indexoidlist;
    ListCell *indexoidscan;
    int i, j;

    // Check for duplicate columns (forbidden by SQL standard)
    for (i = 0; i < numattrs; i++)
    {
        for (j = i + 1; j < numattrs; j++)
        {
            if (attnums[i] == attnums[j])
                ereport(ERROR, (errcode(ERRCODE_INVALID_FOREIGN_KEY),
                    errmsg("foreign key referenced-columns list must not contain duplicates")));
        }
    }

    // Get all indexes for this table
    indexoidlist = RelationGetIndexList(pkrel);

    // Search for a suitable unique index
    foreach(indexoidscan, indexoidlist)
    {
        HeapTuple indexTuple;
        Form_pg_index indexStruct;

        indexoid = lfirst_oid(indexoidscan);
        indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
        if (!HeapTupleIsValid(indexTuple))
            elog(ERROR, "cache lookup failed for index %u", indexoid);
        indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);

        // Check if index is suitable for foreign key reference
        if (indexStruct->indnkeyatts == numattrs &&
            indexStruct->indisunique &&
            indexStruct->indisvalid &&
            heap_attisnull(indexTuple, Anum_pg_index_indpred, NULL) &&
            heap_attisnull(indexTuple, Anum_pg_index_indexprs, NULL))
        {
            Datum indclassDatum;
            oidvector *indclass;

            // Get operator classes from index
            indclassDatum = SysCacheGetAttrNotNull(INDEXRELID, indexTuple,
                                                   Anum_pg_index_indclass);
            indclass = (oidvector *) DatumGetPointer(indclassDatum);

            // Match foreign key columns to index columns (any order)
            for (i = 0; i < numattrs; i++)
            {
                found = false;
                for (j = 0; j < numattrs; j++)
                {
                    if (attnums[i] == indexStruct->indkey.values[j])
                    {
                        opclasses[i] = indclass->values[j];
                        found = true;
                        break;
                    }
                }
                if (!found)
                    break;
            }

            // Reject deferrable unique constraints (per SQL spec)
            if (found && !indexStruct->indimmediate)
            {
                found_deferrable = true;
                found = false;
            }
        }
        ReleaseSysCache(indexTuple);
        if (found)
            break;
    }

    // Report appropriate error if no suitable index found
    if (!found)
    {
        if (found_deferrable)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot use a deferrable unique constraint for referenced table \"%s\"",
                       RelationGetRelationName(pkrel))));
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_FOREIGN_KEY),
                errmsg("there is no unique constraint matching given keys for referenced table \"%s\"",
                       RelationGetRelationName(pkrel))));
    }

    list_free(indexoidlist);
    return indexoid;
}
```