# publication_translate_columns

## Location
[src/backend/catalog/pg_publication.c:502-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L502-L569)

## Overview
Translates a list of column names to an array of attribute numbers and validates that each attribute is appropriate for inclusion in a publication column list.

## Definition
```c
static void publication_translate_columns(Relation targetrel, List *columns, int *natts, AttrNumber **attrs)
```

## Detailed Description
This function performs the critical task of converting human-readable column names into internal attribute numbers while enforcing publication column list restrictions. It takes a list of column name strings and produces a sorted array of AttrNumber values that can be stored in the catalog.

The function performs several validation checks:
1. Verifies that each column name actually exists in the relation
2. Prohibits system columns (negative attribute numbers)
3. Prohibits generated columns, which cannot be meaningfully replicated
4. Prevents duplicate columns in the list
5. Sorts the resulting attribute numbers for consistent catalog representation

The function uses a Bitmapset during processing to efficiently check for duplicates, then produces a sorted AttrNumber array as output. If no column list is provided (columns is NULL), the function returns early without setting the output parameters.

## Parameters / Member Variables
- `targetrel`: The relation for which column names are being translated
- `columns`: List of column name strings to translate (can be NULL)
- `natts`: Output parameter - number of attributes in the resulting array  
- `attrs`: Output parameter - pointer to allocated array of attribute numbers

## Dependencies
- Functions called/Symbols referenced:
  - [get_attnum](../g/get_attnum.md)
  - AttrNumberIsForUserDefinedAttr
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [compare_int16](../c/compare_int16.md)
  - qsort
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [publication_add_relation](publication_add_relation.md) (src/backend/catalog/pg_publication.c:403)
  - published_rel (src/backend/catalog/pg_publication.c:51)

## Notes and Other Information
- This is a static function with internal linkage, only accessible within pg_publication.c
- The function allocates memory for the attribute array using palloc() - caller is responsible for cleanup
- Attribute numbers are not offset by FirstLowInvalidHeapAttributeNumber since system columns are forbidden
- The resulting array is always sorted using qsort() with compare_int16 for consistent catalog representation
- Additional validation regarding replica identity is performed later by other functions like pub_collist_contains_invalid_column
- Uses a temporary Bitmapset for efficient duplicate detection, which is freed before returning
- Error messages provide specific details about which column and relation caused the problem
- Location: src/backend/catalog/pg_publication.c:502-569

## Simplified Source

```c
static void
publication_translate_columns(Relation targetrel, List *columns,
                             int *natts, AttrNumber **attrs)
{
    AttrNumber *attarray = NULL;
    Bitmapset  *set = NULL;
    ListCell   *lc;
    int         n = 0;
    TupleDesc   tupdesc = RelationGetDescr(targetrel);

    // Early return if no column list provided
    if (!columns)
        return;

    // Allocate array to hold attribute numbers
    attarray = palloc(sizeof(AttrNumber) * list_length(columns));

    // Process each column name
    foreach(lc, columns)
    {
        char       *colname = strVal(lfirst(lc));
        AttrNumber  attnum = get_attnum(RelationGetRelid(targetrel), colname);

        // Check column exists
        if (attnum == InvalidAttrNumber)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                     errmsg("column \"%s\" of relation \"%s\" does not exist",
                            colname, RelationGetRelationName(targetrel))));

        // System columns not allowed
        if (!AttrNumberIsForUserDefinedAttr(attnum))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                     errmsg("cannot use system column \"%s\" in publication column list",
                            colname)));

        // Generated columns not allowed
        if (TupleDescAttr(tupdesc, attnum - 1)->attgenerated)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                     errmsg("cannot use generated column \"%s\" in publication column list",
                            colname)));

        // Check for duplicates
        if (bms_is_member(attnum, set))
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                     errmsg("duplicate column \"%s\" in publication column list",
                            colname)));

        // Add to set and array
        set = bms_add_member(set, attnum);
        attarray[n++] = attnum;
    }

    // Sort for consistent catalog representation
    qsort(attarray, n, sizeof(AttrNumber), compare_int16);

    // Set output parameters
    *natts = n;
    *attrs = attarray;

    bms_free(set);
}
```