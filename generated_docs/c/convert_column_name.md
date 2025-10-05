# convert_column_name

## Location
[src/backend/utils/adt/acl.c:2898-2955](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2898-L2955)

## Overview
A static helper function that converts a column name (as text) to its corresponding attribute number for a given table, with special handling for dropped columns and error cases.

## Definition

```c
static AttrNumber
convert_column_name(Oid tableoid, text *column)
```
## Detailed Description
This function serves as a support routine for the has_column_privilege family of functions. It takes a table OID and a column name as a text string and returns the corresponding attribute number (AttrNumber). The function performs a direct lookup in the system catalog (pg_attribute) using SearchSysCache2 rather than using get_attnum() because it needs to distinguish between dropped columns and nonexistent columns. For dropped columns (where attisdropped is true), it returns InvalidAttrNumber, allowing the caller to return NULL instead of failing. If the column doesn't exist but the table does, it throws an ERRCODE_UNDEFINED_COLUMN error. If the table itself doesn't exist (get_rel_name returns NULL), it returns InvalidAttrNumber to allow graceful handling by the caller.

## Parameters / Member Variables
- `tableoid`: Object identifier (OID) of the table containing the column
- `*column`: Text string representing the name of the column to resolve
## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [get_rel_name](../g/get_rel_name.md)
  - ereport
  - [pfree](../p/pfree.md)
  - InvalidAttrNumber
- Called from (representative examples):
  - [has_column_privilege_name_name](../h/has_column_privilege_name_name.md)
  - [has_column_privilege_id_name](../h/has_column_privilege_id_name.md)
  - Various other has_column_privilege variants

## Notes and Other Information
- Returns InvalidAttrNumber (rather than throwing an error) for dropped columns to allow privilege functions to return NULL
- Uses direct system catalog lookup instead of get_attnum() to handle dropped columns appropriately  
- Distinguishes between nonexistent columns (error) and dropped columns (return InvalidAttrNumber)
- If the table OID is invalid or the table has been dropped, returns InvalidAttrNumber rather than erroring
- Memory management: properly frees the converted C string using pfree()
- Part of the internal support infrastructure for PostgreSQL's column privilege checking system
- Located in src/backend/utils/adt/acl.c:2898-2955

## Simplified Source

```c
static AttrNumber convert_column_name(Oid tableoid, text *column) {
    // Convert text to C string
    char *colname = text_to_cstring(column);

    // Look up column in system catalog
    HeapTuple attTuple = SearchSysCache2(ATTNAME,
                                        ObjectIdGetDatum(tableoid),
                                        CStringGetDatum(colname));

    AttrNumber attnum;
    if (HeapTupleIsValid(attTuple)) {
        Form_pg_attribute attributeForm = (Form_pg_attribute) GETSTRUCT(attTuple);

        // Return InvalidAttrNumber for dropped columns
        if (attributeForm->attisdropped)
            attnum = InvalidAttrNumber;
        else
            attnum = attributeForm->attnum;

        ReleaseSysCache(attTuple);
    } else {
        // Check if table exists
        char *tablename = get_rel_name(tableoid);
        if (tablename != NULL) {
            // Table exists but column doesn't - error
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                     errmsg("column \"%s\" of relation \"%s\" does not exist",
                            colname, tablename)));
        }
        // Table doesn't exist - return InvalidAttrNumber
        attnum = InvalidAttrNumber;
    }

    pfree(colname);
    return attnum;
}
```