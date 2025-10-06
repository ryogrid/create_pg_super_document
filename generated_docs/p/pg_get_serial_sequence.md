# pg_get_serial_sequence

## Location
[src/backend/utils/adt/ruleutils.c:2787-2880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2787-L2880)

## Overview
Retrieves the fully qualified name of the sequence associated with a serial or identity column, formatted for use with sequence manipulation functions.

## Definition
```c
Datum pg_get_serial_sequence(PG_FUNCTION_ARGS)
```

## Detailed Description
pg_get_serial_sequence is a PostgreSQL system function that finds the sequence object associated with a specific column in a table, particularly for SERIAL or IDENTITY columns. It takes a table name and column name as parameters, then searches the dependency catalog (pg_depend) to locate the sequence that has an automatic or internal dependency relationship with the specified column.

The function performs several steps: it resolves the table name to an OID, validates that the specified column exists, searches the pg_depend system catalog for dependencies, and filters for sequences with the appropriate dependency type (DEPENDENCY_AUTO for SERIAL columns or DEPENDENCY_INTERNAL for IDENTITY columns). If a matching sequence is found, it returns the sequence's fully qualified name in a format suitable for use with setval(), nextval(), or currval() functions.

## Parameters / Member Variables
- `tablename`: TEXT containing the name of the table (can be schema-qualified)
- `columnname`: TEXT containing the name of the column (treated as double-quoted identifier)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting TEXT arguments)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md) (creates RangeVar from qualified name list)
  - [textToQualifiedNameList](../t/textToQualifiedNameList.md) (parses text into qualified name components)
  - RangeVarGetRelid (resolves RangeVar to relation OID)
  - [text_to_cstring](../t/text_to_cstring.md) (converts TEXT to C string)
  - [get_attnum](../g/get_attnum.md) (gets attribute number for column name)
  - [table_open](../t/table_open.md) (opens system table with lock)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initializes scan keys for system catalog search)
  - [systable_beginscan](../s/systable_beginscan.md) (begins system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (gets next tuple from system scan)
  - [systable_endscan](../s/systable_endscan.md) (ends system catalog scan)
  - [table_close](../t/table_close.md) (closes system table and releases lock)
  - [get_rel_relkind](../g/get_rel_relkind.md) (gets relation kind for OID)
  - [generate_qualified_relation_name](../g/generate_qualified_relation_name.md) (creates qualified name for relation)
  - [string_to_text](../s/string_to_text.md) (converts C string to TEXT)
  - PG_RETURN_TEXT_P (macro for returning TEXT result)
- Called from:
  - SQL function pg_get_serial_sequence() available to users

## Notes and Other Information
- This function is exposed as a SQL-callable system function in PostgreSQL
- Returns NULL if no associated sequence is found or if the column is not a serial/identity column
- The first parameter (table name) is not treated as double-quoted, while the second (column name) is double-quoted
- Searches for both DEPENDENCY_AUTO (SERIAL columns) and DEPENDENCY_INTERNAL (IDENTITY columns)
- Does not lock the target table during lookup to avoid privilege issues
- Uses AccessShareLock when scanning the dependency table
- Located in src/backend/utils/adt/ruleutils.c:2787-2880
- The returned sequence name is fully schema-qualified for unambiguous reference

## Simplified Source

```c
Datum
pg_get_serial_sequence(PG_FUNCTION_ARGS)
{
    text *tablename = PG_GETARG_TEXT_PP(0);
    text *columnname = PG_GETARG_TEXT_PP(1);
    RangeVar *tablerv;
    Oid tableOid;
    char *column;
    AttrNumber attnum;
    Oid sequenceId = InvalidOid;
    Relation depRel;
    ScanKeyData key[3];
    SysScanDesc scan;
    HeapTuple tup;

    // Look up table name and convert to OID
    tablerv = makeRangeVarFromNameList(textToQualifiedNameList(tablename));
    tableOid = RangeVarGetRelid(tablerv, NoLock, false);

    // Get column number
    column = text_to_cstring(columnname);
    attnum = get_attnum(tableOid, column);
    if (attnum == InvalidAttrNumber)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg("column \"%s\" of relation \"%s\" does not exist",
                       column, tablerv->relname)));

    // Search dependency table for sequence with auto/internal dependency
    depRel = table_open(DependRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(tableOid));
    ScanKeyInit(&key[2], Anum_pg_depend_refobjsubid, BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(attnum));

    scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 3, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan)))
    {
        Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

        // Look for sequence dependency (serial or identity column)
        if (deprec->classid == RelationRelationId &&
            deprec->objsubid == 0 &&
            (deprec->deptype == DEPENDENCY_AUTO ||
             deprec->deptype == DEPENDENCY_INTERNAL) &&
            get_rel_relkind(deprec->objid) == RELKIND_SEQUENCE)
        {
            sequenceId = deprec->objid;
            break;
        }
    }

    systable_endscan(scan);
    table_close(depRel, AccessShareLock);

    if (OidIsValid(sequenceId))
    {
        char *result = generate_qualified_relation_name(sequenceId);
        PG_RETURN_TEXT_P(string_to_text(result));
    }

    PG_RETURN_NULL();
}
```