# transformOfType

## Location
[src/backend/parser/parse_utilcmd.c:1461-1513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L1461-L1513)

## Overview
Transforms a CREATE TABLE OF type_name statement by extracting column definitions from the specified composite type and adding them to the table creation context.

## Definition
static void transformOfType(CreateStmtContext *cxt, TypeName *ofTypename)

## Detailed Description
This function handles the OF clause in CREATE TABLE statements, which allows creating a table based on the structure of an existing composite type. It looks up the specified type, validates that it's suitable for table creation (using check_of_type), and then extracts all non-dropped attributes from the type's tuple descriptor. For each attribute, it creates a corresponding ColumnDef and adds it to the table's column list. The resulting table will have the same column names, types, type modifiers, and collations as the source composite type.

## Parameters / Member Variables
- `cxt`: CreateStmtContext containing the accumulated table definition, particularly the columns list that will be populated
- `ofTypename`: TypeName specifying the composite type to base the table structure on

## Dependencies
- Functions called/Symbols referenced:
  - [typenameType](typenameType.md)
  - [check_of_type](../c/check_of_type.md)
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md)
  - [makeColumnDef](../m/makeColumnDef.md)
  - ReleaseTupleDesc
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [transformCreateStmt](transformCreateStmt.md)

## Notes and Other Information
- Only works with composite types that pass the check_of_type validation
- Automatically skips dropped attributes from the source type
- Sets the is_from_type flag on generated column definitions to indicate their origin
- Caches the type OID in the TypeName for later use
- Part of the CREATE TABLE statement transformation pipeline
- Does not copy constraints, defaults, or other table features - only basic column structure

## Simplified Source

```c
static void transformOfType(CreateStmtContext *cxt, TypeName *ofTypename) {
    HeapTuple tuple;
    TupleDesc tupdesc;
    Oid ofTypeId;

    Assert(ofTypename);

    // Look up the composite type
    tuple = typenameType(NULL, ofTypename, NULL);
    check_of_type(tuple);
    ofTypeId = ((Form_pg_type) GETSTRUCT(tuple))->oid;
    ofTypename->typeOid = ofTypeId; // Cache for later

    // Extract column definitions from the type's tuple descriptor
    tupdesc = lookup_rowtype_tupdesc(ofTypeId, -1);
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

        if (attr->attisdropped)
            continue;

        // Create column definition matching the type's attribute
        ColumnDef *n = makeColumnDef(NameStr(attr->attname), attr->atttypid,
                                   attr->atttypmod, attr->attcollation);
        n->is_from_type = true;

        cxt->columns = lappend(cxt->columns, n);
    }

    ReleaseTupleDesc(tupdesc);
    ReleaseSysCache(tuple);
}
```