# ConversionCreate

## Location
[src/backend/catalog/pg_conversion.c:38-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_conversion.c#L38-L151)

## Overview
Creates a new encoding conversion by adding a tuple to the pg_conversion system catalog, with validation to prevent duplicates and proper dependency tracking.

## Definition
```c
ObjectAddress ConversionCreate(const char *conname, Oid connamespace,
                              Oid conowner,
                              int32 conforencoding, int32 contoencoding,
                              Oid conproc, bool def)
```

## Detailed Description
ConversionCreate is responsible for creating new encoding conversions in PostgreSQL by inserting records into the pg_conversion system catalog table. The function performs comprehensive validation to ensure no duplicate conversions exist, particularly for default conversions. It creates a new OID for the conversion, populates all required catalog fields, and establishes proper dependency relationships with the conversion procedure, namespace, and owner. The function also handles extension dependencies and triggers post-creation hooks for proper system integration.

## Parameters / Member Variables
- `conname`: Name of the conversion to create
- `connamespace`: OID of the namespace where the conversion will be created
- `conowner`: OID of the user who will own the conversion
- `conforencoding`: Source encoding ID for the conversion
- `contoencoding`: Target encoding ID for the conversion  
- `conproc`: OID of the procedure that performs the actual conversion
- `def`: Boolean flag indicating whether this is a default conversion

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists2
  - [FindDefaultConversion](../F/FindDefaultConversion.md)
  - [pg_encoding_to_char](../p/pg_encoding_to_char.md)
  - [namestrcpy](../n/namestrcpy.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - InvokeObjectPostCreateHook
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [CreateConversionCommand](CreateConversionCommand.md)

## Notes and Other Information
- Returns ObjectAddress of the newly created conversion for dependency tracking
- Validates that no conversion with the same name exists in the target namespace
- For default conversions (def=true), ensures no existing default conversion exists for the same encoding pair
- Creates dependencies on the conversion procedure, namespace, owner, and current extension
- Uses RowExclusiveLock on the pg_conversion relation during the operation
- Triggers post-creation hooks to notify other system components of the new conversion

## Simplified Source
```c
ObjectAddress
ConversionCreate(const char *conname, Oid connamespace,
                 Oid conowner,
                 int32 conforencoding, int32 contoencoding,
                 Oid conproc, bool def)
{
    Relation rel;
    HeapTuple tup;
    Oid oid;
    bool nulls[Natts_pg_conversion];
    Datum values[Natts_pg_conversion];
    NameData cname;
    ObjectAddress myself, referenced;

    // Basic validation
    if (!conname)
        elog(ERROR, "no conversion name supplied");

    // Check for duplicate conversion name
    if (SearchSysCacheExists2(CONNAMENSP,
                             PointerGetDatum(conname),
                             ObjectIdGetDatum(connamespace)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                       errmsg("conversion \"%s\" already exists", conname)));

    // For default conversions, check for existing default for this encoding pair
    if (def) {
        if (FindDefaultConversion(connamespace, conforencoding, contoencoding))
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("default conversion for %s to %s already exists",
                                 pg_encoding_to_char(conforencoding),
                                 pg_encoding_to_char(contoencoding))));
    }

    // Open catalog and initialize tuple data
    rel = table_open(ConversionRelationId, RowExclusiveLock);
    for (int i = 0; i < Natts_pg_conversion; i++) {
        nulls[i] = false;
        values[i] = (Datum) NULL;
    }

    // Prepare tuple values
    namestrcpy(&cname, conname);
    oid = GetNewOidWithIndex(rel, ConversionOidIndexId, Anum_pg_conversion_oid);
    values[Anum_pg_conversion_oid - 1] = ObjectIdGetDatum(oid);
    values[Anum_pg_conversion_conname - 1] = NameGetDatum(&cname);
    values[Anum_pg_conversion_connamespace - 1] = ObjectIdGetDatum(connamespace);
    values[Anum_pg_conversion_conowner - 1] = ObjectIdGetDatum(conowner);
    values[Anum_pg_conversion_conforencoding - 1] = Int32GetDatum(conforencoding);
    values[Anum_pg_conversion_contoencoding - 1] = Int32GetDatum(contoencoding);
    values[Anum_pg_conversion_conproc - 1] = ObjectIdGetDatum(conproc);
    values[Anum_pg_conversion_condefault - 1] = BoolGetDatum(def);

    // Create and insert tuple
    tup = heap_form_tuple(rel->rd_att, values, nulls);
    CatalogTupleInsert(rel, tup);

    // Set up object address for dependency tracking
    myself.classId = ConversionRelationId;
    myself.objectId = oid;
    myself.objectSubId = 0;

    // Create dependencies
    referenced.classId = ProcedureRelationId;
    referenced.objectId = conproc;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    referenced.classId = NamespaceRelationId;
    referenced.objectId = connamespace;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    recordDependencyOnOwner(ConversionRelationId, oid, conowner);
    recordDependencyOnCurrentExtension(&myself, false);

    // Cleanup and post-creation hook
    InvokeObjectPostCreateHook(ConversionRelationId, oid, 0);
    heap_freetuple(tup);
    table_close(rel, RowExclusiveLock);

    return myself;
}
```