# ConversionCreate

## Location
src/backend/catalog/pg_conversion.c: 38 - 151

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
  - pg_encoding_to_char
  - namestrcpy
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