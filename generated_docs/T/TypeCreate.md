# TypeCreate

## Location
[src/backend/catalog/pg_type.c:195-556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_type.c#L195-L556)

## Overview
TypeCreate is the comprehensive function that creates a fully-defined type entry in the pg_type system catalog, handling both new type creation and updating existing shell types with complete type definitions.

## Definition

```c
ObjectAddress
TypeCreate(Oid newTypeOid,
		   const char *typeName,
		   Oid typeNamespace,
		   Oid relationOid,		/* only for relation rowtypes */
		   char relationKind,	/* ditto */
		   Oid ownerId,
		   int16 internalSize,
		   char typeType,
		   char typeCategory,
		   bool typePreferred,
		   char typDelim,
		   Oid inputProcedure,
		   Oid outputProcedure,
		   Oid receiveProcedure,
		   Oid sendProcedure,
		   Oid typmodinProcedure,
		   Oid typmodoutProcedure,
		   Oid analyzeProcedure,
		   Oid subscriptProcedure,
		   Oid elementType,
		   bool isImplicitArray,
		   Oid arrayType,
		   Oid baseType,
		   const char *defaultTypeValue,	/* human-readable rep */
		   char *defaultTypeBin,	/* cooked rep */
		   bool passedByValue,
		   char alignment,
		   char storage,
		   int32 typeMod,
		   int32 typNDims,		/* Array dimensions for baseType */
		   bool typeNotNull,
		   Oid typeCollation)
```
## Detailed Description
TypeCreate is the core function responsible for creating complete type definitions in PostgreSQL's type system. It performs extensive validation of type parameters, ensures consistency between size, alignment, and pass-by-value semantics, and handles both new type creation and shell type completion.

The function validates that internal size specifications are appropriate (positive for fixed-length, -1 for varlena, -2 for cstring), checks alignment constraints match the type's characteristics, and ensures storage options are compatible with the type's structure. For pass-by-value types, it enforces strict rules about supported sizes (char, int16, int32, or Datum on 64-bit systems) and their corresponding alignments.

The function can operate in two modes: creating a completely new type or updating an existing shell type. When updating a shell type (created by TypeShellMake), it verifies ownership consistency and replaces placeholder values with actual type specifications. It handles dependent types (implicit arrays, multirange types, relation rowtypes) differently by not creating ACL entries and managing dependencies through the related objects.

## Parameters
- : Specific OID to use for the type (0 for auto-assignment)
- : Name of the type being created
- : OID of the schema containing the type
- : OID of related relation (for composite types)
- : Kind of related relation (for composite types)
- : OID of the type owner
- : Internal storage size (-2=cstring, -1=varlena, >0=fixed)
- : Type category (base, composite, domain, enum, etc.)
- : General category for type (numeric, string, etc.)
- : Whether this is the preferred type in its category
- : Array element delimiter character
- : OID of input function (text to internal)
- : OID of output function (internal to text)
- : OID of binary input function
- : OID of binary output function
- : OID of type modifier input function
- : OID of type modifier output function
- : OID of analyze function for statistics
- : OID of subscripting function
- : OID of array element type (for array types)
- : Whether this is an implicitly created array type
- : OID of corresponding array type
- : OID of base type (for domains)
- : Human-readable default value
- : Binary representation of default value
- : Whether values are passed by value or reference
- : Storage alignment requirement (char, short, int, double)
- : TOAST storage strategy (plain, external, extended, main)
- : Type-specific modifier value
- : Number of array dimensions (for domains over arrays)
- : Whether type has NOT NULL constraint
- : OID of collation for the type

## Dependencies
- Functions called/Symbols referenced:
  - [namestrcpy](../n/namestrcpy.md), NameGetDatum, Int16GetDatum, CharGetDatum, BoolGetDatum, ObjectIdGetDatum, Int32GetDatum
  - CStringGetTextDatum, PointerGetDatum
  - [get_user_default_acl](../g/get_user_default_acl.md)
  - [table_open](../t/table_open.md), table_close
  - SearchSysCacheCopy2
  - [heap_modify_tuple](../h/heap_modify_tuple.md), heap_form_tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md), CatalogTupleInsert
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - IsBootstrapProcessingMode
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md)
  - [stringToNode](../s/stringToNode.md)
  - InvokeObjectPostCreateHook
  - ObjectAddressSet
  - [aclcheck_error](../a/aclcheck_error.md)
- Called from (representative examples):
  - [AddNewRelationType](../A/AddNewRelationType.md) (src/backend/catalog/heap.c:1036)
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md) (src/backend/catalog/heap.c:1359)
  - [DefineType](../D/DefineType.md) (src/backend/commands/typecmds.c:573, 615)
  - [DefineDomain](../D/DefineDomain.md) (src/backend/commands/typecmds.c:1024, 1065)
  - [DefineEnum](../D/DefineEnum.md) (src/backend/commands/typecmds.c:1187, 1228)
  - [DefineRange](../D/DefineRange.md) (src/backend/commands/typecmds.c:1529, 1596, 1639, 1678)

## Notes and Other Information
- Performs comprehensive validation of type size, alignment, and pass-by-value consistency
- Handles both new type creation and shell type completion in a single interface
- Supports binary upgrade mode with predetermined OIDs
- Manages dependent type relationships (arrays, multirange, composite types) with special dependency handling
- Only varlena types (internal size -1) can use non-PLAIN storage strategies for TOAST
- Pass-by-value types are restricted to specific sizes with matching alignment requirements
- Creates appropriate ACL entries except for dependent types which inherit permissions
- The function sets  to true, marking the type as fully defined and usable

## Simplified Source

```c
ObjectAddress
TypeCreate(Oid newTypeOid,
           const char *typeName,
           Oid typeNamespace,
           Oid relationOid,
           char relationKind,
           Oid ownerId,
           int16 internalSize,
           char typeType,
           char typeCategory,
           bool typePreferred,
           char typDelim,
           Oid inputProcedure,
           Oid outputProcedure,
           Oid receiveProcedure,
           Oid sendProcedure,
           Oid typmodinProcedure,
           Oid typmodoutProcedure,
           Oid analyzeProcedure,
           Oid subscriptProcedure,
           Oid elementType,
           bool isImplicitArray,
           Oid arrayType,
           Oid baseType,
           const char *defaultTypeValue,
           char *defaultTypeBin,
           bool passedByValue,
           char alignment,
           char storage,
           int32 typeMod,
           int32 typNDims,
           bool typeNotNull,
           Oid typeCollation) {

    Relation pg_type_desc;
    Oid typeObjectId;
    bool isDependentType;
    bool rebuildDeps = false;
    Acl *typacl;
    HeapTuple tup;
    bool nulls[Natts_pg_type];
    bool replaces[Natts_pg_type];
    Datum values[Natts_pg_type];
    NameData name;
    ObjectAddress address;

    // Validate size specifications
    if (!(internalSize > 0 || internalSize == -1 || internalSize == -2))
        ereport(ERROR, "invalid type internal size");

    // Validate pass-by-value types and alignment
    if (passedByValue) {
        validate_pass_by_value_alignment(internalSize, alignment);
    } else {
        // Validate varlena and cstring alignment
        validate_reference_type_alignment(internalSize, alignment);
    }

    // Only varlena types can be toasted
    if (storage != TYPSTORAGE_PLAIN && internalSize != -1)
        ereport(ERROR, "fixed-size types must have storage PLAIN");

    // Determine if this is a dependent type
    isDependentType = isImplicitArray ||
                     typeType == TYPTYPE_MULTIRANGE ||
                     (OidIsValid(relationOid) && relationKind != RELKIND_COMPOSITE_TYPE);

    // Initialize arrays for tuple creation
    initialize_tuple_arrays(nulls, replaces, values);

    // Set all the type attribute values
    setup_type_values(values, nulls, typeName, typeNamespace, ownerId,
                      internalSize, typeType, typeCategory, typePreferred,
                      typDelim, relationOid, subscriptProcedure, elementType,
                      arrayType, inputProcedure, outputProcedure,
                      receiveProcedure, sendProcedure, typmodinProcedure,
                      typmodoutProcedure, analyzeProcedure, alignment,
                      storage, typeNotNull, baseType, typeMod, typNDims,
                      typeCollation, defaultTypeBin, defaultTypeValue);

    // Set up ACL for non-dependent types
    if (isDependentType) {
        typacl = NULL;
    } else {
        typacl = get_user_default_acl(OBJECT_TYPE, ownerId, typeNamespace);
    }

    if (typacl != NULL)
        values[Anum_pg_type_typacl - 1] = PointerGetDatum(typacl);
    else
        nulls[Anum_pg_type_typacl - 1] = true;

    // Open pg_type catalog
    pg_type_desc = table_open(TypeRelationId, RowExclusiveLock);

    // Check if type already exists (as shell type)
    tup = SearchSysCacheCopy2(TYPENAMENSP,
                              CStringGetDatum(typeName),
                              ObjectIdGetDatum(typeNamespace));

    if (HeapTupleIsValid(tup)) {
        // Update existing shell type
        Form_pg_type typform = (Form_pg_type) GETSTRUCT(tup);

        if (typform->typisdefined)
            ereport(ERROR, "type already exists");

        if (typform->typowner != ownerId)
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TYPE, typeName);

        // Update the shell type
        replaces[Anum_pg_type_oid - 1] = false;
        tup = heap_modify_tuple(tup, RelationGetDescr(pg_type_desc),
                               values, nulls, replaces);
        CatalogTupleUpdate(pg_type_desc, &tup->t_self, tup);
        typeObjectId = typform->oid;
        rebuildDeps = true;
    } else {
        // Create new type entry
        if (OidIsValid(newTypeOid))
            typeObjectId = newTypeOid;
        else if (IsBinaryUpgrade)
            typeObjectId = binary_upgrade_next_pg_type_oid;
        else
            typeObjectId = GetNewOidWithIndex(pg_type_desc, TypeOidIndexId, Anum_pg_type_oid);

        values[Anum_pg_type_oid - 1] = ObjectIdGetDatum(typeObjectId);
        tup = heap_form_tuple(RelationGetDescr(pg_type_desc), values, nulls);
        CatalogTupleInsert(pg_type_desc, tup);
    }

    // Create dependencies unless in bootstrap mode
    if (!IsBootstrapProcessingMode()) {
        GenerateTypeDependencies(tup, pg_type_desc,
                                defaultTypeBin ? stringToNode(defaultTypeBin) : NULL,
                                typacl, relationKind, isImplicitArray,
                                isDependentType, true, rebuildDeps);
    }

    // Post-creation hook
    InvokeObjectPostCreateHook(TypeRelationId, typeObjectId, 0);
    ObjectAddressSet(address, TypeRelationId, typeObjectId);

    table_close(pg_type_desc, RowExclusiveLock);
    return address;
}
```