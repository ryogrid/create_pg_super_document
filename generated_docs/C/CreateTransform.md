# CreateTransform

## Location
[src/backend/commands/functioncmds.c:1814-2018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L1814-L2018)

## Overview
Implements the CREATE TRANSFORM command to define data conversion functions between SQL data types and procedural language representations.

## Definition

```c
struct;
```
## Detailed Description
CreateTransform processes CREATE TRANSFORM statements to establish bidirectional data conversion mechanisms between PostgreSQL's SQL data types and procedural language-specific data representations. The function supports creating or replacing transform entries in the pg_transform system catalog.

Key operations include:
1. **Type validation** - Ensures the target type is valid (not pseudo-type or domain)
2. **Permission checks** - Validates ownership/usage rights on type, language, and functions
3. **Function validation** - Verifies transform functions meet strict requirements via check_transform_function
4. **Transform function requirements**:
   - FROM SQL: Must return 'internal' type to pass data to procedural language
   - TO SQL: Must return the transform data type to convert back to SQL
5. **Catalog management** - Handles both new transform creation and replacement of existing transforms
6. **Dependency management** - Records dependencies on type, language, and transform functions
7. **Extension integration** - Properly handles extension membership for transforms

The function supports optional FROM SQL and TO SQL transform functions, allowing unidirectional or bidirectional conversions as needed.

## Parameters / Member Variables
- : CreateTransformStmt structure containing type name, language name, optional FROM SQL function, optional TO SQL function, and replace flag

## Dependencies
- Functions called/Symbols referenced:
  - [typenameTypeId](../t/typenameTypeId.md)
  - [get_typtype](../g/get_typtype.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [get_language_oid](../g/get_language_oid.md)
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)/SearchSysCache2
  - [check_transform_function](../c/check_transform_function.md)
  - [table_open](../t/table_open.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](CatalogTupleUpdate.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility.c:1740)

## Notes and Other Information
- Supports the REPLACE option to update existing transforms without dropping and recreating
- Enforces strict ownership requirements: must own the type and transform functions
- Requires USAGE privilege on type and language, EXECUTE privilege on transform functions
- Transform functions must meet specific signature requirements validated by check_transform_function
- FROM SQL functions convert from SQL type to procedural language representation (return type: internal)
- TO SQL functions convert from procedural language back to SQL type (return type: target type)
- Manages dependencies carefully to ensure proper cleanup when objects are dropped
- Integrates with extension system for proper packaging and dependency management

## Simplified Source

```c
ObjectAddress CreateTransform(CreateTransformStmt *stmt)
{
    Oid typeid, langid, fromsqlfuncid = InvalidOid, tosqlfuncid = InvalidOid;
    Oid transformid;
    Datum values[Natts_pg_transform];
    bool nulls[Natts_pg_transform] = {0};
    bool replaces[Natts_pg_transform] = {0};
    HeapTuple tuple, newtuple;
    Relation relation;
    ObjectAddress myself, referenced;
    ObjectAddresses *addrs;
    bool is_replace = false;

    // Validate and get the target type
    typeid = typenameTypeId(NULL, stmt->type_name);
    char typtype = get_typtype(typeid);

    if (typtype == TYPTYPE_PSEUDO)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("data type %s is a pseudo-type",
                              TypeNameToString(stmt->type_name))));

    if (typtype == TYPTYPE_DOMAIN)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("data type %s is a domain",
                              TypeNameToString(stmt->type_name))));

    // Check type ownership and permissions
    if (!object_ownercheck(TypeRelationId, typeid, GetUserId()))
        aclcheck_error_type(ACLCHECK_NOT_OWNER, typeid);

    if (object_aclcheck(TypeRelationId, typeid, GetUserId(), ACL_USAGE) != ACLCHECK_OK)
        aclcheck_error_type(ACLCHECK_NO_PRIV, typeid);

    // Validate language and permissions
    langid = get_language_oid(stmt->lang, false);

    if (object_aclcheck(LanguageRelationId, langid, GetUserId(), ACL_USAGE) != ACLCHECK_OK)
        aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_LANGUAGE, stmt->lang);

    // Validate FROM SQL function if specified
    if (stmt->fromsql) {
        fromsqlfuncid = LookupFuncWithArgs(OBJECT_FUNCTION, stmt->fromsql, false);

        if (!object_ownercheck(ProcedureRelationId, fromsqlfuncid, GetUserId()))
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FUNCTION,
                          NameListToString(stmt->fromsql->objname));

        if (object_aclcheck(ProcedureRelationId, fromsqlfuncid, GetUserId(), ACL_EXECUTE) != ACLCHECK_OK)
            aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_FUNCTION,
                          NameListToString(stmt->fromsql->objname));

        // Verify FROM SQL function returns 'internal'
        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fromsqlfuncid));
        Form_pg_proc procstruct = (Form_pg_proc) GETSTRUCT(tuple);
        if (procstruct->prorettype != INTERNALOID)
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                           errmsg("return data type of FROM SQL function must be internal")));

        check_transform_function(procstruct);
        ReleaseSysCache(tuple);
    }

    // Validate TO SQL function if specified
    if (stmt->tosql) {
        tosqlfuncid = LookupFuncWithArgs(OBJECT_FUNCTION, stmt->tosql, false);

        if (!object_ownercheck(ProcedureRelationId, tosqlfuncid, GetUserId()))
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FUNCTION,
                          NameListToString(stmt->tosql->objname));

        if (object_aclcheck(ProcedureRelationId, tosqlfuncid, GetUserId(), ACL_EXECUTE) != ACLCHECK_OK)
            aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_FUNCTION,
                          NameListToString(stmt->tosql->objname));

        // Verify TO SQL function returns the transform data type
        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(tosqlfuncid));
        Form_pg_proc procstruct = (Form_pg_proc) GETSTRUCT(tuple);
        if (procstruct->prorettype != typeid)
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                           errmsg("return data type of TO SQL function must be the transform data type")));

        check_transform_function(procstruct);
        ReleaseSysCache(tuple);
    }

    // Prepare tuple data
    values[Anum_pg_transform_trftype - 1] = ObjectIdGetDatum(typeid);
    values[Anum_pg_transform_trflang - 1] = ObjectIdGetDatum(langid);
    values[Anum_pg_transform_trffromsql - 1] = ObjectIdGetDatum(fromsqlfuncid);
    values[Anum_pg_transform_trftosql - 1] = ObjectIdGetDatum(tosqlfuncid);

    relation = table_open(TransformRelationId, RowExclusiveLock);

    // Check if transform already exists
    tuple = SearchSysCache2(TRFTYPELANG, ObjectIdGetDatum(typeid), ObjectIdGetDatum(langid));
    if (HeapTupleIsValid(tuple)) {
        Form_pg_transform form = (Form_pg_transform) GETSTRUCT(tuple);

        if (!stmt->replace)
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("transform for type %s language \"%s\" already exists",
                                  format_type_be(typeid), stmt->lang)));

        // Update existing transform
        replaces[Anum_pg_transform_trffromsql - 1] = true;
        replaces[Anum_pg_transform_trftosql - 1] = true;

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);
        CatalogTupleUpdate(relation, &newtuple->t_self, newtuple);

        transformid = form->oid;
        ReleaseSysCache(tuple);
        is_replace = true;
    }
    else {
        // Create new transform
        transformid = GetNewOidWithIndex(relation, TransformOidIndexId, Anum_pg_transform_oid);
        values[Anum_pg_transform_oid - 1] = ObjectIdGetDatum(transformid);
        newtuple = heap_form_tuple(RelationGetDescr(relation), values, nulls);
        CatalogTupleInsert(relation, newtuple);
    }

    // Clean up old dependencies if replacing
    if (is_replace)
        deleteDependencyRecordsFor(TransformRelationId, transformid, true);

    // Record new dependencies
    addrs = new_object_addresses();
    ObjectAddressSet(myself, TransformRelationId, transformid);

    // Dependencies on language, type, and functions
    ObjectAddressSet(referenced, LanguageRelationId, langid);
    add_exact_object_address(&referenced, addrs);

    ObjectAddressSet(referenced, TypeRelationId, typeid);
    add_exact_object_address(&referenced, addrs);

    if (OidIsValid(fromsqlfuncid)) {
        ObjectAddressSet(referenced, ProcedureRelationId, fromsqlfuncid);
        add_exact_object_address(&referenced, addrs);
    }
    if (OidIsValid(tosqlfuncid)) {
        ObjectAddressSet(referenced, ProcedureRelationId, tosqlfuncid);
        add_exact_object_address(&referenced, addrs);
    }

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    // Record extension dependency
    recordDependencyOnCurrentExtension(&myself, is_replace);

    InvokeObjectPostCreateHook(TransformRelationId, transformid, 0);
    heap_freetuple(newtuple);
    table_close(relation, RowExclusiveLock);

    return myself;
}
```