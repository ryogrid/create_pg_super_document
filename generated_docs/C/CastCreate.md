# CastCreate

## Location
[src/backend/catalog/pg_cast.c:49-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_cast.c#L49-L138)

## Overview
Creates a new type cast in the PostgreSQL catalog by forming and inserting tuples into pg_cast, along with proper dependency tracking for all related objects.

## Definition

```c
ObjectAddress
CastCreate(Oid sourcetypeid, Oid targettypeid,
		   Oid funcid, Oid incastid, Oid outcastid,
		   char castcontext, char castmethod, DependencyType behavior)
```
## Detailed Description
CastCreate is responsible for creating a new cast entry in the PostgreSQL system catalog. It performs several critical operations: validates that the cast doesn't already exist, assigns a new OID, creates the catalog tuple with all necessary attributes, and establishes dependency relationships between the cast and its dependent objects (source type, target type, cast function, and any required intermediate casts). The function handles both function-based and binary-compatible casts, ensuring proper dependency tracking for automatic cleanup when dependent objects are dropped.

## Parameters / Member Variables
- : OID of the source data type being cast from
- : OID of the target data type being cast to  
- : OID of the cast function (InvalidOid for binary coercible casts)
- : OID of input cast required for binary coercibility (InvalidOid if none)
- : OID of output cast required for binary coercibility (InvalidOid if none)
- : Context in which the cast can be invoked ('e' = explicit, 'a' = assignment, 'i' = implicit)
- : Method of casting ('f' = function, 'i' = inout, 'b' = binary compatible)
- : Dependency type for relationships with referenced objects

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)  
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - ObjectAddressSet
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - InvokeObjectPostCreateHook
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [CreateCast](CreateCast.md) (src/backend/commands/functioncmds.c:1777)
  - [DefineRange](../D/DefineRange.md) (src/backend/commands/typecmds.c:1718)

## Notes and Other Information
The function performs duplicate checking before insertion using SearchSysCache2 to provide user-friendly error messages. It creates dependencies not only on the primary objects (source/target types, cast function) but also on any intermediate casts that may be required for binary coercibility. Extension dependencies are automatically recorded, and post-creation hooks are invoked for proper system integration. Memory cleanup is handled through heap_freetuple and proper relation closing.

## Simplified Source
```c
ObjectAddress
CastCreate(Oid sourcetypeid, Oid targettypeid,
           Oid funcid, Oid incastid, Oid outcastid,
           char castcontext, char castmethod, DependencyType behavior)
{
    Relation relation;
    HeapTuple tuple;
    Oid castid;
    Datum values[Natts_pg_cast];
    bool nulls[Natts_pg_cast] = {0};
    ObjectAddress myself, referenced;
    ObjectAddresses *addrs;

    // Open pg_cast catalog for modification
    relation = table_open(CastRelationId, RowExclusiveLock);

    // Check if cast already exists
    tuple = SearchSysCache2(CASTSOURCETARGET,
                           ObjectIdGetDatum(sourcetypeid),
                           ObjectIdGetDatum(targettypeid));
    if (HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                       errmsg("cast from type %s to type %s already exists",
                             format_type_be(sourcetypeid),
                             format_type_be(targettypeid))));

    // Assign new OID and prepare tuple values
    castid = GetNewOidWithIndex(relation, CastOidIndexId, Anum_pg_cast_oid);
    values[Anum_pg_cast_oid - 1] = ObjectIdGetDatum(castid);
    values[Anum_pg_cast_castsource - 1] = ObjectIdGetDatum(sourcetypeid);
    values[Anum_pg_cast_casttarget - 1] = ObjectIdGetDatum(targettypeid);
    values[Anum_pg_cast_castfunc - 1] = ObjectIdGetDatum(funcid);
    values[Anum_pg_cast_castcontext - 1] = CharGetDatum(castcontext);
    values[Anum_pg_cast_castmethod - 1] = CharGetDatum(castmethod);

    // Create and insert catalog tuple
    tuple = heap_form_tuple(RelationGetDescr(relation), values, nulls);
    CatalogTupleInsert(relation, tuple);

    // Set up dependency tracking
    addrs = new_object_addresses();
    ObjectAddressSet(myself, CastRelationId, castid);

    // Add dependencies on source and target types
    ObjectAddressSet(referenced, TypeRelationId, sourcetypeid);
    add_exact_object_address(&referenced, addrs);
    ObjectAddressSet(referenced, TypeRelationId, targettypeid);
    add_exact_object_address(&referenced, addrs);

    // Add dependency on cast function if present
    if (OidIsValid(funcid)) {
        ObjectAddressSet(referenced, ProcedureRelationId, funcid);
        add_exact_object_address(&referenced, addrs);
    }

    // Add dependencies on required intermediate casts
    if (OidIsValid(incastid)) {
        ObjectAddressSet(referenced, CastRelationId, incastid);
        add_exact_object_address(&referenced, addrs);
    }
    if (OidIsValid(outcastid)) {
        ObjectAddressSet(referenced, CastRelationId, outcastid);
        add_exact_object_address(&referenced, addrs);
    }

    // Record all dependencies and cleanup
    record_object_address_dependencies(&myself, addrs, behavior);
    free_object_addresses(addrs);

    recordDependencyOnCurrentExtension(&myself, false);
    InvokeObjectPostCreateHook(CastRelationId, castid, 0);

    heap_freetuple(tuple);
    table_close(relation, RowExclusiveLock);

    return myself;
}
```