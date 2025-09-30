# ParameterAclCreate

## Location
[src/backend/catalog/pg_parameter_acl.c:68-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_parameter_acl.c#L68-L110)

## Overview
ParameterAclCreate adds a new tuple to the pg_parameter_acl system catalog, creating an ACL entry for a configuration parameter with a null (default) access control list.

## Definition
Oid ParameterAclCreate(const char *parameter)

## Detailed Description
This function creates a new entry in the pg_parameter_acl catalog for the specified configuration parameter. It first validates that the parameter name is suitable for ACL entries using check_GUC_name_for_parameter_acl, then converts the name to the standardized form. The function opens the pg_parameter_acl relation with RowExclusiveLock, generates a new OID using GetNewOidWithIndex, and creates a tuple with the parameter name and a null ACL field. The tuple is inserted into the catalog using CatalogTupleInsert. The function relies on the unique index to prevent duplicate entries rather than taking stronger locks, and maintains the lock until transaction commit for consistency.

## Parameters / Member Variables
- : The name of the configuration parameter for which to create an ACL entry (caller should verify no entry exists)

## Dependencies
- Functions called/Symbols referenced:
  - [check_GUC_name_for_parameter_acl](../c/check_GUC_name_for_parameter_acl.md)
  - [convert_GUC_name_for_parameter_acl](../c/convert_GUC_name_for_parameter_acl.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [cstring_to_text](../c/cstring_to_text.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [objectNamesToOids](../o/objectNamesToOids.md)

## Notes and Other Information
- Creates entries with null ACL fields, representing default permissions
- Uses RowExclusiveLock on the pg_parameter_acl relation during insertion
- Prevents cluttering the catalog by validating parameter names before insertion
- Relies on unique index constraints rather than stronger locking to handle concurrent insertions
- Maintains catalog locks until transaction commit for consistency
- Returns the newly created entry's OID for further reference
- Part of PostgreSQL's parameter-level access control infrastructure introduced for fine-grained security

## Simplified Source

```c
Oid
ParameterAclCreate(const char *parameter)
{
    Oid parameterId;
    char *parname;
    Relation rel;
    TupleDesc tupDesc;
    HeapTuple tuple;
    Datum values[Natts_pg_parameter_acl] = {0};
    bool nulls[Natts_pg_parameter_acl] = {0};

    // Validate parameter name for ACL entry
    check_GUC_name_for_parameter_acl(parameter);

    // Convert name to standardized form
    parname = convert_GUC_name_for_parameter_acl(parameter);

    // Open parameter ACL catalog for insertion
    rel = table_open(ParameterAclRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(rel);

    // Generate new OID for this parameter ACL entry
    parameterId = GetNewOidWithIndex(rel,
                                     ParameterAclOidIndexId,
                                     Anum_pg_parameter_acl_oid);

    // Build tuple with parameter name and null ACL
    values[Anum_pg_parameter_acl_oid - 1] = ObjectIdGetDatum(parameterId);
    values[Anum_pg_parameter_acl_parname - 1] =
        PointerGetDatum(cstring_to_text(parname));
    nulls[Anum_pg_parameter_acl_paracl - 1] = true;

    // Insert new ACL entry
    tuple = heap_form_tuple(tupDesc, values, nulls);
    CatalogTupleInsert(rel, tuple);

    // Cleanup
    heap_freetuple(tuple);
    table_close(rel, NoLock);

    return parameterId;
}
```