# storeOperators

## Location
[src/backend/commands/opclasscmds.c:1429-1558](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1429-L1558)

## Overview
Stores operator family members into the pg_amop system catalog and creates all necessary dependency relationships for proper database object management.

## Definition

```c
static void
storeOperators(List *opfamilyname, Oid amoid, Oid opfamilyoid,
			   List *operators, bool isAdd)
```
## Detailed Description
This function persists operator family members to the pg_amop catalog table, which stores the association between operators and operator families. It handles both search and ordering operators, determining the purpose based on the presence of a sort family. The function creates comprehensive dependency records to track relationships between the pg_amop entry and the referenced operator, operator class/family, data types, and sort family. It includes conflict detection when adding to existing families and invokes post-creation hooks. The dependency strength (NORMAL, INTERNAL, AUTO) is determined by the ref_is_hard flag and object type.

## Parameters / Member Variables
- `*opfamilyname`: List representation of the operator family name for error reporting
- `amoid`: OID of the access method associated with this operator family
- `opfamilyoid`: OID of the operator family receiving the operators
- `*operators`: List of OpFamilyMember structures representing operators to store
- `isAdd`: Boolean indicating if this is an addition to an existing family (enables conflict checking)
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [OpFamilyMember](../O/OpFamilyMember.md) (type)
  - SearchSysCacheExists4
  - [Int16GetDatum](../I/Int16GetDatum.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [format_type_be](../f/format_type_be.md)
  - [NameListToString](../N/NameListToString.md)
  - OidIsValid
  - AMOP_ORDER
  - AMOP_SEARCH
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [CharGetDatum](../C/CharGetDatum.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - DEPENDENCY_NORMAL
  - DEPENDENCY_AUTO
  - DEPENDENCY_INTERNAL
  - [typeDepNeeded](../t/typeDepNeeded.md)
  - InvokeObjectPostCreateHook
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [DefineOpClass](../D/DefineOpClass.md)
  - [AlterOpFamilyAdd](../A/AlterOpFamilyAdd.md)

## Notes and Other Information
- Creates entries in pg_amop with all required attributes including strategy number and purpose
- Establishes four types of dependencies: operator, class/family, left/right types, and sort family
- Differentiates between search operators (return boolean) and ordering operators (have sortfamily)
- Uses RowExclusiveLock to ensure concurrent access safety during catalog modifications
- Includes conflict detection to provide clear error messages for duplicate operator definitions
- Part of the operator class/family storage and dependency management infrastructure
- Invokes object creation hooks to notify other subsystems of new pg_amop entries
- Uses typeDepNeeded helper to determine if type dependencies should be created

## Simplified Source

```c
static void
storeOperators(List *opfamilyname, Oid amoid, Oid opfamilyoid,
               List *operators, bool isAdd)
{
    Relation rel = table_open(AccessMethodOperatorRelationId, RowExclusiveLock);

    foreach(ListCell *l, operators) {
        OpFamilyMember *op = (OpFamilyMember *) lfirst(l);

        // Check for conflicts when adding to existing family
        if (isAdd && SearchSysCacheExists4(AMOPSTRATEGY,
                                          ObjectIdGetDatum(opfamilyoid),
                                          ObjectIdGetDatum(op->lefttype),
                                          ObjectIdGetDatum(op->righttype),
                                          Int16GetDatum(op->number))) {
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("operator %d(%s,%s) already exists in operator family \"%s\"",
                                  op->number, format_type_be(op->lefttype),
                                  format_type_be(op->righttype),
                                  NameListToString(opfamilyname))));
        }

        // Determine operator purpose: search or ordering
        char oppurpose = OidIsValid(op->sortfamily) ? AMOP_ORDER : AMOP_SEARCH;

        // Create pg_amop entry with all required fields
        Oid entryoid = GetNewOidWithIndex(rel, AccessMethodOperatorOidIndexId, Anum_pg_amop_oid);
        // ... populate values array and insert tuple ...
        CatalogTupleInsert(rel, tup);

        // Create dependency relationships
        ObjectAddress myself = {AccessMethodOperatorRelationId, entryoid, 0};

        // Operator dependency
        recordDependencyOn(&myself,
                          &(ObjectAddress){OperatorRelationId, op->object, 0},
                          op->ref_is_hard ? DEPENDENCY_NORMAL : DEPENDENCY_AUTO);

        // Class/family dependency
        recordDependencyOn(&myself,
                          &(ObjectAddress){op->ref_is_family ? OperatorFamilyRelationId : OperatorClassRelationId,
                                          op->refobjid, 0},
                          op->ref_is_hard ? DEPENDENCY_INTERNAL : DEPENDENCY_AUTO);

        // Type dependencies (if needed)
        if (typeDepNeeded(op->lefttype, op)) {
            recordDependencyOn(&myself, &(ObjectAddress){TypeRelationId, op->lefttype, 0},
                              op->ref_is_hard ? DEPENDENCY_NORMAL : DEPENDENCY_AUTO);
        }

        if (op->lefttype != op->righttype && typeDepNeeded(op->righttype, op)) {
            recordDependencyOn(&myself, &(ObjectAddress){TypeRelationId, op->righttype, 0},
                              op->ref_is_hard ? DEPENDENCY_NORMAL : DEPENDENCY_AUTO);
        }

        // Sort family dependency for ordering operators
        if (OidIsValid(op->sortfamily)) {
            recordDependencyOn(&myself, &(ObjectAddress){OperatorFamilyRelationId, op->sortfamily, 0},
                              op->ref_is_hard ? DEPENDENCY_NORMAL : DEPENDENCY_AUTO);
        }

        InvokeObjectPostCreateHook(AccessMethodOperatorRelationId, entryoid, 0);
    }

    table_close(rel, RowExclusiveLock);
}
```