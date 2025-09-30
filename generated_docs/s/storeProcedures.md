# storeProcedures

## Location
[src/backend/commands/opclasscmds.c:1559-1674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1559-L1674)

## Overview
Stores operator family support procedures (support routines) into the pg_amproc system catalog and creates the necessary dependency entries in pg_depend.

## Definition

```c
static void
storeProcedures(List *opfamilyname, Oid amoid, Oid opfamilyoid,
				List *procedures, bool isAdd)
```
## Detailed Description
This function is responsible for inserting procedure entries into the pg_amproc catalog table when defining or modifying operator families. It processes a list of OpFamilyMember structures representing support procedures and creates corresponding entries in the system catalog. The function also establishes proper dependency relationships between the procedures and related database objects (operator families/classes, types, and the procedures themselves) to ensure referential integrity. When adding procedures to an existing operator family, it performs conflict checking to prevent duplicate entries and provides meaningful error messages.

## Parameters / Member Variables
- : List representing the name of the operator family (used for error reporting)
- : Object identifier of the access method
- : Object identifier of the operator family these procedures belong to
- : List of OpFamilyMember structures containing procedure information
- : Boolean flag indicating whether procedures are being added to an existing family (true) or created as part of a new family (false)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - SearchSysCacheExists4
  - ereport
  - [format_type_be](../f/format_type_be.md)
  - [NameListToString](../N/NameListToString.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [typeDepNeeded](../t/typeDepNeeded.md)
  - InvokeObjectPostCreateHook
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [DefineOpClass](../D/DefineOpClass.md) (src/backend/commands/opclasscmds.c:711)
  - [AlterOpFamilyAdd](../A/AlterOpFamilyAdd.md) (src/backend/commands/opclasscmds.c:1018)

## Notes and Other Information
- The function uses RowExclusiveLock when opening the pg_amproc relation to ensure exclusive access during modification
- Dependency strength (DEPENDENCY_NORMAL, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL) is determined based on the ref_is_hard flag in the OpFamilyMember structure
- Type dependencies are conditionally created using typeDepNeeded() to avoid unnecessary dependencies for built-in types
- The function generates a new OID for each pg_amproc entry using GetNewOidWithIndex
- Post-creation hooks are invoked to allow extensions to perform additional processing after procedure creation

## Simplified Source

```c
static void
storeProcedures(List *opfamilyname, Oid amoid, Oid opfamilyoid,
                List *procedures, bool isAdd)
{
    Relation rel = table_open(AccessMethodProcedureRelationId, RowExclusiveLock);

    foreach(ListCell *l, procedures) {
        OpFamilyMember *proc = (OpFamilyMember *) lfirst(l);

        // Check for conflicts when adding to existing family
        if (isAdd && SearchSysCacheExists4(AMPROCNUM,
                                          ObjectIdGetDatum(opfamilyoid),
                                          ObjectIdGetDatum(proc->lefttype),
                                          ObjectIdGetDatum(proc->righttype),
                                          Int16GetDatum(proc->number))) {
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("function %d(%s,%s) already exists in operator family \"%s\"",
                                  proc->number, format_type_be(proc->lefttype),
                                  format_type_be(proc->righttype),
                                  NameListToString(opfamilyname))));
        }

        // Create pg_amproc entry with all required fields
        Oid entryoid = GetNewOidWithIndex(rel, AccessMethodProcedureOidIndexId, Anum_pg_amproc_oid);
        // ... populate values array and insert tuple ...
        CatalogTupleInsert(rel, tup);

        // Create dependency relationships
        ObjectAddress myself = {AccessMethodProcedureRelationId, entryoid, 0};

        // Procedure dependency
        recordDependencyOn(&myself,
                          &(ObjectAddress){ProcedureRelationId, proc->object, 0},
                          proc->ref_is_hard ? DEPENDENCY_NORMAL : DEPENDENCY_AUTO);

        // Class/family dependency
        recordDependencyOn(&myself,
                          &(ObjectAddress){proc->ref_is_family ? OperatorFamilyRelationId : OperatorClassRelationId,
                                          proc->refobjid, 0},
                          proc->ref_is_hard ? DEPENDENCY_INTERNAL : DEPENDENCY_AUTO);

        // Type dependencies (if needed)
        if (typeDepNeeded(proc->lefttype, proc)) {
            recordDependencyOn(&myself, &(ObjectAddress){TypeRelationId, proc->lefttype, 0},
                              proc->ref_is_hard ? DEPENDENCY_NORMAL : DEPENDENCY_AUTO);
        }

        if (proc->lefttype != proc->righttype && typeDepNeeded(proc->righttype, proc)) {
            recordDependencyOn(&myself, &(ObjectAddress){TypeRelationId, proc->righttype, 0},
                              proc->ref_is_hard ? DEPENDENCY_NORMAL : DEPENDENCY_AUTO);
        }

        InvokeObjectPostCreateHook(AccessMethodProcedureRelationId, entryoid, 0);
    }

    table_close(rel, RowExclusiveLock);
}
```