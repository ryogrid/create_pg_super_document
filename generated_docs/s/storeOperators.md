# storeOperators

## Location
src/backend/commands/opclasscmds.c: 1429 - 1558

## Overview
Stores operator family members into the pg_amop system catalog and creates all necessary dependency relationships for proper database object management.

## Definition


## Detailed Description
This function persists operator family members to the pg_amop catalog table, which stores the association between operators and operator families. It handles both search and ordering operators, determining the purpose based on the presence of a sort family. The function creates comprehensive dependency records to track relationships between the pg_amop entry and the referenced operator, operator class/family, data types, and sort family. It includes conflict detection when adding to existing families and invokes post-creation hooks. The dependency strength (NORMAL, INTERNAL, AUTO) is determined by the ref_is_hard flag and object type.

## Parameters / Member Variables
- : List representation of the operator family name for error reporting
- : OID of the access method associated with this operator family
- : OID of the operator family receiving the operators
- : List of OpFamilyMember structures representing operators to store
- : Boolean indicating if this is an addition to an existing family (enables conflict checking)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [OpFamilyMember](../O/OpFamilyMember.md) (type)
  - SearchSysCacheExists4
  - Int16GetDatum
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
  - table_close
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