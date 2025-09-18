# assignProcTypes

## Location
src/backend/commands/opclasscmds.c: 1203 - 1391

## Overview
Determines and assigns the lefttype/righttype for a support procedure in an operator family, performing extensive validation checks specific to different access methods and procedure types.

## Definition


## Detailed Description
This comprehensive function processes support procedures being added to operator families, validating their signatures and determining their associated data types. It performs specialized validation based on the access method (btree, hash) and procedure number. For btree, it validates comparison functions, sort support functions, in_range functions, and equal image functions. For hash, it validates standard and extended hash functions. It also handles operator class options parsing functions with specific signature requirements. The function automatically infers lefttype/righttype from procedure signatures when not explicitly specified, falling back to the opclass input type.

## Parameters / Member Variables
- : Pointer to OpFamilyMember structure containing procedure information to be processed
- : OID of the access method that will use this procedure
- : OID of the operator class input type, used as fallback for lefttype/righttype
- : Procedure number designated for operator class options parsing functions

## Dependencies
- Functions called/Symbols referenced:
  - [OpFamilyMember](../O/OpFamilyMember.md) (type)
  - Form_pg_proc (type)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - OidIsValid
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errhint](../e/errhint.md)
  - BTORDER_PROC
  - BTSORTSUPPORT_PROC
  - BTINRANGE_PROC
  - BTEQUALIMAGE_PROC
  - HASHSTANDARD_PROC
  - HASHEXTENDED_PROC
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [DefineOpClass](../D/DefineOpClass.md)
  - [AlterOpFamilyAdd](../A/AlterOpFamilyAdd.md)

## Notes and Other Information
- Implements access method-specific validation logic for btree and hash indexes
- Operator class options parsing functions must have signature: (internal) RETURNS void
- Btree comparison functions must be 2-arg returning int4
- Btree sort support functions must accept internal and return void
- Btree in_range functions must be 5-arg returning bool
- Btree equal image functions must be 1-arg returning bool and cannot be cross-type
- Hash function 1 must be 1-arg returning int4, function 2 must be 2-arg returning int8
- Automatically infers data types from procedure signatures when possible
- Requires explicit type specification when inference is not possible
- Part of the operator class/family validation and setup infrastructure