# assignOperTypes

## Location
src/backend/commands/opclasscmds.c: 1137 - 1202

## Overview
Determines and assigns the lefttype/righttype for an operator member in an operator family, performing validation checks to ensure the operator is suitable for index operations.

## Definition


## Detailed Description
This function processes an operator that is being added to an operator family, determining its left and right operand types and validating that it meets the requirements for index operations. It fetches the operator definition from the system catalog, enforces that the operator is binary, and performs different validation based on whether it's a search operator or an ordering operator. For search operators, it ensures the return type is boolean. For ordering operators, it verifies that the access method supports ordering operations. If the member's lefttype or righttype are not explicitly specified, it uses the operator's intrinsic input types.

## Parameters / Member Variables
- : Pointer to OpFamilyMember structure containing operator information to be processed
- : OID of the access method that will use this operator
- : OID of the data type (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - OpFamilyMember (type)
  - Operator (type)
  - Form_pg_operator (type)
  - SearchSysCache1
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - ereport
  - errcode
  - errmsg
  - OidIsValid
  - GetIndexAmRoutineByAmId
  - IndexAmRoutine (type)
  - get_am_name
  - ReleaseSysCache
- Called from (representative examples):
  - DefineOpClass
  - AlterOpFamilyAdd

## Notes and Other Information
- Enforces that all opfamily operators must be binary (oprkind = 'b')
- Search operators must return boolean type (BOOLOID)
- Ordering operators require access method support (amcanorderbyop flag)
- Automatically assigns operator's intrinsic input types if not explicitly specified
- Part of the operator class/family validation and setup process
- Uses PostgreSQL's system catalog caching mechanism for efficient operator lookup
- Contains detailed comments about ordering hazards during dump/reload scenarios