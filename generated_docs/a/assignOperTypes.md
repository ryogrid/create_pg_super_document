# assignOperTypes

## Location
[src/backend/commands/opclasscmds.c:1137-1202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1137-L1202)

## Overview
Determines and assigns the lefttype/righttype for an operator member in an operator family, performing validation checks to ensure the operator is suitable for index operations.

## Definition

```c
static void
assignOperTypes(OpFamilyMember *member, Oid amoid, Oid typeoid)
```
## Detailed Description
This function processes an operator that is being added to an operator family, determining its left and right operand types and validating that it meets the requirements for index operations. It fetches the operator definition from the system catalog, enforces that the operator is binary, and performs different validation based on whether it's a search operator or an ordering operator. For search operators, it ensures the return type is boolean. For ordering operators, it verifies that the access method supports ordering operations. If the member's lefttype or righttype are not explicitly specified, it uses the operator's intrinsic input types.

## Parameters / Member Variables
- : Pointer to OpFamilyMember structure containing operator information to be processed
- : OID of the access method that will use this operator
- : OID of the data type (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [OpFamilyMember](../O/OpFamilyMember.md) (type)
  - Operator (type)
  - Form_pg_operator (type)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - OidIsValid
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md)
  - [IndexAmRoutine](../I/IndexAmRoutine.md) (type)
  - [get_am_name](../g/get_am_name.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [DefineOpClass](../D/DefineOpClass.md)
  - [AlterOpFamilyAdd](../A/AlterOpFamilyAdd.md)

## Notes and Other Information
- Enforces that all opfamily operators must be binary (oprkind = 'b')
- Search operators must return boolean type (BOOLOID)
- Ordering operators require access method support (amcanorderbyop flag)
- Automatically assigns operator's intrinsic input types if not explicitly specified
- Part of the operator class/family validation and setup process
- Uses PostgreSQL's system catalog caching mechanism for efficient operator lookup
- Contains detailed comments about ordering hazards during dump/reload scenarios

## Simplified Source

```c
static void
assignOperTypes(OpFamilyMember *member, Oid amoid, Oid typeoid)
{
    Operator optup;
    Form_pg_operator opform;

    // Fetch operator definition from system catalog
    optup = SearchSysCache1(OPEROID, ObjectIdGetDatum(member->object));
    if (!HeapTupleIsValid(optup))
        elog(ERROR, "cache lookup failed for operator %u", member->object);
    opform = (Form_pg_operator) GETSTRUCT(optup);

    // Validate operator is binary
    if (opform->oprkind != 'b')
        ereport(ERROR, "index operators must be binary");

    // Handle ordering vs search operators
    if (OidIsValid(member->sortfamily)) {
        // Ordering operator - check access method supports it
        IndexAmRoutine *amroutine = GetIndexAmRoutineByAmId(amoid, false);
        if (!amroutine->amcanorderbyop)
            ereport(ERROR, "access method does not support ordering operators");
    } else {
        // Search operator - must return boolean
        if (opform->oprresult != BOOLOID)
            ereport(ERROR, "index search operators must return boolean");
    }

    // Set lefttype/righttype from operator if not specified
    if (!OidIsValid(member->lefttype))
        member->lefttype = opform->oprleft;
    if (!OidIsValid(member->righttype))
        member->righttype = opform->oprright;

    ReleaseSysCache(optup);
}
```