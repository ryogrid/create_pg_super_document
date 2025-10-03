# assignProcTypes

## Location
[src/backend/commands/opclasscmds.c:1203-1391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1203-L1391)

## Overview
Determines and assigns the lefttype/righttype for a support procedure in an operator family, performing extensive validation checks specific to different access methods and procedure types.

## Definition

```c
static void
assignProcTypes(OpFamilyMember *member, Oid amoid, Oid typeoid,
				int opclassOptsProcNum)
```
## Detailed Description
This comprehensive function processes support procedures being added to operator families, validating their signatures and determining their associated data types. It performs specialized validation based on the access method (btree, hash) and procedure number. For btree, it validates comparison functions, sort support functions, in_range functions, and equal image functions. For hash, it validates standard and extended hash functions. It also handles operator class options parsing functions with specific signature requirements. The function automatically infers lefttype/righttype from procedure signatures when not explicitly specified, falling back to the opclass input type.

## Parameters / Member Variables
- `*member`: Pointer to OpFamilyMember structure containing procedure information to be processed
- `amoid`: OID of the access method that will use this procedure
- `typeoid`: OID of the operator class input type, used as fallback for lefttype/righttype
- `opclassOptsProcNum`: Procedure number designated for operator class options parsing functions
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
- [Hash](../H/Hash.md) function 1 must be 1-arg returning int4, function 2 must be 2-arg returning int8
- Automatically infers data types from procedure signatures when possible
- Requires explicit type specification when inference is not possible
- Part of the operator class/family validation and setup infrastructure

## Simplified Source

```c
static void
assignProcTypes(OpFamilyMember *member, Oid amoid, Oid typeoid, int opclassOptsProcNum)
{
    HeapTuple proctup;
    Form_pg_proc procform;

    // Fetch procedure definition
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(member->object));
    if (!HeapTupleIsValid(proctup))
        elog(ERROR, "cache lookup failed for function %u", member->object);
    procform = (Form_pg_proc) GETSTRUCT(proctup);

    // Validate operator class options parsing function
    if (member->number == opclassOptsProcNum) {
        if (OidIsValid(typeoid)) {
            if ((OidIsValid(member->lefttype) && member->lefttype != typeoid) ||
                (OidIsValid(member->righttype) && member->righttype != typeoid))
                ereport(ERROR, "data types must match opclass input type");
        } else {
            if (member->lefttype != member->righttype)
                ereport(ERROR, "left and right data types must match");
        }

        if (procform->prorettype != VOIDOID || procform->pronargs != 1 ||
            procform->proargtypes.values[0] != INTERNALOID)
            ereport(ERROR, "invalid operator class options parsing function");
    }
    // Access method specific validation
    else if (amoid == BTREE_AM_OID) {
        if (member->number == BTORDER_PROC) {
            // Btree comparison function: 2 args returning int4
            if (procform->pronargs != 2)
                ereport(ERROR, "btree comparison functions must have two arguments");
            if (procform->prorettype != INT4OID)
                ereport(ERROR, "btree comparison functions must return integer");

            // Infer types from procedure arguments
            if (!OidIsValid(member->lefttype))
                member->lefttype = procform->proargtypes.values[0];
            if (!OidIsValid(member->righttype))
                member->righttype = procform->proargtypes.values[1];
        }
        else if (member->number == BTSORTSUPPORT_PROC) {
            // Sort support: (internal) returns void
            if (procform->pronargs != 1 || procform->proargtypes.values[0] != INTERNALOID)
                ereport(ERROR, "btree sort support functions must accept internal");
            if (procform->prorettype != VOIDOID)
                ereport(ERROR, "btree sort support functions must return void");
        }
        else if (member->number == BTINRANGE_PROC) {
            // In-range function: 5 args returning bool
            if (procform->pronargs != 5)
                ereport(ERROR, "btree in_range functions must have five arguments");
            if (procform->prorettype != BOOLOID)
                ereport(ERROR, "btree in_range functions must return boolean");

            // Infer types from test-value and offset arguments
            if (!OidIsValid(member->lefttype))
                member->lefttype = procform->proargtypes.values[0];
            if (!OidIsValid(member->righttype))
                member->righttype = procform->proargtypes.values[2];
        }
        else if (member->number == BTEQUALIMAGE_PROC) {
            // Equal image function: 1 arg returning bool, no cross-type
            if (procform->pronargs != 1)
                ereport(ERROR, "btree equal image functions must have one argument");
            if (procform->prorettype != BOOLOID)
                ereport(ERROR, "btree equal image functions must return boolean");
            if (member->lefttype != member->righttype)
                ereport(ERROR, "btree equal image functions must not be cross-type");
        }
    }
    else if (amoid == HASH_AM_OID) {
        if (member->number == HASHSTANDARD_PROC) {
            // Hash function 1: 1 arg returning int4
            if (procform->pronargs != 1)
                ereport(ERROR, "hash function 1 must have one argument");
            if (procform->prorettype != INT4OID)
                ereport(ERROR, "hash function 1 must return integer");
        }
        else if (member->number == HASHEXTENDED_PROC) {
            // Hash function 2: 2 args returning int8
            if (procform->pronargs != 2)
                ereport(ERROR, "hash function 2 must have two arguments");
            if (procform->prorettype != INT8OID)
                ereport(ERROR, "hash function 2 must return bigint");
        }

        // For hash, use first argument type for both left and right
        if (!OidIsValid(member->lefttype))
            member->lefttype = procform->proargtypes.values[0];
        if (!OidIsValid(member->righttype))
            member->righttype = procform->proargtypes.values[0];
    }

    // Default fallback to opclass input type
    if (!OidIsValid(member->lefttype))
        member->lefttype = typeoid;
    if (!OidIsValid(member->righttype))
        member->righttype = typeoid;

    // Ensure types are specified
    if (!OidIsValid(member->lefttype) || !OidIsValid(member->righttype))
        ereport(ERROR, "associated data types must be specified");

    ReleaseSysCache(proctup);
}
```