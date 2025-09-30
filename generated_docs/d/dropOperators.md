# dropOperators

## Location
[src/backend/commands/opclasscmds.c:1725-1764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1725-L1764)

## Overview
Removes operator entries from an operator family by deleting their corresponding pg_amop catalog entries using restrictive deletion behavior.

## Definition

```c
static void
dropOperators(List *opfamilyname, Oid amoid, Oid opfamilyoid,
			  List *operators)
```
## Detailed Description
This function handles the removal of operator entries from an existing operator family during ALTER OPERATOR FAMILY DROP operations. It processes a list of OpFamilyMember structures representing operators to be removed, validates their existence in the pg_amop catalog, and performs their deletion. The function uses RESTRICT behavior, meaning it only allows removal of "loose" members that can be safely deleted without cascading effects. Each operator is identified by its strategy number and operand types within the specified operator family, and proper error reporting is provided if an operator doesn't exist.

## Parameters / Member Variables
- : List representing the name of the operator family (used for error reporting)
- : Object identifier of the access method (currently unused but maintained for consistency)
- : Object identifier of the operator family from which operators are being removed
- : List of OpFamilyMember structures specifying the operators to be dropped

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid4
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [Int16GetDatum](../I/Int16GetDatum.md)
  - OidIsValid
  - ereport
  - [format_type_be](../f/format_type_be.md)
  - [NameListToString](../N/NameListToString.md)
  - [performDeletion](../p/performDeletion.md)
- Called from (representative examples):
  - [AlterOpFamilyDrop](../A/AlterOpFamilyDrop.md) (src/backend/commands/opclasscmds.c:1095)

## Notes and Other Information
- Only supports RESTRICT deletion behavior, which prevents cascading deletions that could affect dependent objects
- Uses the AMOPSTRATEGY system cache to efficiently locate operator entries by family, types, and strategy number
- Provides detailed error messages including operator signature and family name when operators don't exist
- The amoid parameter is present for API consistency but not actively used in the current implementation
- Each operator deletion is performed individually through performDeletion() with appropriate ObjectAddress setup
- This function is specifically designed for "loose" operator family members that can be safely removed without affecting the structural integrity of the operator family

## Simplified Source

```c
static void dropOperators(List *opfamilyname, Oid amoid, Oid opfamilyoid,
                          List *operators) {
    ListCell *l;

    // Process each operator to be dropped
    foreach(l, operators) {
        OpFamilyMember *op = (OpFamilyMember *) lfirst(l);
        Oid amopid;
        ObjectAddress object;

        // Look up operator in pg_amop by family, types, and strategy number
        amopid = GetSysCacheOid4(AMOPSTRATEGY, Anum_pg_amop_oid,
                                ObjectIdGetDatum(opfamilyoid),
                                ObjectIdGetDatum(op->lefttype),
                                ObjectIdGetDatum(op->righttype),
                                Int16GetDatum(op->number));

        // Error if operator doesn't exist in the family
        if (!OidIsValid(amopid))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("operator %d(%s,%s) does not exist in operator family \"%s\"",
                            op->number,
                            format_type_be(op->lefttype),
                            format_type_be(op->righttype),
                            NameListToString(opfamilyname))));

        // Set up object address and perform deletion
        object.classId = AccessMethodOperatorRelationId;
        object.objectId = amopid;
        object.objectSubId = 0;

        performDeletion(&object, DROP_RESTRICT, 0);
    }
}
```