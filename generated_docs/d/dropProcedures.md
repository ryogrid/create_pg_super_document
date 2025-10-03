# dropProcedures

## Location
[src/backend/commands/opclasscmds.c:1765-1804](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1765-L1804)

## Overview
Removes support procedure entries from an operator family by deleting their corresponding pg_amproc catalog entries using restrictive deletion behavior.

## Definition

```c
static void
dropProcedures(List *opfamilyname, Oid amoid, Oid opfamilyoid,
			   List *procedures)
```
## Detailed Description
This function handles the removal of support procedure (support function) entries from an existing operator family during ALTER OPERATOR FAMILY DROP operations. It processes a list of OpFamilyMember structures representing support procedures to be removed, validates their existence in the pg_amproc catalog, and performs their deletion. Similar to dropOperators, it uses RESTRICT behavior to only allow removal of "loose" members that can be safely deleted without cascading effects. Each support procedure is identified by its support number and operand types within the specified operator family, with comprehensive error reporting for non-existent procedures.

## Parameters / Member Variables
- `*opfamilyname`: List representing the name of the operator family (used for error reporting)
- `amoid`: Object identifier of the access method (currently unused but maintained for consistency)
- `opfamilyoid`: Object identifier of the operator family from which support procedures are being removed
- `*procedures`: List of OpFamilyMember structures specifying the support procedures to be dropped
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
  - [AlterOpFamilyDrop](../A/AlterOpFamilyDrop.md) (src/backend/commands/opclasscmds.c:1096)

## Notes and Other Information
- Only supports RESTRICT deletion behavior, preventing cascading deletions that could affect dependent objects
- Uses the AMPROCNUM system cache to efficiently locate support procedure entries by family, types, and support number
- Provides detailed error messages including procedure signature and family name when procedures don't exist
- The amoid parameter is present for API consistency but not actively used in the current implementation
- Each procedure deletion is performed individually through performDeletion() with proper ObjectAddress configuration
- This function is the procedural counterpart to dropOperators, handling support functions rather than operators
- Designed specifically for "loose" operator family members that can be safely removed without compromising operator family integrity
- Support procedures are critical for index access method functionality, so their removal is carefully controlled

## Simplified Source

```c
static void dropProcedures(List *opfamilyname, Oid amoid, Oid opfamilyoid,
                           List *procedures) {
    ListCell *l;

    // Process each support procedure to be dropped
    foreach(l, procedures) {
        OpFamilyMember *op = (OpFamilyMember *) lfirst(l);
        Oid amprocid;
        ObjectAddress object;

        // Look up procedure in pg_amproc by family, types, and support number
        amprocid = GetSysCacheOid4(AMPROCNUM, Anum_pg_amproc_oid,
                                  ObjectIdGetDatum(opfamilyoid),
                                  ObjectIdGetDatum(op->lefttype),
                                  ObjectIdGetDatum(op->righttype),
                                  Int16GetDatum(op->number));

        // Error if procedure doesn't exist in the family
        if (!OidIsValid(amprocid))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("function %d(%s,%s) does not exist in operator family \"%s\"",
                            op->number,
                            format_type_be(op->lefttype),
                            format_type_be(op->righttype),
                            NameListToString(opfamilyname))));

        // Set up object address and perform deletion
        object.classId = AccessMethodProcedureRelationId;
        object.objectId = amprocid;
        object.objectSubId = 0;

        performDeletion(&object, DROP_RESTRICT, 0);
    }
}
```