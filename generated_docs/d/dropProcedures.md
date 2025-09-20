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
- : List representing the name of the operator family (used for error reporting)
- : Object identifier of the access method (currently unused but maintained for consistency)
- : Object identifier of the operator family from which support procedures are being removed
- : List of OpFamilyMember structures specifying the support procedures to be dropped

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid4
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - Int16GetDatum
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