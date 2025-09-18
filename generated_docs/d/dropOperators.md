# dropOperators

## Location
src/backend/commands/opclasscmds.c: 1725 - 1764

## Overview
Removes operator entries from an operator family by deleting their corresponding pg_amop catalog entries using restrictive deletion behavior.

## Definition


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
  - ObjectIdGetDatum
  - Int16GetDatum
  - OidIsValid
  - ereport
  - format_type_be
  - NameListToString
  - performDeletion
- Called from (representative examples):
  - AlterOpFamilyDrop (src/backend/commands/opclasscmds.c:1095)

## Notes and Other Information
- Only supports RESTRICT deletion behavior, which prevents cascading deletions that could affect dependent objects
- Uses the AMOPSTRATEGY system cache to efficiently locate operator entries by family, types, and strategy number
- Provides detailed error messages including operator signature and family name when operators don't exist
- The amoid parameter is present for API consistency but not actively used in the current implementation
- Each operator deletion is performed individually through performDeletion() with appropriate ObjectAddress setup
- This function is specifically designed for "loose" operator family members that can be safely removed without affecting the structural integrity of the operator family