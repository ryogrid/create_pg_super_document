# ginadjustmembers

## Location
src/backend/access/gin/ginvalidate.c: 277 - 336

## Overview
Prechecking function that adjusts dependency settings for operators and support functions when adding them to a GIN operator family, ensuring proper dependency relationships are established.

## Definition


## Detailed Description
The `ginadjustmembers` function configures dependency settings for operators and support functions being added to a GIN operator family. It establishes appropriate dependency relationships based on GIN-specific requirements:

**Operator Dependencies**: All GIN operators are assigned soft dependencies pointing to the operator family rather than hard dependencies. This is because their connection to the opfamily depends solely on what the support functions determine, which can be altered.

**Support Function Dependencies**: The function categorizes support functions into required and optional categories:
- Required functions (EXTRACTVALUE, EXTRACTQUERY) get hard dependencies
- Optional functions (COMPARE, CONSISTENT, COMPARE_PARTIAL, TRICONSISTENT, OPTIONS) get soft family dependencies

The dependency configuration ensures proper catalog management and prevents issues during ALTER OPERATOR FAMILY operations.

## Parameters / Member Variables
- `opfamilyoid`: OID of the operator family being modified
- `opclassoid`: OID of the operator class (used for dependency reference)
- `operators`: List of OpFamilyMember structs representing operators to add
- `functions`: List of OpFamilyMember structs representing support functions to add

## Dependencies
- Functions called/Symbols referenced:
  - [OpFamilyMember](../O/OpFamilyMember.md) (struct access)
  - GIN_EXTRACTVALUE_PROC
  - GIN_EXTRACTQUERY_PROC
  - GIN_COMPARE_PROC
  - GIN_CONSISTENT_PROC
  - GIN_COMPARE_PARTIAL_PROC
  - GIN_TRICONSISTENT_PROC
  - GIN_OPTIONS_PROC
  - ereport
- Called from (representative examples):
  - [ginhandler](ginhandler.md) (src/backend/access/gin/ginutil.c:76)

## Notes and Other Information
- This function modifies OpFamilyMember structures in-place by setting dependency flags
- GIN operators never have hard dependencies due to their flexible relationship with support functions
- Required support functions (numbers 2 and 3) are assigned hard dependencies for proper catalog integrity
- Optional support functions are assigned soft family dependencies to allow flexible modification
- Invalid support function numbers trigger an ERROR with ERRCODE_INVALID_OBJECT_DEFINITION
- The function is designed to work with both CREATE OPERATOR CLASS and ALTER OPERATOR FAMILY operations