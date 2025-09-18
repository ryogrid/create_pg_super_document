# addFamilyMember

## Location
src/backend/commands/opclasscmds.c: 1392 - 1428

## Overview
Adds a new operator or function member to an operator family list while checking for duplicate strategy or procedure numbers.

## Definition


## Detailed Description
This function safely adds a new OpFamilyMember to a list while ensuring uniqueness constraints are maintained. It checks for duplicates by comparing the member number, lefttype, and righttype against existing members in the list. If a duplicate is found, it reports an appropriate error message indicating whether it's a function or operator conflict. The function prevents invalid operator family definitions by enforcing that each strategy/procedure number can only be defined once for a given type combination.

## Parameters / Member Variables
- : Double pointer to the List structure containing OpFamilyMember entries
- : Pointer to the OpFamilyMember structure to be added to the list

## Dependencies
- Functions called/Symbols referenced:
  - OpFamilyMember (type)
  - foreach
  - lfirst
  - ereport
  - errcode
  - errmsg
  - format_type_be
  - lappend
- Called from (representative examples):
  - DefineOpClass
  - AlterOpFamilyAdd
  - AlterOpFamilyDrop

## Notes and Other Information
- Enforces uniqueness constraint: one member per (number, lefttype, righttype) combination
- Provides specific error messages for function vs operator conflicts
- Uses format_type_be to display human-readable type names in error messages
- Essential for maintaining operator family integrity during creation and modification
- Part of the operator class/family management infrastructure
- Supports both function and operator member addition with appropriate validation