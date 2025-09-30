# addFamilyMember

## Location
[src/backend/commands/opclasscmds.c:1392-1428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1392-L1428)

## Overview
Adds a new operator or function member to an operator family list while checking for duplicate strategy or procedure numbers.

## Definition

```c
static void
addFamilyMember(List **list, OpFamilyMember *member)
```
## Detailed Description
This function safely adds a new OpFamilyMember to a list while ensuring uniqueness constraints are maintained. It checks for duplicates by comparing the member number, lefttype, and righttype against existing members in the list. If a duplicate is found, it reports an appropriate error message indicating whether it's a function or operator conflict. The function prevents invalid operator family definitions by enforcing that each strategy/procedure number can only be defined once for a given type combination.

## Parameters / Member Variables
- : Double pointer to the List structure containing OpFamilyMember entries
- : Pointer to the OpFamilyMember structure to be added to the list

## Dependencies
- Functions called/Symbols referenced:
  - [OpFamilyMember](../O/OpFamilyMember.md) (type)
  - foreach
  - lfirst
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [format_type_be](../f/format_type_be.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [DefineOpClass](../D/DefineOpClass.md)
  - [AlterOpFamilyAdd](../A/AlterOpFamilyAdd.md)
  - [AlterOpFamilyDrop](../A/AlterOpFamilyDrop.md)

## Notes and Other Information
- Enforces uniqueness constraint: one member per (number, lefttype, righttype) combination
- Provides specific error messages for function vs operator conflicts
- Uses format_type_be to display human-readable type names in error messages
- Essential for maintaining operator family integrity during creation and modification
- Part of the operator class/family management infrastructure
- Supports both function and operator member addition with appropriate validation

## Simplified Source

```c
static void
addFamilyMember(List **list, OpFamilyMember *member)
{
    ListCell *l;

    // Check for duplicates in existing list
    foreach(l, *list) {
        OpFamilyMember *old = (OpFamilyMember *) lfirst(l);

        // Compare member number and type combination
        if (old->number == member->number &&
            old->lefttype == member->lefttype &&
            old->righttype == member->righttype) {

            // Report appropriate error for function vs operator
            if (member->is_func)
                ereport(ERROR,
                        "function number %d for (%s,%s) appears more than once",
                        member->number,
                        format_type_be(member->lefttype),
                        format_type_be(member->righttype));
            else
                ereport(ERROR,
                        "operator number %d for (%s,%s) appears more than once",
                        member->number,
                        format_type_be(member->lefttype),
                        format_type_be(member->righttype));
        }
    }

    // Add member to list if no duplicates found
    *list = lappend(*list, member);
}
```