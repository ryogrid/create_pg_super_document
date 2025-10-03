# ExecFindJunkAttributeInTlist

## Location
[src/backend/executor/execJunk.c:222-246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execJunk.c#L222-L246)

## Overview
Searches through a target list to find a junk attribute by name and returns its result number, providing the core implementation for junk attribute lookup without requiring a JunkFilter structure.

## Definition
```c
AttrNumber ExecFindJunkAttributeInTlist(List *targetlist, const char *attrName)
```

## Detailed Description
ExecFindJunkAttributeInTlist performs the fundamental operation of locating a named junk attribute within a target list. This function provides the core search logic used by other junk attribute functions and can work directly with any target list, not just those encapsulated in a JunkFilter.

The function iterates through the target list, examining each TargetEntry to find one that:
1. Is marked as a junk attribute (resjunk = true)
2. Has a non-NULL result name (resname)  
3. Has a result name that exactly matches the requested attribute name

This is particularly useful for finding system attributes like "ctid", "tableoid", "xmin", "xmax", etc., which are commonly added as junk attributes for internal executor operations but need to be accessed by name.

The function is more flexible than ExecFindJunkAttribute since it doesn't require a JunkFilter structure, making it suitable for use during plan initialization before JunkFilters are created.

## Parameters / Member Variables
- `targetlist`: List of TargetEntry nodes to search through for the named junk attribute
- `attrName`: Name of the junk attribute to locate within the target list

## Dependencies
- Functions called/Symbols referenced:
  - InvalidAttrNumber: Constant returned when attribute is not found
- Called from (representative examples):
  - [ExecFindJunkAttribute](ExecFindJunkAttribute.md): Higher-level wrapper function using JunkFilter
  - [ExecBuildAuxRowMark](ExecBuildAuxRowMark.md): For setting up row marking during execution
  - [ExecInitModifyTable](ExecInitModifyTable.md): During modify table node initialization

## Notes and Other Information
- Returns InvalidAttrNumber if the specified attribute is not found or is not marked as junk
- Only searches attributes marked with resjunk=true, ignoring regular output attributes
- Performs exact string matching on attribute names using strcmp()
- Does not require the target list to be part of a JunkFilter, making it more versatile
- Critical for executor operations that need to locate system attributes by name during query processing
- Used extensively in modify operations where system attributes like ctid are needed for tuple identification

## Simplified Source

```c
AttrNumber ExecFindJunkAttributeInTlist(List *targetlist, const char *attrName)
{
    ListCell *t;

    // Search through each target entry in the list
    foreach(t, targetlist)
    {
        TargetEntry *tle = lfirst(t);

        // Check if this is a junk attribute with matching name
        if (tle->resjunk && tle->resname &&
            (strcmp(tle->resname, attrName) == 0))
        {
            return tle->resno;  // Found it - return attribute number
        }
    }

    return InvalidAttrNumber;  // Not found
}
```