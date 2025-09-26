# ExecFindJunkAttribute

## Location
[src/backend/executor/execJunk.c:210-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execJunk.c#L210-L221)

## Overview
Locates a specified junk attribute by name within a JunkFilter's target list and returns its result number, serving as a convenient wrapper around ExecFindJunkAttributeInTlist.

## Definition
```c
AttrNumber ExecFindJunkAttribute(JunkFilter *junkfilter, const char *attrName)
```

## Detailed Description
ExecFindJunkAttribute is a simple wrapper function that provides an interface for finding junk attributes within an initialized JunkFilter structure. It delegates the actual search logic to ExecFindJunkAttributeInTlist, passing the JunkFilter's target list for processing.

This function is typically used during query execution when the executor needs to locate specific system attributes (like "ctid", "tableoid", "xmin", etc.) or other junk attributes that were included in the target list for internal processing purposes.

The function provides a clean abstraction layer, allowing callers to work with JunkFilter objects without directly accessing their internal target list structure.

## Parameters / Member Variables
- `junkfilter`: Pointer to an initialized JunkFilter structure containing the target list to search
- `attrName`: Name of the junk attribute to locate within the target list

## Dependencies
- Functions called/Symbols referenced:
  - ExecFindJunkAttributeInTlist: Performs the actual search within the target list
  - JunkFilter: Input structure type containing target list
- Called from (representative examples):
  - Various executor functions that need to access system attributes or internal columns

## Notes and Other Information
- Returns InvalidAttrNumber if the specified attribute is not found in the target list
- This is a thin wrapper that maintains the abstraction of the JunkFilter interface
- The function only searches for junk attributes (those with resjunk=true in the target list)
- Part of the PostgreSQL executor's system for managing internal-only attributes during query processing