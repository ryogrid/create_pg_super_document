# make_tlist_from_pathtarget

## Location
src/backend/optimizer/util/tlist.c: 624 - 656

## Overview
Constructs a targetlist from a PathTarget structure, essentially performing the reverse operation of make_pathtarget_from_tlist by creating TargetEntry nodes from PathTarget expressions.

## Definition
```c
List *make_tlist_from_pathtarget(PathTarget *target)
```

## Detailed Description
This function creates a complete targetlist (List of TargetEntry nodes) from a PathTarget structure. It iterates through each expression in the PathTarget's expression list and creates corresponding TargetEntry nodes using makeTargetEntry(). Each TargetEntry is assigned a sequential resource number (starting from 1), no column name (NULL), and is marked as not resjunk (false). If the PathTarget contains sort group references, these are preserved in the corresponding TargetEntry nodes.

The function serves as the inverse of make_pathtarget_from_tlist, allowing the optimizer to convert between the lightweight PathTarget representation and full targetlists as needed during query planning.

## Parameters / Member Variables
- `target`: A PathTarget structure containing expressions and optional sort group references to be converted into a targetlist

## Dependencies
- Functions called/Symbols referenced:
  - [PathTarget](../P/PathTarget.md) (data structure)
  - [makeTargetEntry](makeTargetEntry.md) (TargetEntry creation)
  - lappend (list append)
  - NIL (empty list constant)
- Called from (representative examples):
  - [set_subquery_pathlist](../s/set_subquery_pathlist.md)
  - [build_setop_child_paths](../b/build_setop_child_paths.md)
  - Various optimizer functions needing targetlist representations

## Notes and Other Information
- This function performs the inverse operation of make_pathtarget_from_tlist
- [TargetEntry](../T/TargetEntry.md) nodes are created with sequential resource numbers starting from 1
- Column names are set to NULL since PathTarget doesn't preserve original column names
- All created TargetEntry nodes are marked as not resjunk (false)
- Sort group references are preserved if present in the source PathTarget
- The function is declared in src/include/optimizer/tlist.h