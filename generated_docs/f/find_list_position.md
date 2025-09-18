# find_list_position

## Location
src/backend/optimizer/path/indxpath.c: 1704 - 1729

## Overview
A utility function that finds the position of a node in a list of nodes, or adds it to the end if not found.

## Definition


## Detailed Description
This function searches through a list of Node pointers to find the position (0-based index) of a given node. It uses the equal() function to perform deep equality comparison between nodes. If the node is found, it returns the index position. If the node is not found in the list, it appends the node to the end of the list and returns the new position.

The function is static and used internally within the index path optimization module to maintain lists of unique nodes while tracking their positions.

## Parameters / Member Variables
- : The Node pointer to search for in the list
- : A pointer to a List pointer that contains Node elements; the list may be modified if the node is not found

## Dependencies
- Functions called/Symbols referenced:
  - [equal](../e/equal.md) (for deep equality comparison of nodes)
  - lfirst (list cell access macro)
  - lappend (to add node to list if not found)
- Called from (representative examples):
  - ec_member_matches_arg
  - [classify_index_clause_usage](../c/classify_index_clause_usage.md)

## Notes and Other Information
- This function modifies the input list by adding the node if it's not already present
- Uses 0-based indexing for position counting
- The function ensures uniqueness of nodes in the list based on structural equality
- Part of the index path optimization infrastructure in PostgreSQL's query planner