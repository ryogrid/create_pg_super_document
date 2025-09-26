# rightneighbor

## Location
src/backend/storage/freespace/fsmpage.c: 37 - 62

## Overview
The rightneighbor function finds the right neighbor of a given node position in a binary tree structure used by the Free Space Map, with wrapping behavior within the same tree level.

## Definition

```c
static int
rightneighbor(int x)
```
## Detailed Description
This function implements a navigation mechanism for binary tree nodes used in PostgreSQL's Free Space Map pages. It moves to the right neighbor of the current node position, with special handling for level boundaries. When reaching the end of a level (the rightmost node), it wraps around to continue at the same level by moving to the parent node's position.

The function uses a clever bit manipulation technique to detect when a node is at the leftmost position of the next level. Since leftmost nodes at each level are numbered as 2^level - 1, the function checks if (x + 1) is a power of two using the expression , which is true only for powers of two in two's complement arithmetic.

## Parameters / Member Variables
- : The current node position in the binary tree structure for which to find the right neighbor

## Dependencies
- Functions called/Symbols referenced:
  -  (macro): Calculates the parent node position as 
- Called from (representative examples):
  - : Uses rightneighbor to navigate through tree nodes when searching for available space

## Notes and Other Information
- This is a static function, meaning it's only accessible within the fsmpage.c file
- The function is specifically designed for the binary tree structure used in Free Space Map pages
- The wrapping behavior ensures that navigation stays within the same logical level of the tree
- The bit manipulation trick  is a well-known method to check if a number is a power of two
- Part of PostgreSQL's Free Space Map implementation for efficient space management in heap files