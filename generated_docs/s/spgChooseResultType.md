# spgChooseResultType

## Location
src/include/access/spgist.h: 72 - 73

## Overview
An enumeration that defines the possible result types returned by the SP-GiST "choose" support function, indicating the action to be taken when inserting a new tuple into the index.

## Definition


## Detailed Description
The spgChooseResultType enum is a fundamental component of the SP-GiST (Space-Partitioned Generalized Search Tree) access method in PostgreSQL. This enum is used by the "choose" support function to communicate the decision made when determining where to insert a new datum in the tree structure. The choose function examines the characteristics of the new datum relative to an inner tuple and decides whether to descend into an existing child node, add a new node, or split the current tuple to accommodate the new data.

Each enum value corresponds to a specific structural operation on the SP-GiST tree:
- **spgMatchNode**: The new datum can be accommodated by descending into one of the existing child nodes of the current inner tuple
- **spgAddNode**: A new child node needs to be created and added to the current inner tuple to accommodate the new datum
- **spgSplitTuple**: The current inner tuple needs to be split, typically by changing its prefix, to create space for the new datum

## Parameters / Member Variables
- : Value 1 - Indicates that the insertion should proceed by descending into an existing child node of the current inner tuple
- : Value 2 - Indicates that a new child node should be added to the current inner tuple to accommodate the new datum
- : Value 3 - Indicates that the current inner tuple should be split, usually by modifying its prefix, to make room for the new datum

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum type with no direct function calls)
- Used by:
  - [spgChooseOut](spgChooseOut.md) structure (as resultType field)
  - SP-GiST choose support functions (SPGIST_CHOOSE_PROC)

## Notes and Other Information
- This enum is part of the SP-GiST access method API, defined in src/include/access/spgist.h:67-72
- The enum values are used in conjunction with the spgChooseOut structure, which contains a union of result data specific to each action type
- Each enum value corresponds to different fields in the spgChooseOut.result union:
  - spgMatchNode uses the matchNode struct (nodeN, levelAdd, restDatum)
  - spgAddNode uses the addNode struct (nodeLabel, nodeN)
  - spgSplitTuple uses the splitTuple struct (prefix and postfix information)
- The choose function is one of the required support functions for SP-GiST operator classes (function number SPGIST_CHOOSE_PROC = 2)
- This enum enables the SP-GiST access method to handle dynamic tree restructuring during insertions, making it suitable for various spatial and hierarchical data types