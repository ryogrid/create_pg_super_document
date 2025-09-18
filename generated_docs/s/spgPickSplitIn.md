# spgPickSplitIn

## Location
src/include/access/spgist.h: 110 - 115

## Overview
A struct that serves as input parameter for the SP-GiST opclass picksplit method, containing information about the leaf tuples that need to be organized when splitting an overfull leaf page.

## Definition


## Detailed Description
spgPickSplitIn is an input structure used in the SP-GiST (Space-Partitioned Generalized Search Tree) index access method. It is passed to the opclass picksplit method when a leaf page becomes too full and needs to be split. The method uses this information to decide how to partition the leaf tuples into groups and potentially create new inner tuple structure to organize them efficiently.

## Parameters / Member Variables
- : The number of leaf tuples that need to be organized in the split operation
- : Array of datum values from the leaf tuples, with length equal to nTuples
- : The current depth level in the tree where the split is occurring, starting from zero at the root level

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL generic data value type)
- Called from (representative examples):
  - checkAllTheSame (src/backend/access/spgist/spgdoinsert.c:599)
  - doPickSplit (src/backend/access/spgist/spgdoinsert.c:683)
  - spg_kd_picksplit (src/backend/access/spgist/spgkdtreeproc.c:110)
  - spg_quad_picksplit (src/backend/access/spgist/spgquadtreeproc.c:171)
  - spg_text_picksplit (src/backend/access/spgist/spgtextproc.c:335)
  - spg_box_quad_picksplit (src/backend/utils/adt/geo_spgist.c:443)
  - inet_spg_picksplit (src/backend/utils/adt/network_spgist.c:167)
  - spg_range_quad_picksplit (src/backend/utils/adt/rangetypes_spgist.c:202)

## Notes and Other Information
- This struct is part of the SP-GiST index access method interface
- It works in conjunction with spgPickSplitOut to allow opclass picksplit methods to receive input data and return partitioning decisions
- The picksplit method is called when leaf pages become overfull during insertion operations
- Different opclasses use different strategies to analyze the input datums and determine optimal partitioning
- The level information helps opclasses make decisions that may vary by depth in the tree
- The method must examine all the datum values to determine how to best partition them for efficient future searches
- This is a critical operation for maintaining good performance in SP-GiST indexes as it determines the tree structure