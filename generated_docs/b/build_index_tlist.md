# build_index_tlist

## Location
src/backend/optimizer/util/plancat.c: 1885 - 1946

## Overview
Constructs a target list representing the columns of a specified index, with each column represented by a Var or expression in base-relation terms.

## Definition


## Detailed Description
This function builds a target list that represents the structure of an index by creating appropriate expressions for each indexed column. Unlike , this function does not need to handle dropped columns since indexes never contain dropped columns.

The function processes two types of index columns:
1. **Simple columns** (indexkey != 0): Regular table columns referenced by their attribute number, including system columns (negative indexkey values)
2. **Expression columns** (indexkey == 0): Complex expressions stored in the index's  list

For simple columns, it creates Var nodes using the column's type information from either the heap relation's tuple descriptor or system attribute definitions. For expression columns, it retrieves the pre-parsed expressions from the IndexOptInfo structure.

The function ensures consistency by validating that the number of expression columns matches the expected count based on zero indexkey values.

## Parameters / Member Variables
- : PlannerInfo containing global planner state (currently unused in implementation)
- : IndexOptInfo structure containing index metadata including column keys and expressions
- : Relation structure for the base table to access attribute information

## Dependencies
- Functions called/Symbols referenced:
  - list_head (gets first element of index expressions list)
  - [lnext](../l/lnext.md) (advances to next list element)
  - [SystemAttributeDefinition](../S/SystemAttributeDefinition.md) (gets system attribute information for negative indexkeys)
  - TupleDescAttr (accesses heap relation attribute information)
  - makeVar (creates Var nodes for simple columns)
  - [makeTargetEntry](../m/makeTargetEntry.md) (creates target list entries)
  - [IndexOptInfo](../I/IndexOptInfo.md) (index optimization information structure)

- Called from (representative examples):
  - [get_relation_info](../g/get_relation_info.md) (src/backend/optimizer/util/plancat.c:455)

## Notes and Other Information
- Static function (internal to plancat.c module)
- No failure cases needed since indexes cannot contain dropped columns
- Handles both regular columns and expression-based index columns
- System columns (negative indexkey) are supported via SystemAttributeDefinition
- Expression columns are taken from the pre-parsed  list in IndexOptInfo
- Critical for index-only scans and index scan planning
- Target entries use 1-based numbering (i + 1) to match PostgreSQL conventions
- Location: src/backend/optimizer/util/plancat.c:1885-1946