# _bt_find_extreme_element

## Location
src/backend/access/nbtree/nbtutils.c: 789 - 848

## Overview
Finds the least or greatest element in an array of values for a specific B-tree index column using the column's opfamily comparison semantics.

## Definition


## Detailed Description
This function identifies either the minimum or maximum element from an array of values based on the comparison strategy specified. It uses the operator family (opfamily) associated with the index column to determine the appropriate comparison operator and procedure. The function dynamically looks up the comparison operator for the given element type and strategy, then iterates through all elements to find the extreme value.

The function is designed to work with B-tree index preprocessing, particularly for handling array values in scan keys where finding boundary elements is necessary for optimization.

## Parameters / Member Variables
- : IndexScanDesc containing information about the index scan, including the index relation
- : ScanKey identifying the index column and providing collation information
- : OID of the element type being compared
- : Strategy number indicating comparison direction (BTLessStrategyNumber for minimum, BTGreaterStrategyNumber for maximum)
- : Array of Datum values to search through
- : Number of elements in the elems array

## Dependencies
- Functions called/Symbols referenced:
  - get_opfamily_member
  - get_opcode
  - RegProcedureIsValid
  - fmgr_info
  - FunctionCall2Coll
  - IndexScanDesc
  - ScanKey
  - StrategyNumber
  - RegProcedure
- Called from (representative examples):
  - _bt_preprocess_array_keys

## Notes and Other Information
- The function assumes the opfamily is complete and contains the necessary comparison operators for the element type
- It uses assertions to ensure the strategy is not BTEqualStrategyNumber and that elemtype is valid
- Error handling includes checks for missing operators and procedures in the opfamily
- The comparison is performed using the collation specified in the scan key
- This is a static function, so it's only accessible within the nbtutils.c file