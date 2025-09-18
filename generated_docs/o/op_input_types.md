# op_input_types

## Location
src/backend/utils/cache/lsyscache.c: 1358 - 1385

## Overview
Retrieves the left and right input data types for a given operator from the system catalog.

## Definition


## Detailed Description
This function looks up an operator in the pg_operator system catalog using its OID and returns the data types of its left and right operands. It's a utility function in the system cache layer that provides a convenient interface for accessing operator type information. The function uses the system cache (syscache) for efficient lookup and will throw an error if the operator OID is not found.

## Parameters / Member Variables
- : The OID of the operator to look up
- : Output parameter that receives the OID of the left operand data type (InvalidOid if not applicable)
- : Output parameter that receives the OID of the right operand data type (InvalidOid if not applicable)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - ReleaseSysCache
  - Form_pg_operator
- Called from (representative examples):
  - CheckIndexCompatible
  - typeDepNeeded
  - process_equivalence
  - reconsider_outer_join_clause
  - initialize_mergeclause_eclasses
  - ri_HashCompareOp

## Notes and Other Information
- The function will raise an ERROR if the operator OID is not found in the system catalog
- For unary operators, one of the output type parameters will be set to InvalidOid
- This is part of the lsyscache module which provides cached access to system catalog information
- The function is commonly used in query planning and optimization phases