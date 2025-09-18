# ExecMaterialMarkPos

## Location
[src/backend/executor/nodeMaterial.c:262-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMaterial.c#L262-L289)

## Overview
ExecMaterialMarkPos saves the current position in the tuplestore as a mark that can be restored later, enabling mark/restore functionality for Material nodes.

## Definition


## Detailed Description
ExecMaterialMarkPos implements the mark operation for Material nodes by copying the current read pointer position to the mark pointer in the tuplestore. This allows the executor to return to this marked position later using ExecMaterialRestrPos. The function also performs an optimization by calling tuplestore_trim to potentially free memory from tuples that are no longer needed.

The function includes safety checks - it asserts that the EXEC_FLAG_MARK flag is set (indicating mark/restore support was requested during initialization) and gracefully handles the case where the tuplestore hasn't been materialized yet.

## Parameters / Member Variables
- : The MaterialState node for which to set the mark position

## Dependencies
- Functions called/Symbols referenced:
  - [MaterialState](../M/MaterialState.md)
  - EXEC_FLAG_MARK
  - [tuplestore_copy_read_pointer](../t/tuplestore_copy_read_pointer.md)
  - [tuplestore_trim](../t/tuplestore_trim.md)
- Called from (representative examples):
  - [ExecMarkPos](ExecMarkPos.md)

## Notes and Other Information
- Requires EXEC_FLAG_MARK to be set during node initialization
- Uses read pointer 0 (active) and 1 (mark) as established during ExecInitMaterial
- The tuplestore_trim call is an optimization to free unused tuples after advancing the mark
- Returns immediately if no tuplestore has been created yet (lazy initialization)
- The mark position is persistent until explicitly changed by another mark operation