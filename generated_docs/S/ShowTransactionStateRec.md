# ShowTransactionStateRec

## Location
src/backend/access/transam/xact.c: 5598 - 5644

## Overview
A recursive debugging function that displays detailed transaction state information for a transaction and all its parent transactions in PostgreSQL's transaction hierarchy.

## Definition


## Detailed Description
ShowTransactionStateRec is a recursive subroutine used internally by ShowTransactionState to provide comprehensive debugging output about transaction states. The function traverses up the transaction hierarchy by recursively calling itself on parent transactions first, then displays detailed state information for the current transaction including nesting level, block state, transaction state, XIDs, and child transaction information. To prevent stack overflow during deep recursion, the function includes a stack depth check that omits parent details when the stack becomes too deep.

## Parameters / Member Variables
- : A descriptive string prefix used in debug messages to identify the context of the transaction state display
- : Pointer to the TransactionState structure containing the transaction information to be displayed

## Dependencies
- Functions called/Symbols referenced:
  - stack_is_too_deep
  - ereport (with DEBUG5 level)
  - errmsg_internal
  - ShowTransactionStateRec (recursive call)
  - initStringInfo
  - appendStringInfo
  - PointerIsValid
  - BlockStateAsString
  - TransStateAsString
  - XidFromFullTransactionId
  - pfree
- Called from (representative examples):
  - ShowTransactionState
  - ShowTransactionStateRec (recursive call)

## Notes and Other Information
- This is a static function used exclusively for debugging purposes with DEBUG5 log level
- The function implements stack overflow protection by checking stack depth before recursing
- Child transaction XIDs are displayed in a comma-separated format when present
- The output includes comprehensive transaction metadata: nesting level, name, block state, transaction state, XID, sub-transaction ID, and command ID
- Memory allocated for the StringInfo buffer is properly freed after use