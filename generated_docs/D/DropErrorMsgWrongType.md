# DropErrorMsgWrongType

## Location
src/backend/commands/tablecmds.c: 1441 - 1467

## Overview
DropErrorMsgWrongType generates specific error messages when a DROP command is issued on a relation that exists but is of the wrong type.

## Definition


## Detailed Description
DropErrorMsgWrongType is a static helper function that produces informative error messages when users attempt to drop a relation using the wrong DROP command variant (e.g., using DROP TABLE on a view, or DROP INDEX on a table). The function looks up both the expected relation type (rightkind) and the actual relation type (wrongkind) in the dropmsgstringarray to construct an error message that explains what the relation actually is and provides a hint about the correct DROP command to use.

## Parameters / Member Variables
- : Name of the relation being dropped
- : Character representing the actual type of the relation
- : Character representing the expected type of relation for the DROP command

## Dependencies
- Functions called/Symbols referenced:
  - dropmsgstringarray
  - ereport
  - [errhint](../e/errhint.md)
  - Assert
- Called from (representative examples):
  - [RangeVarCallbackForDropRelation](../R/RangeVarCallbackForDropRelation.md)

## Notes and Other Information
DropErrorMsgWrongType provides user-friendly guidance when type mismatches occur in DROP operations. The function uses the static dropmsgstringarray to map relation kinds to their corresponding error messages and hints. It handles cases where the wrongkind might not be found in the table by conditionally including the hint. The error uses ERRCODE_WRONG_OBJECT_TYPE to categorize the issue appropriately. This function is essential for making PostgreSQL's error messages more helpful by not just saying "relation not found" but explaining what the relation actually is and how to drop it correctly.