# ExplainModifyTarget

## Location
src/backend/commands/explain.c: 4025 - 4033

## Overview
ExplainModifyTarget is a static function that displays the target relation of a ModifyTable node in PostgreSQL's EXPLAIN output.

## Definition


## Detailed Description
This function shows the nominal target relation for ModifyTable operations (INSERT, UPDATE, DELETE) in EXPLAIN output. It specifically displays the relation that was originally named in the query. If the actual target relations differ from the nominal one (such as in partitioned table scenarios), those differences are handled separately by the show_modifytable_info() function. The function serves as a bridge between the ModifyTable plan node and the generic target relation explanation functionality.

## Parameters / Member Variables
- : Pointer to the ModifyTable plan node containing the operation details
- : Pointer to the ExplainState structure controlling the format and options for the EXPLAIN output

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainTargetRel](ExplainTargetRel.md)
  - [ModifyTable](../M/ModifyTable.md) (struct)
  - ExplainState (struct)
- Called from (representative examples):
  - [ExplainNode](ExplainNode.md)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- The function focuses on the 'nominal' relation, which represents the relation as it appeared in the original query
- For complex scenarios like partitioned tables where actual targets may differ, additional information is provided by show_modifytable_info()
- Part of PostgreSQL's query execution plan explanation system