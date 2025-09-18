# resetQueryRepresentation

## Location
src/backend/utils/adt/tsrank.c: 593 - 605

## Overview
Resets a QueryRepresentation structure to its initial state, clearing operand existence flags and resetting position counters for all query operands.

## Definition
static void resetQueryRepresentation(QueryRepresentation *qr, bool reverseinsert)

## Detailed Description
This function initializes or resets a QueryRepresentation structure by iterating through all operands in the associated query and resetting their state. It sets the operandexists flag to false for each operand, configures the reverseinsert flag according to the parameter, and resets the position counter (npos) to 0. This is typically used to prepare the QueryRepresentation for a new text search ranking calculation.

## Parameters / Member Variables
- : Pointer to the QueryRepresentation structure to be reset
- : Boolean flag indicating whether operands should be processed in reverse insertion order

## Dependencies
- Functions called/Symbols referenced:
  - QueryRepresentation (struct type)
- Called from (representative examples):
  - Cover (called at lines 658 and 687)

## Notes and Other Information
This is a static function within the tsrank.c module, indicating it's an internal utility function for text search ranking operations. The function is essential for ensuring clean state before performing new ranking calculations on the same QueryRepresentation structure.