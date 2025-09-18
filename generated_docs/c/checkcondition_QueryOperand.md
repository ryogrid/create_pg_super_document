# checkcondition_QueryOperand

## Location
src/backend/utils/adt/tsrank.c: 563 - 589

## Overview
A callback function used by TS_execute to check if a query operand matches against QueryRepresentation data during text search ranking.

## Definition


## Detailed Description
The  function serves as a callback for the TS_execute function during text search query evaluation. It determines whether a specific query operand (word) exists in the document representation and optionally provides positional information for phrase matching. The function returns a ternary value indicating definite match (TS_YES) or definite no-match (TS_NO), and can populate positional data when phrase matching is required. This callback is essential for bridging the gap between abstract query execution and concrete document representation data used in ranking calculations.

## Parameters / Member Variables
-  (void*): Pointer to QueryRepresentation structure cast as void* containing operand existence and position data
-  (QueryOperand*): Pointer to the query operand being checked for existence in the document
-  (ExecPhraseData*): Optional structure for returning position information when phrase matching is needed; can be NULL

## Dependencies
- Functions called/Symbols referenced:
  - QueryRepresentation: Structure containing operand data and query information
  - QueryRepresentationOperand: Structure holding operand existence and position data
  - QR_GET_OPERAND_DATA: Macro to retrieve operand data from QueryRepresentation
  - TS_NO: Return value indicating the operand does not exist in the document
  - TS_YES: Return value indicating the operand exists in the document
  - MAXQROPOS: Maximum number of positions that can be stored per operand
- Called from (representative examples):
  - Cover: Uses this callback during cover distance calculations to check operand matches

## Notes and Other Information
- Returns TSTernaryValue: TS_NO if operand doesn't exist, TS_YES if it does
- When data parameter is provided, fills in position information for phrase matching
- Handles reverse insertion mode for position arrays when operandData->reverseinsert is true
- The reverseinsert flag affects how position arrays are organized in memory for efficient processing
- Critical component of PostgreSQL's text search ranking system, specifically for cover distance algorithms
- Position data is essential for phrase queries and proximity-based ranking calculations
- Part of the callback-based architecture that allows flexible query evaluation strategies