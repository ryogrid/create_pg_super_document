# CreateIntoRelDestReceiver

## Location
src/backend/commands/createas.c: 433 - 451

## Overview
CreateIntoRelDestReceiver creates and initializes a DestReceiver object specifically for CREATE TABLE AS and CREATE MATERIALIZED VIEW operations, setting up callbacks for handling tuple insertion into new relations.

## Definition
DestReceiver *CreateIntoRelDestReceiver(IntoClause *intoClause)

## Detailed Description
This function allocates and initializes a DR_intorel structure that implements the DestReceiver interface for operations that create new relations from query results. The function sets up callback functions for the complete lifecycle of tuple processing: startup (intorel_startup), receiving tuples (intorel_receive), shutdown (intorel_shutdown), and cleanup (intorel_destroy). The intoClause parameter can be NULL when called from CreateDestReceiver(), allowing it to be provided later, but it's convenient to fill it immediately for other callers.

## Parameters / Member Variables
- : An IntoClause structure containing the specification for the target relation, including table name, column names, and various options. Can be NULL if provided later.

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [intorel_receive](../i/intorel_receive.md)
  - [intorel_startup](../i/intorel_startup.md)  
  - [intorel_shutdown](../i/intorel_shutdown.md)
  - [intorel_destroy](../i/intorel_destroy.md)
  - DestIntoRel
- Called from (representative examples):
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md)
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [CreateDestReceiver](CreateDestReceiver.md)

## Notes and Other Information
The function initializes the pub.mydest field to DestIntoRel to identify this as a relation destination receiver. Other private fields of the DR_intorel structure are set during the intorel_startup phase rather than during creation. The returned DestReceiver pointer provides a generic interface while hiding the specific DR_intorel implementation details.