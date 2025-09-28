# CreateIntoRelDestReceiver

## Location
[src/backend/commands/createas.c:433-451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/createas.c#L433-L451)

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

## Simplified Source

```c
// Simplified version of CreateIntoRelDestReceiver
DestReceiver *CreateIntoRelDestReceiver(IntoClause *intoClause) {
    // Allocate and zero-initialize the into-relation destination receiver
    DR_intorel *into_receiver = (DR_intorel *) palloc0(sizeof(DR_intorel));

    // Set up the callback functions for relation creation operations
    into_receiver->pub.receiveSlot = intorel_receive;      // Insert each tuple into relation
    into_receiver->pub.rStartup = intorel_startup;         // Create the target relation
    into_receiver->pub.rShutdown = intorel_shutdown;       // Finalize the relation
    into_receiver->pub.rDestroy = intorel_destroy;         // Final cleanup

    // Set the destination type to indicate relation creation operation
    into_receiver->pub.mydest = DestIntoRel;

    // Store the INTO clause specification (can be NULL if provided later)
    into_receiver->into = intoClause;

    // Note: Other private fields will be set during intorel_startup

    // Return as base DestReceiver type
    return (DestReceiver *) into_receiver;
}
```

Key simplifications made:
- Added descriptive variable name for clarity
- Added comments explaining each callback function's purpose
- Clarified that intoClause can be NULL and set later
- Explained deferred initialization of other fields
- Focused on core logic: allocate memory, set callbacks, store clause, return receiver