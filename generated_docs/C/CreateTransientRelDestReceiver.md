# CreateTransientRelDestReceiver

## Location
[src/backend/commands/matview.c:448-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/matview.c#L448-L465)

## Overview
CreateTransientRelDestReceiver creates a DestReceiver that redirects query output to a transient relation, used during materialized view refresh operations.

## Definition
```c
DestReceiver *CreateTransientRelDestReceiver(Oid transientoid)
```

## Detailed Description
This function creates and initializes a specialized destination receiver of type DR_transientrel that handles inserting query results into a transient table during materialized view refresh operations. The receiver implements the DestReceiver interface with custom callback functions optimized for bulk insertion into temporary tables.

The created receiver configures function pointers for the standard DestReceiver lifecycle: startup, tuple reception, shutdown, and destruction. It stores the OID of the target transient relation and sets the destination type to DestTransientRel, enabling the query executor to route results appropriately.

This receiver is specifically designed for the materialized view refresh workflow, where query results need to be efficiently inserted into a temporary table before being swapped with the original materialized view storage.

## Parameters / Member Variables
- `transientoid`: Object identifier of the transient relation that will receive the query output

## Dependencies
- Functions called/Symbols referenced:
  - DR_transientrel (structure type for transient relation destination receiver)
  - [palloc0](../p/palloc0.md) (allocates zero-initialized memory)
  - [transientrel_receive](../t/transientrel_receive.md) (callback function for receiving individual tuples)
  - [transientrel_startup](../t/transientrel_startup.md) (callback function for receiver initialization)
  - [transientrel_shutdown](../t/transientrel_shutdown.md) (callback function for receiver cleanup)
  - [transientrel_destroy](../t/transientrel_destroy.md) (callback function for receiver destruction)
  - DestTransientRel (destination type constant for transient relations)
  - [DestReceiver](../D/DestReceiver.md) (base interface type for destination receivers)

- Called from (representative examples):
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md) (main materialized view refresh function)
  - [CreateDestReceiver](CreateDestReceiver.md) (general destination receiver factory function)

## Notes and Other Information
- The function allocates memory using palloc0 to ensure the structure is zero-initialized
- The DR_transientrel structure extends the basic DestReceiver with fields specific to transient relation handling:
  - transientoid: OID of the target relation
  - transientrel: Relation pointer (filled during startup)
  - output_cid: Command ID for inserted tuples
  - ti_options: Table insertion performance options
  - bistate: Bulk insert state for optimization
- The receiver implements optimized bulk insertion suitable for large result sets typical in materialized view refreshes
- This is part of the destination receiver pattern used throughout PostgreSQL for flexible query result handling
- The returned receiver must be properly managed through its lifecycle callbacks to ensure resource cleanup