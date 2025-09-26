# DestReceiver

## Location
[src/include/tcop/dest.h:113-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tcop/dest.h#L113-L114)

## Overview
DestReceiver is a typedef for struct _DestReceiver that serves as the base type for destination-specific local state in PostgreSQL's tuple output system, providing function pointers that the executor must call to handle query results.

## Definition

```c
typedef struct _DestReceiver DestReceiver;
```
## Detailed Description
DestReceiver is a fundamental component of PostgreSQL's result destination management system. It provides an abstraction layer that allows the executor to send query results to various destinations (frontend processes, files, internal storage, etc.) without needing to know the specific implementation details of each destination type.

The DestReceiver system is designed around a callback-based architecture where different destination types implement the required function pointers according to their specific needs. This allows for flexible and extensible result handling while maintaining a consistent interface for the executor.

The system supports both stateless implementations (where the DestReceiver contains only function pointers) and stateful implementations (where additional fields are added to structures that have DestReceiver as their first field, allowing safe casting).

## Parameters / Member Variables
DestReceiver is a typedef, so it doesn't have direct members, but it refers to the _DestReceiver struct which contains:
- Function pointers for tuple processing operations (receiveSlot, rStartup, rShutdown, rDestroy)
- CommandDest identifier indicating the destination type
- Optional private fields for destination-specific state

## Dependencies
- Functions called/Symbols referenced:
  - struct _DestReceiver (the actual struct definition)
  - CommandDest (enum for destination types)
  - [TupleTableSlot](../T/TupleTableSlot.md) (for tuple handling)
  - [TupleDesc](../T/TupleDesc.md) (for tuple description)

- Called from (representative examples):
  - [CreateDestReceiver](../C/CreateDestReceiver.md) (creates receiver instances)
  - [ExecutePlan](../E/ExecutePlan.md) (executor main function)
  - [ProcessQuery](../P/ProcessQuery.md) (query processing)
  - [PortalRun](../P/PortalRun.md) (portal execution)
  - [standard_ExecutorRun](../s/standard_ExecutorRun.md) (executor entry point)
  - Many destination-specific implementations (printtup, copy, SPI, etc.)

## Notes and Other Information
- A special permanent instance 'None_Receiver' exists for DestNone destination to avoid unnecessary allocation/deallocation
- Receiver objects can be reused multiple times before being destroyed
- The typical lifecycle is: CreateDestReceiver → rStartup → receiveSlot (0+ times) → rShutdown → rDestroy
- Memory context management is important - receivers should be allocated in contexts that live long enough for their usage
- The receiveSlot function returns bool: true means continue processing, false means stop early