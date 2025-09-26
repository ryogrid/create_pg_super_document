# _DestReceiver

## Location
src/include/tcop/dest.h: 115 - 148

## Overview
_DestReceiver is the actual struct definition that implements PostgreSQL's destination receiver interface, containing function pointers and state for handling tuple output to various destinations during query execution.

## Definition
```c
struct _DestReceiver
{
	/* Called for each tuple to be output: */
	bool		(*receiveSlot) (TupleTableSlot *slot,
								DestReceiver *self);
	/* Per-executor-run initialization and shutdown: */
	void		(*rStartup) (DestReceiver *self,
							 int operation,
							 TupleDesc typeinfo);
	void		(*rShutdown) (DestReceiver *self);
	/* Destroy the receiver object itself (if dynamically allocated) */
	void		(*rDestroy) (DestReceiver *self);
	/* CommandDest code for this receiver */
	CommandDest mydest;
	/* Private fields might appear beyond this point... */
};
```

## Detailed Description
_DestReceiver is the core struct that defines the interface contract for all destination receivers in PostgreSQL. It implements a callback-based pattern where different destination types provide their own implementations of the function pointers to handle tuples according to their specific requirements.

The struct serves as both a function pointer table and a way to identify the destination type through the mydest field. The design allows for polymorphic behavior where the executor can call the same interface methods regardless of whether tuples are being sent to a client, written to a file, stored in memory, or processed by other subsystems.

The structure is designed to be extended - destination-specific implementations can create larger structs with _DestReceiver as the first field, then safely cast between the types. This allows each destination type to maintain its own private state while still conforming to the common interface.

## Parameters / Member Variables
- `receiveSlot`: Function pointer called for each output tuple. Takes a TupleTableSlot and returns bool (true = continue, false = stop early)
- `rStartup`: Function pointer for per-executor-run initialization. Called with operation type and tuple description
- `rShutdown`: Function pointer for per-executor-run cleanup. Called after all tuples have been processed
- `rDestroy`: Function pointer to destroy the receiver object itself, used when the receiver is dynamically allocated
- `mydest`: CommandDest enum value indicating which type of destination this receiver handles
- Private fields may follow after mydest for destination-specific state (indicated by comment)

## Dependencies
- Functions called/Symbols referenced:
  - TupleTableSlot (for tuple data handling)
  - TupleDesc (for tuple structure description)
  - CommandDest (enum for destination identification)
  - Various destination-specific types and functions

- Called from (representative examples):
  - printtup_startup, printtup, printtup_shutdown (client output)
  - copy_dest_startup, copy_dest_receive, copy_dest_shutdown (COPY command)
  - intorel_startup, intorel_receive, intorel_shutdown (SELECT INTO)
  - spi_dest_startup, spi_printtup (SPI interface)
  - tqueueReceiveSlot, tqueueStartupReceiver (tuple queues)
  - Many other destination-specific implementations

## Notes and Other Information
- The struct is designed for extension - most real implementations embed this as the first field in a larger struct
- Function pointers must not be NULL; each destination type provides complete implementations
- The receiveSlot function contract: identical TupleDesc must be used as provided to rStartup
- Memory management: rDestroy should handle freeing any dynamically allocated memory
- The struct supports both simple stateless destinations and complex stateful ones
- Thread safety and concurrency aspects depend on the specific destination implementation
- The interface supports early termination via receiveSlot returning false