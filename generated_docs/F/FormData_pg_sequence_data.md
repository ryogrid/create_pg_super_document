# FormData_pg_sequence_data

## Location
src/include/commands/sequence.h: 25 - 30

## Overview
FormData_pg_sequence_data represents the runtime state of a PostgreSQL sequence object, storing the current sequence values and status information.

## Definition

```c
typedef struct FormData_pg_sequence_data
{
	int64		last_value;
	int64		log_cnt;
	bool		is_called;
} FormData_pg_sequence_data;
```
## Detailed Description
This structure defines the data stored in sequence relations to track the current state of a sequence. Each sequence relation contains exactly one tuple with these three fields. The structure is used both for in-memory representation and on-disk storage of sequence state. It works in conjunction with the pg_sequence catalog to provide complete sequence functionality, where pg_sequence stores the sequence parameters (increment, min/max values, etc.) and FormData_pg_sequence_data stores the current runtime state.

The structure is essential for sequence operations like nextval(), currval(), and setval(), allowing PostgreSQL to maintain sequence state across transactions and system restarts.

## Parameters / Member Variables
- : The last value returned by the sequence (or the starting value if the sequence has never been used)
- : The number of sequence values that have been preallocated and logged to WAL but not yet used - used for performance optimization
- : Boolean flag indicating whether nextval() has been called on this sequence (false for newly created sequences)

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a pure data structure)
- Called from (representative examples):
  - DefineSequence (used to initialize sequence data during sequence creation)
  - nextval_internal (accessed via Form_pg_sequence_data pointer for sequence value generation)
  - do_setval (used when setting sequence values explicitly)
  - seq_redo (used during WAL replay for sequence operations)

## Notes and Other Information
- This structure corresponds to the three columns defined by SEQ_COL_LASTVAL, SEQ_COL_LOG, and SEQ_COL_CALLED constants
- The typedef Form_pg_sequence_data provides a pointer type to this structure for easier manipulation
- The log_cnt field is used for WAL optimization - PostgreSQL can log multiple sequence values at once to reduce WAL overhead
- The is_called flag ensures that currval() returns an error if called before nextval() on a sequence
- This structure is stored in the sequence relation's single tuple and is accessed through the buffer cache system