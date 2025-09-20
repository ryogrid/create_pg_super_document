# ProcessingMode

## Location
[src/include/miscadmin.h:458-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/miscadmin.h#L458-L461)

## Overview
An enumeration that defines the three distinct processing modes in PostgreSQL, controlling system behavior during bootstrap, initialization, and normal operations.

## Definition

```c
NormalProcessing,			/* normal processing */
} ProcessingMode;

extern PGDLLIMPORT ProcessingMode Mode;

#define IsBootstrapProcessingMode() (Mode == BootstrapProcessing)
#define IsInitProcessingMode()		(Mode == InitProcessing)
#define IsNormalProcessingMode()	(Mode == NormalProcessing)

#define GetProcessingMode() Mode

#define SetProcessingMode(mode) \
	do
```
## Detailed Description
ProcessingMode is a fundamental enumeration that controls PostgreSQL's operational state throughout its lifecycle. The three modes represent distinct phases of system operation: BootstrapProcessing for initial template database creation where all transactions receive transaction ID "one" and are guaranteed to commit; InitProcessing for backend startup and system initialization; and NormalProcessing for standard operational behavior. This mode switching enables PostgreSQL to handle special bootstrap requirements while maintaining normal transactional integrity during regular operations.

## Parameters / Member Variables
- : Bootstrap mode used during initial template database creation where transactions are assigned ID "one" and guaranteed to commit
- : Initialization mode used during backend startup until all normal initialization is complete
- : Normal operational mode where all code may be executed with standard transactional behavior

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - Mode (global variable of type ProcessingMode)
  - IsBootstrapProcessingMode() macro
  - IsInitProcessingMode() macro
  - IsNormalProcessingMode() macro
  - GetProcessingMode() macro

## Notes and Other Information
- The global variable Mode (of type ProcessingMode) tracks the current processing state
- Convenience macros are provided for mode checking: IsBootstrapProcessingMode(), IsInitProcessingMode(), IsNormalProcessingMode()
- GetProcessingMode() macro provides access to the current mode value
- Bootstrap mode is critical for system initialization as it bypasses normal transaction ID assignment
- Some code behaves differently when executed in InitProcessing mode to enable proper system bootstrapping
- The processing mode directly affects transaction behavior and system state management