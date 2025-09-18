# LocalProcessControlFile

## Location
src/backend/access/transam/xlog.c: 4801 - 4813

## Overview
Reads the PostgreSQL control file during startup and stores its contents in local memory before shared memory is available.

## Definition
```c
void LocalProcessControlFile(bool reset)
```

## Detailed Description
This function is responsible for reading the PostgreSQL control file during the startup process, including crash recovery cycles. It operates before shared memory is fully initialized since the sizing of shared memory can depend on the contents of the control file. The function allocates local memory to store the control file data, which will later be copied to shared memory by XLOGShmemInit(). This function is not called in bootstrap mode where no control file exists yet.

## Parameters / Member Variables
- `reset`: Boolean flag that controls whether previous contents are expected. When true, indicates there may be a dangling pointer into old shared memory; when false, expects ControlFile to be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - ReadControlFile
  - ControlFileData (struct type)
- Called from (representative examples):
  - [SubPostmasterMain](../S/SubPostmasterMain.md)
  - [PostmasterMain](../P/PostmasterMain.md)
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md)
  - [PostgresSingleUserMain](../P/PostgresSingleUserMain.md)

## Notes and Other Information
- Must be called during startup before shared memory initialization
- Not used in bootstrap mode where control file doesn't exist yet
- The allocated ControlFile memory will later be copied to shared memory by XLOGShmemInit()
- Includes assertion to verify reset parameter consistency with ControlFile state
- Located in src/backend/access/transam/xlog.c:4801-4813