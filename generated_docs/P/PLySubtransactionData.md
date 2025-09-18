# PLySubtransactionData

## Location
src/pl/plpython/plpy_subxactobject.h: 24 - 28

## Overview
PLySubtransactionData is a data structure that stores the execution context state for explicit subtransactions in PL/Python procedural language extension.

## Definition


## Detailed Description
PLySubtransactionData is a lightweight data structure used internally by PostgreSQL's PL/Python extension to preserve the execution context when entering an explicit subtransaction. When a subtransaction is started via the Python interface (plpy.subtransaction()), this structure captures the current memory context and resource owner so they can be properly restored when the subtransaction exits.

This structure is essential for maintaining PostgreSQL's resource management invariants across subtransaction boundaries. It ensures that memory allocations and resource ownership are correctly tracked and restored, preventing resource leaks and maintaining transaction isolation.

The data is stored in a global list (explicit_subtransactions) and is managed automatically by the subtransaction infrastructure.

## Parameters / Member Variables
- : The MemoryContext that was active before entering the subtransaction, used for restoration upon exit
- : The ResourceOwner that was active before entering the subtransaction, used for proper resource cleanup and restoration

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContext (PostgreSQL memory management system)
  - ResourceOwner (PostgreSQL resource tracking system)
- Called from (representative examples):
  - PLy_abort_open_subtransactions
  - PLy_subtransaction_enter
  - PLy_subtransaction_exit

## Notes and Other Information
- Allocated in TopTransactionContext to ensure it survives subtransaction boundaries
- Instances are stored in the global explicit_subtransactions list for tracking active subtransactions
- Automatically cleaned up when subtransactions are properly exited or forcibly aborted
- Critical for maintaining PostgreSQL's memory and resource management guarantees within PL/Python
- Used in conjunction with BeginInternalSubTransaction() and RollbackAndReleaseCurrentSubTransaction()
- Part of PostgreSQL's PL/Python extension located in src/pl/plpython/