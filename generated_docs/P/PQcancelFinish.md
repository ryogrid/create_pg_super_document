# PQcancelFinish

## Location
src/interfaces/libpq/fe-cancel.c: 335 - 349

## Overview
Properly closes and frees all resources associated with a PostgreSQL cancel connection, ensuring complete cleanup of the connection object.

## Definition
```c
void PQcancelFinish(PGcancelConn *cancelConn)
```

## Detailed Description
PQcancelFinish performs the final cleanup of a cancel connection by closing the underlying connection and freeing all associated resources. This function serves as a wrapper around PQfinish, specifically designed to work with PGcancelConn structures used for query cancellation. It ensures that all memory and network resources allocated for the cancel connection are properly released, preventing resource leaks in applications that use query cancellation functionality.

## Parameters / Member Variables
- `cancelConn`: A pointer to a PGcancelConn structure that will be closed and freed

## Dependencies
- Functions called/Symbols referenced:
  - PQfinish
  - PGcancelConn (type)
  - PGcancel (related type)
- Called from (representative examples):
  - test_cancel (in libpq_pipeline test module)
  - libpqsrv_cancel (libpq backend-frontend helpers)
  - try_complete_step (isolation tester)
  - disconnectDatabase (connect utilities)

## Notes and Other Information
- This function should be called when the cancel connection is no longer needed
- After calling this function, the cancelConn pointer becomes invalid and should not be used
- Essential for preventing memory leaks in long-running applications
- Commonly used in cleanup routines and error handling paths
- The function handles all internal cleanup, including closing network connections and freeing allocated memory
- Should be paired with successful PQcancelCreate calls to ensure proper resource management