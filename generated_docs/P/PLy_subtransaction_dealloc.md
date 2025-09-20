# PLy_subtransaction_dealloc

## Location
[src/pl/plpython/plpy_subxactobject.c:71-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_subxactobject.c#L71-L83)

## Overview
A required but empty deallocation function for PLy subtransaction objects to satisfy Python's type system requirements.

## Definition

```c
static void
PLy_subtransaction_dealloc(PyObject *subxact)
```
## Detailed Description
This function serves as the deallocation handler for PLySubtransactionObject instances. While Python's type system requires a dealloc function to be defined for custom types, this implementation is intentionally empty. The function exists purely to satisfy Python's API requirements and does not perform any cleanup operations. This suggests that subtransaction objects either don't require explicit cleanup or that cleanup is handled elsewhere in the system.

## Parameters / Member Variables
- `subxact`: The PLy subtransaction object to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - None (empty function)
- Called from:
  - Python's garbage collection system (automatic, no explicit references found)

## Notes and Other Information
- The function is marked as static, indicating internal use only
- Despite being empty, this function is required by Python's type system
- The comment explicitly states that Python requires this function to be defined
- No actual deallocation logic is performed within this function
- Located in src/pl/plpython/plpy_subxactobject.c:71-83