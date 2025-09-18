# PLy_get_scratch_context

## Location
[src/pl/plpython/plpy_main.c:376-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.c#L376-L390)

## Overview
Returns a scratch memory context for temporary allocations during PL/Python procedure execution, creating it lazily on first request.

## Definition


## Detailed Description
PLy_get_scratch_context provides access to a dedicated memory context for temporary allocations within a PL/Python procedure execution context. The function implements lazy initialization - the scratch context is only allocated when first requested, as it might never be needed for some procedures. When created, the scratch context is a child of TopTransactionContext and uses default allocation set sizes for efficient memory management.

## Parameters / Member Variables
- `context`: Pointer to the current PLyExecutionContext containing execution state and memory contexts

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - ALLOCSET_DEFAULT_SIZES
  - [PLyExecutionContext](PLyExecutionContext.md)
- Called from (representative examples):
  - [PLy_input_convert](PLy_input_convert.md)
  - [PLy_input_from_tuple](PLy_input_from_tuple.md)

## Notes and Other Information
- The scratch context is lazily allocated to avoid unnecessary memory overhead for procedures that don't need temporary storage
- Uses TopTransactionContext as parent to ensure proper cleanup at transaction end
- The context is automatically cleaned up when the execution context is popped via PLy_pop_execution_context
- Primarily used for type conversion operations that require temporary memory allocations