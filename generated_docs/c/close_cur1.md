# close_cur1

## Location
[src/interfaces/ecpg/test/expected/preproc-outofscope.c:251-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-outofscope.c#L251-L261)

## Overview
A static function that closes a database cursor in ECPG test code, properly releasing cursor resources after completion of cursor operations.

## Definition

```c
static void
close_cur1(void)
```
## Detailed Description
The  function is part of the ECPG test infrastructure that demonstrates proper cursor lifecycle management. It uses the ECPGdo function to execute a CLOSE statement on the 'mycur' cursor that was previously opened with open_cur1. This function represents the final step in the cursor operation sequence, ensuring that database resources are properly released.

The function demonstrates the ECPG translation of embedded SQL cursor close operations into the underlying PostgreSQL client library calls. It's specifically designed to test cursor resource management in scenarios where cursor operations span different function scopes.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [ECPGdo](../E/ECPGdo.md) (ECPG runtime function for SQL execution)
  - ECPGt_EOIT, ECPGt_EORT (ECPG type constants for statement boundaries)
- Called from (representative examples):
  - [main](../m/main.md) (in the same test file)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- Part of the ECPG test suite for validating out-of-scope variable handling
- The function uses a simple ECPGdo call with minimal parameters since CLOSE operations don't require data binding
- Includes automatic error handling via sqlca.sqlcode checking
- The operation corresponds to 'CLOSE mycur' in SQL
- Essential for proper cursor resource management and preventing resource leaks
- Should be called after all desired FETCH operations have been completed
- The function expects that the cursor 'mycur' has been previously opened and used
- Simpler than open_cur1 and get_record1 as it doesn't require complex parameter binding
- Completes the cursor lifecycle: declare/open → fetch → close