# NewGUCNestLevel

## Location
[src/backend/utils/misc/guc.c:2237-2247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2237-L2247)

## Overview
NewGUCNestLevel increments and returns the current GUC nesting level, used when entering a new context that requires transactional GUC variable management.

## Definition

```c
int
NewGUCNestLevel(void)
```
## Detailed Description
This function provides a safe mechanism for incrementing the GUC nesting level when entering contexts that require transactional configuration changes. It is designed to be error-free to ensure that subtransaction start and other critical operations can proceed safely. The function is used in various scenarios including subtransaction start, function execution with proconfig settings, and other situations requiring transient GUC variable modifications.

Key behaviors:
- Atomically increments GUCNestLevel by 1
- Returns the new nesting level value
- Guaranteed not to raise errors (critical for subtransaction start)
- Creates a new logical nesting context for GUC variable state management

## Parameters / Member Variables
This function takes no parameters and returns:
- : The new GUC nesting level after incrementing

## Dependencies
- Functions called/Symbols referenced:
  - GUCNestLevel (global variable)
- Called from (representative examples):
  - [PushTransaction](../P/PushTransaction.md) (subtransaction management)
  - [fmgr_security_definer](../f/fmgr_security_definer.md) (function execution with security context)
  - [execute_extension_script](../e/execute_extension_script.md) (extension script execution)
  - [DefineIndex](../D/DefineIndex.md) (index creation with specific settings)
  - Various DDL operations requiring transient configuration changes

## Notes and Other Information
- This is a public function declared in guc.h
- Must be paired with corresponding AtEOSubXact_GUC or AtEOXact_GUC calls to decrement the nesting level
- The function is intentionally simple to avoid any possibility of errors during critical operations
- Higher nesting levels allow for more granular rollback of configuration changes
- Used extensively throughout PostgreSQL for operations that need to temporarily modify configuration
- Essential for maintaining proper ACID semantics for configuration changes in nested contexts
- The nesting level affects how push_old_value and other GUC functions handle state management

## Simplified Source

```c
// Simplified version of NewGUCNestLevel
int NewGUCNestLevel(void) {
    // Increment and return the GUC nesting level
    return ++GUCNestLevel;
}
```

Key simplifications made:
- Removed detailed comments (essential logic is self-explanatory)
- Preserved the atomic increment-and-return operation
- Maintained the error-free guarantee critical for subtransaction start