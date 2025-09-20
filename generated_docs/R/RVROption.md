# RVROption

## Location
[src/include/catalog/namespace.h:75-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/namespace.h#L75-L79)

## Overview
An enumeration that defines option flag bits for controlling the behavior of  when looking up relation OIDs from relation names.

## Definition

```c
typedef void (*RangeVarGetRelidCallback) (const RangeVar *relation, Oid relId,
										  Oid oldRelId, void *callback_arg);
```
## Detailed Description
This enumeration provides bit flags that modify the behavior of PostgreSQL's  function when resolving relation names to object identifiers (OIDs). The flags control error handling for missing relations and locking behavior when the target relation is already locked by another process.

These options are designed to handle different scenarios in DDL operations, concurrent access patterns, and error tolerance requirements. The flags can be combined using bitwise OR operations to achieve the desired behavior, with the constraint that  and  are mutually exclusive.

## Parameters / Member Variables
- : When set, the function returns  instead of raising an error if the specified relation does not exist
- : When set, the function immediately raises an error if it cannot acquire the required lock on the relation without waiting
- : When set, the function returns  instead of waiting or erroring if it cannot acquire the required lock on the relation

## Dependencies
- Functions called/Symbols referenced:
  - Used as flags parameter in 
- Called from (representative examples):
  -  (uses  conditionally)
  -  (uses )
  -  (uses )
  -  (uses )
  - Various DDL command functions for error tolerance

## Notes and Other Information
- The flags are implemented as bit values (powers of 2) to allow combining multiple options
-  and  cannot be used together as they represent conflicting locking strategies
- When both  and  are specified, a return value of  is ambiguous (could mean missing relation or lock unavailable)
- The flags primarily control error handling and concurrency behavior rather than affecting the core name resolution logic
- Commonly used in DDL operations where graceful handling of missing relations or lock conflicts is required
- The macro  is a convenience wrapper that conditionally sets  based on a boolean parameter