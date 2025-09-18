# get_equal_strategy_number

## Location
[src/backend/executor/execReplication.c:75-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execReplication.c#L75-L95)

## Overview
Returns the strategy number for the equality operator by determining the access method from an operator class and delegating to the access method-specific strategy lookup.

## Definition
```c
static StrategyNumber get_equal_strategy_number(Oid opclass)
```

## Detailed Description
This is a convenience wrapper function that takes an operator class OID and returns the appropriate strategy number for the equality operator. It works by first determining which index access method the operator class belongs to using `get_opclass_method()`, then calling `get_equal_strategy_number_for_am()` to get the actual strategy number for that access method.

This function serves as an abstraction layer, allowing callers to work with operator classes directly without needing to know the underlying access method details. It's particularly useful in replication scenarios where equality operations need to be performed on indexed columns.

## Parameters / Member Variables
- `opclass`: OID of the operator class for which to retrieve the equality strategy number

## Dependencies
- Functions called/Symbols referenced:
  - [get_opclass_method](get_opclass_method.md) (to determine the access method from the operator class)
  - [get_equal_strategy_number_for_am](get_equal_strategy_number_for_am.md) (to get the strategy number for the access method)

- Called from (representative examples):
  - [build_replindex_scan_key](../b/build_replindex_scan_key.md)

## Notes and Other Information
- This is a static function, so it's only accessible within the execReplication.c file
- The function is part of the replication infrastructure in PostgreSQL
- It provides a higher-level interface compared to `get_equal_strategy_number_for_am` by working with operator classes instead of access methods directly
- Inherits the same limitations as `get_equal_strategy_number_for_am` - only supports B-tree and Hash indexes