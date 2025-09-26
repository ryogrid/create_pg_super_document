# SPIPlanPtr

## Location
[src/include/executor/spi.h:66-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/spi.h#L66-L67)

## Overview
SPIPlanPtr is an opaque pointer type that represents a prepared execution plan in PostgreSQL's Server Programming Interface, providing a handle for managing and executing prepared SQL statements.

## Definition

```c
typedef struct _SPI_plan *SPIPlanPtr;
```
## Detailed Description
SPIPlanPtr serves as the primary handle for prepared execution plans within the SPI system. It is deliberately designed as an opaque pointer to hide the internal implementation details of the _SPI_plan structure from external users, providing a clean abstraction layer for plan management. This design ensures that users cannot directly manipulate plan internals while still allowing efficient plan reuse and execution.

The opaque nature of SPIPlanPtr promotes encapsulation and allows the PostgreSQL development team to modify internal plan structures without breaking external code. Plans referenced by SPIPlanPtr contain parsed, analyzed, and optimized representations of SQL statements that can be executed multiple times with different parameter values, providing significant performance benefits for repeated operations.

## Parameters / Member Variables
This is an opaque pointer type - the internal structure members are not exposed to external users. The actual _SPI_plan structure contains internal implementation details for plan management, caching, and execution.

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_plan](_SPI_plan.md) (internal structure, not directly accessible)

- Called from (representative examples):
  - [SPI_execute_plan](SPI_execute_plan.md)
  - [SPI_execute_plan_extended](SPI_execute_plan_extended.md)
  - [SPI_execute_plan_with_paramlist](SPI_execute_plan_with_paramlist.md)
  - [SPI_execute_snapshot](SPI_execute_snapshot.md)
  - [SPI_execute_with_args](SPI_execute_with_args.md)
  - [SPI_prepare](SPI_prepare.md)
  - [SPI_prepare_cursor](SPI_prepare_cursor.md)
  - [SPI_prepare_extended](SPI_prepare_extended.md)
  - [SPI_prepare_params](SPI_prepare_params.md)
  - [SPI_keepplan](SPI_keepplan.md)
  - [SPI_saveplan](SPI_saveplan.md)
  - [SPI_freeplan](SPI_freeplan.md)
  - [SPI_cursor_open](SPI_cursor_open.md)
  - [SPI_cursor_open_with_paramlist](SPI_cursor_open_with_paramlist.md)
  - [SPI_getargtypeid](SPI_getargtypeid.md)
  - [SPI_getargcount](SPI_getargcount.md)
  - [SPI_is_cursor_plan](SPI_is_cursor_plan.md)
  - [SPI_plan_is_valid](SPI_plan_is_valid.md)
  - [SPI_plan_get_plan_sources](SPI_plan_get_plan_sources.md)
  - [SPI_plan_get_cached_plan](SPI_plan_get_cached_plan.md)
  - Various referential integrity functions
  - Procedural language implementations (PL/Perl, PL/Python, PL/Tcl)

## Notes and Other Information
- The opaque design prevents direct access to plan internals, ensuring API stability and encapsulation
- Plans can be saved across transaction boundaries using SPI_keepplan or SPI_saveplan
- Proper resource management requires calling SPI_freeplan when plans are no longer needed
- Plans can be interrogated for metadata using functions like SPI_getargtypeid and SPI_getargcount
- The type is extensively used throughout PostgreSQL's procedural language implementations
- Plans support parameter binding for efficient repeated execution with different values
- Cursor plans can be identified using SPI_is_cursor_plan
- [Plan](../P/Plan.md) validity can be checked using SPI_plan_is_valid, which is important for long-lived plans
- The referential integrity system extensively uses SPIPlanPtr for cached constraint checking plans
- This abstraction enables sophisticated plan caching and optimization strategies without exposing complexity to users