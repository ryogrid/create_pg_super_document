# SPI_saveplan

## Location
[src/backend/executor/spi.c:1003-1024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1003-L1024)

## Overview
Creates a saved copy of an SPI execution plan that persists beyond the current memory context, allowing the plan to be reused across different procedure calls and contexts.

## Definition
```c
SPIPlanPtr SPI_saveplan(SPIPlanPtr plan)
```

## Detailed Description
SPI_saveplan creates a persistent copy of an existing SPI execution plan by copying it to a long-lived memory context. Unlike SPI_keepplan which modifies the original plan in-place, SPI_saveplan creates a completely new plan structure that is independent of the original. The new plan is allocated in a memory context that will survive beyond the current procedure context, making it suitable for caching and reuse across multiple SPI operations. This function is particularly useful when you need to preserve a plan but also want to keep the original plan with its original lifetime semantics intact.

## Parameters / Member Variables
- `plan`: Pointer to the existing SPI execution plan to be copied and saved

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_PLAN_MAGIC
  - [_SPI_begin_call](_SPI_begin_call.md)
  - [_SPI_save_plan](_SPI_save_plan.md)
  - [_SPI_end_call](_SPI_end_call.md)
  - SPI_ERROR_ARGUMENT
- Called from (representative examples):
  - Functions using SPI_OPT_NONATOMIC option

## Notes and Other Information
- Returns a new SPIPlanPtr on success, NULL on failure with SPI_result set to error code
- The original plan remains unchanged and retains its original lifetime semantics
- The returned plan must be freed with SPI_freeplan when no longer needed
- Unlike SPI_keepplan, this creates a separate copy rather than modifying the original
- The function validates the input plan has the correct magic number before proceeding
- Uses _SPI_begin_call with false parameter to avoid changing the current memory context
- The saved plan is allocated in a persistent memory context suitable for long-term storage
- Commonly used when building plan caches or when multiple contexts need access to the same plan