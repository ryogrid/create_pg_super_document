# check_spi_usage_allowed

## Location
[src/pl/plperl/plperl.c:3106-3132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3106-L3132)

## Overview
Validates that SPI (Server Programming Interface) functions can be safely called in the current execution context within PL/Perl.

## Definition

```c
static void
check_spi_usage_allowed(void)
```
## Detailed Description
This function performs critical safety checks before allowing SPI operations in PL/Perl. It prevents SPI usage in two dangerous scenarios:
1. During PL/Perl cleanup (END blocks) when PostgreSQL infrastructure may be partially torn down
2. During function compilation when the execution context is not fully established

The function uses Perl's croak() function to immediately terminate execution if SPI usage is not allowed, preventing potential crashes or undefined behavior.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - croak (Perl function for fatal errors)
- Global variables referenced:
  - plperl_ending (indicates if PL/Perl is in cleanup phase)
  - current_call_data (current execution context)
- Called from (representative examples):
  - [plperl_sv_to_literal](../p/plperl_sv_to_literal.md)
  - [plperl_spi_exec](../p/plperl_spi_exec.md)
  - [plperl_spi_execute_fetch_result](../p/plperl_spi_execute_fetch_result.md)
  - [plperl_return_next](../p/plperl_return_next.md)
  - [plperl_spi_query](../p/plperl_spi_query.md)
  - [plperl_spi_fetchrow](../p/plperl_spi_fetchrow.md)
  - [plperl_spi_cursor_close](../p/plperl_spi_cursor_close.md)
  - [plperl_spi_prepare](../p/plperl_spi_prepare.md)
  - [plperl_spi_exec_prepared](../p/plperl_spi_exec_prepared.md)
  - [plperl_spi_query_prepared](../p/plperl_spi_query_prepared.md)
  - [plperl_spi_freeplan](../p/plperl_spi_freeplan.md)
  - [plperl_spi_commit](../p/plperl_spi_commit.md)
  - [plperl_spi_rollback](../p/plperl_spi_rollback.md)

## Notes and Other Information
- Essential safety mechanism that prevents crashes and undefined behavior
- Called at the beginning of most SPI-related functions in PL/Perl
- Uses simple croak() rather than ereport() to avoid invoking PostgreSQL error handling infrastructure during unsafe states
- Protects against code execution during function validation, which could be a security concern
- The function compilation check prevents dereferencing NULL prodesc pointers

## Simplified Source

```c
static void
check_spi_usage_allowed(void)
{
    // Don't allow SPI during PL/Perl cleanup
    if (plperl_ending) {
        croak("SPI functions can not be used in END blocks");
    }

    // Don't allow SPI during function compilation
    if (current_call_data == NULL || current_call_data->prodesc == NULL) {
        croak("SPI functions can not be used during function compilation");
    }
}
```