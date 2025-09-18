# check_spi_usage_allowed

## Location
src/pl/plperl/plperl.c: 3106 - 3132

## Overview
Validates that SPI (Server Programming Interface) functions can be safely called in the current execution context within PL/Perl.

## Definition


## Detailed Description
This function performs critical safety checks before allowing SPI operations in PL/Perl. It prevents SPI usage in two dangerous scenarios:
1. During PL/Perl cleanup (END blocks) when PostgreSQL infrastructure may be partially torn down
2. During function compilation when the execution context is not fully established

The function uses Perl's croak() function to immediately terminate execution if SPI usage is not allowed, preventing potential crashes or undefined behavior.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - croak (Perl function for fatal errors)
- Global variables referenced:
  - plperl_ending (indicates if PL/Perl is in cleanup phase)
  - current_call_data (current execution context)
- Called from (representative examples):
  - plperl_sv_to_literal
  - plperl_spi_exec
  - plperl_spi_execute_fetch_result
  - plperl_return_next
  - plperl_spi_query
  - plperl_spi_fetchrow
  - plperl_spi_cursor_close
  - plperl_spi_prepare
  - plperl_spi_exec_prepared
  - plperl_spi_query_prepared
  - plperl_spi_freeplan
  - plperl_spi_commit
  - plperl_spi_rollback

## Notes and Other Information
- Essential safety mechanism that prevents crashes and undefined behavior
- Called at the beginning of most SPI-related functions in PL/Perl
- Uses simple croak() rather than ereport() to avoid invoking PostgreSQL error handling infrastructure during unsafe states
- Protects against code execution during function validation, which could be a security concern
- The function compilation check prevents dereferencing NULL prodesc pointers