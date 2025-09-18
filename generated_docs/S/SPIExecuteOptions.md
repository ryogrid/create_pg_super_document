# SPIExecuteOptions

## Location
src/include/executor/spi.h: 46 - 55

## Overview
SPIExecuteOptions is a structure that provides optional configuration parameters for SPI statement execution functions, controlling execution behavior, parameter binding, result handling, and resource management.

## Definition


## Detailed Description
SPIExecuteOptions serves as a comprehensive configuration structure for SPI statement execution operations. It provides fine-grained control over various aspects of statement execution including parameter binding, transaction behavior, result expectations, tuple counting limits, result destination handling, and resource ownership. This structure enables advanced SPI usage scenarios while maintaining backward compatibility through optional parameter patterns.

The structure is designed to accommodate both simple and complex execution scenarios, from basic read-only queries to sophisticated operations requiring custom result handling and non-atomic execution modes. It centralizes execution configuration to avoid function signature proliferation.

## Parameters / Member Variables
- : ParamListInfo structure containing parameter values to bind to the prepared statement during execution
- : Boolean flag indicating whether the statement should be executed in read-only mode, preventing modifications
- : Boolean flag allowing execution outside of atomic contexts (useful for certain procedural language scenarios)
- : Boolean flag indicating that the statement must return tuple data (used for validation and optimization)
- : Maximum number of tuples to return or process (0 means no limit)
- : Custom DestReceiver for directing query results to alternative destinations instead of the default SPI tuple table
- : ResourceOwner for managing resource cleanup and ownership tracking during execution

## Dependencies
- Functions called/Symbols referenced:
  - ParamListInfo
  - DestReceiver
  - ResourceOwner

- Called from (representative examples):
  - SPI_execute
  - SPI_execute_extended
  - SPI_execute_plan
  - SPI_execute_plan_extended
  - SPI_execute_plan_with_paramlist
  - SPI_execute_snapshot
  - SPI_execute_with_args
  - _SPI_execute_plan

## Notes and Other Information
- This structure is the primary mechanism for configuring advanced SPI execution behavior
- The allow_nonatomic flag is particularly important for procedural languages that need to execute statements outside transaction blocks
- Custom DestReceiver usage allows for streaming results or alternative result processing without materializing full tuple tables
- The tcount parameter provides memory protection against runaway queries in procedural language contexts
- Resource ownership tracking helps ensure proper cleanup in complex execution scenarios
- All fields are optional and can be initialized to appropriate defaults (NULL/false/0) when not needed
- The structure design allows for future extension without breaking existing code