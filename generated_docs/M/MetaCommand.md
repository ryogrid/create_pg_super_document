# MetaCommand

## Location
[src/bin/pgbench/pgbench.c:703-704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L703-L704)

## Overview
The MetaCommand enumeration defines all supported meta-commands (backslash commands) that can be used within pgbench scripts for control flow, variable management, and pipeline operations.

## Definition


## Detailed Description
The MetaCommand enumeration provides a comprehensive catalog of meta-commands that extend pgbench's scripting capabilities beyond basic SQL execution. These commands enable advanced benchmark scenarios including variable manipulation, conditional execution, shell integration, timing control, and database query pipelining.

Meta-commands are distinguished from regular SQL by their backslash prefix and provide pgbench-specific functionality that is processed during script execution. The enumeration serves as the basis for command parsing and routing to appropriate execution handlers throughout the pgbench engine.

The commands are grouped into several functional categories: variable operations (SET, SETSHELL, GSET, ASET), control flow (IF, ELIF, ELSE, ENDIF), timing (SLEEP), system integration (SHELL), and performance optimization (pipeline commands).

## Parameters / Member Variables
- : Indicates an unrecognized or invalid meta-command
- : Variable assignment with literal values or expressions  
- : Variable assignment from shell command output
- : Execute shell commands during benchmark execution
- : Introduce delays between operations for timing control
- : Assign query result values to variables (single row)
- : Assign query result values to variables (array form)
- : Begin conditional execution block based on expression evaluation
- : Alternative condition in conditional execution block
- : Default branch in conditional execution block  
- : Terminate conditional execution block
- : Begin database query pipelining for performance optimization
- : Synchronize pipeline operations and collect results
- : Terminate pipeline mode and return to normal execution

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references - used as enum constants)
- Called from (representative examples):
  - [Command](../C/Command.md) (structure that contains MetaCommand field)
  - [evaluateExpr](../e/evaluateExpr.md) (for expression evaluation in meta-commands)
  - [getMetaCommand](../g/getMetaCommand.md) (for parsing and identifying meta-commands)
  - [readCommandResponse](../r/readCommandResponse.md) (for processing meta-command responses)

## Notes and Other Information
- Located in src/bin/pgbench/pgbench.c at lines 687-703
- Essential component of pgbench's scripting language and execution engine
- Each meta-command corresponds to a specific backslash command syntax in pgbench scripts
- Pipeline commands (STARTPIPELINE, SYNCPIPELINE, ENDPIPELINE) enable advanced PostgreSQL pipeline mode for improved performance
- Conditional commands (IF, ELIF, ELSE, ENDIF) provide branching logic for complex benchmark scenarios
- [Variable](../V/Variable.md) commands (SET, SETSHELL, GSET, ASET) enable dynamic benchmark parameterization
- SHELL command allows integration with external tools and system commands during benchmarking
- SLEEP command provides precise timing control for rate-limited or scheduled operations
- META_NONE serves as a sentinel value for error handling and unknown command detection