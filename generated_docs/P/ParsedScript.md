# ParsedScript

## Location
[src/bin/pgbench/pgbench.c:756-762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L756-L762)

## Overview
The ParsedScript structure represents a complete, parsed pgbench script containing all its commands, metadata, and execution statistics for weighted script selection and performance tracking.

## Definition
```c
typedef struct ParsedScript
{
    const char *desc;         /* script descriptor (eg, file name) */
    int         weight;       /* selection weight */
    Command   **commands;     /* NULL-terminated array of Commands */
    StatsData   stats;        /* total time spent in script */
} ParsedScript;
```

## Detailed Description
This structure encapsulates a complete pgbench script after parsing, providing a container for all commands within the script along with metadata for script identification and selection. The structure supports pgbench's weighted script selection feature, allowing multiple scripts to be executed with different probabilities based on their assigned weights. It also maintains comprehensive performance statistics for the entire script execution.

## Parameters / Member Variables
- `desc`: Human-readable script descriptor, typically the file name or identifier, used for logging and error reporting
- `weight`: Numerical weight value used in weighted random selection when multiple scripts are available (higher weights increase selection probability)
- `commands`: NULL-terminated array of pointers to Command structures representing all commands in the script in execution order
- `stats`: StatsData structure containing cumulative performance statistics and execution metrics for the entire script

## Dependencies
- Functions called/Symbols referenced:
  - [Command](../C/Command.md)
  - [StatsData](../S/StatsData.md)
- Called from (representative examples):
  - [allocCStatePrepared](../a/allocCStatePrepared.md)
  - [CheckConditional](../C/CheckConditional.md)
  - [ParseScript](ParseScript.md)
  - [addScript](../a/addScript.md)

## Notes and Other Information
ParsedScript serves as the primary container for script execution in pgbench, supporting both single-script and multi-script benchmark scenarios. The weight mechanism enables sophisticated workload modeling where different transaction patterns can be executed with controlled frequency distributions. The structure maintains script-level statistics that can be aggregated for comprehensive performance analysis across different script types within a single benchmark run.