addScript

## Overview
Appends a parsed SQL script to the global list of scripts to be processed during pgbench execution.

## Definition
static void addScript(const ParsedScript *script)

## Detailed Description
The addScript function is responsible for adding a pre-parsed SQL script to the global array sql_script[] that pgbench uses to store all scripts that will be executed during benchmarking. The function performs several validation checks before adding the script to ensure the script is valid and that the maximum number of allowed scripts has not been exceeded. It also validates conditional logic within the script before acceptance.

## Parameters / Member Variables
- script: A pointer to a ParsedScript structure containing the parsed SQL commands and metadata for the script to be added

## Dependencies
- Functions called/Symbols referenced:
  - [pg_fatal](../p/pg_fatal.md) - Error reporting function when validation fails
  - [CheckConditional](../C/CheckConditional.md) - Validates conditional logic in the script
  - [ParsedScript](../P/ParsedScript.md) - Structure type for the script parameter
  - MAX_SCRIPTS - Constant defining maximum allowed scripts
- Called from (representative examples):
  - Function at line 6064 in pgbench.c (within script processing logic)

## Notes and Other Information
- The function validates that the script contains at least one command before adding it
- Enforces a limit of MAX_SCRIPTS total scripts that can be registered
- Uses pg_fatal() to terminate the program if validation fails
- The function increments the global num_scripts counter after successfully adding a script
- Part of pgbench script management system for handling multiple SQL workloads

## Simplified Source

```c
static void
addScript(const ParsedScript *script)
{
    // Validate script has at least one command
    if (script->commands == NULL || script->commands[0] == NULL)
        pg_fatal("empty command list for script \"%s\"", script->desc);

    // Check maximum script limit
    if (num_scripts >= MAX_SCRIPTS)
        pg_fatal("at most %d SQL scripts are allowed", MAX_SCRIPTS);

    // Validate conditional structure in script
    CheckConditional(script);

    // Add script to global array and increment counter
    sql_script[num_scripts] = *script;
    num_scripts++;
}
```