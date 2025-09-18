# describeConfigurationParameters

## Location
[src/bin/psql/describe.c:4546-4613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4546-L4613)

## Overview
Describes PostgreSQL configuration parameters and their current values, corresponding to the psql \dconfig command.

## Definition
bool describeConfigurationParameters(const char *pattern, bool verbose, bool showSystem)

## Detailed Description
The describeConfigurationParameters function generates and executes a SQL query to retrieve information about PostgreSQL configuration parameters from the pg_catalog.pg_settings system view. It displays parameter names and their current values using the current_setting() function. When verbose mode is enabled, it includes additional details such as parameter type, context, and access privileges (for PostgreSQL 15+). When no pattern is provided, it filters to show only non-default parameters (where the current setting differs from the boot value). The function can optionally join with pg_parameter_acl to show access control information for parameters in newer PostgreSQL versions.

## Parameters / Member Variables
- `pattern`: Optional SQL pattern to filter parameter names (can be NULL to show only non-default parameters)
- `verbose`: Boolean flag to include extended information (type, context, ACLs) in the output
- `showSystem`: Boolean flag (parameter present but not used in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [processSQLNamePattern](../p/processSQLNamePattern.md)
  - [PSQLexec](../P/PSQLexec.md)
  - termPQExpBuffer
  - [printQuery](../p/printQuery.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher)

## Notes and Other Information
- Implements the \dconfig psql meta-command functionality
- Queries the pg_catalog.pg_settings system view
- Uses pg_catalog.current_setting() to retrieve current parameter values
- When no pattern is specified, shows only non-default parameters (source <> 'default' AND setting IS DISTINCT FROM boot_val)
- For PostgreSQL 15+, joins with pg_parameter_acl to show access privileges in verbose mode
- Uses processSQLNamePattern for flexible pattern matching on parameter names
- Returns false on query execution failure, true on success
- Output title changes based on whether a pattern was provided
- Results are ordered by parameter name for consistent presentation
- The showSystem parameter is accepted but not currently used in the function logic