# exec_bind_message

## Location
src/backend/tcop/postgres.c: 1630 - 2100

## Overview
Processes a "Bind" message to create a portal from a prepared statement, binding parameter values and format specifications for subsequent execution.

## Definition


## Detailed Description
This function implements the Bind phase of PostgreSQL's extended query protocol. It creates a portal (execution context) from a previously prepared statement by binding actual parameter values and result format specifications. The function handles both named and unnamed prepared statements and portals.

Key responsibilities include:
- Extracting portal and statement names from the protocol message
- Fetching the corresponding prepared statement and its cached plan source
- Parsing and validating parameter format codes and values
- Converting parameters between text/binary formats as needed
- Creating and configuring a portal for execution
- Setting up result format specifications
- Comprehensive error handling with detailed parameter logging

The function performs extensive validation including parameter count matching, format code validation, transaction state checks, and handles both text and binary parameter formats with proper encoding conversion.

## Parameters / Member Variables
- : StringInfo containing the complete Bind protocol message with portal name, statement name, parameter formats, parameter values, and result format codes

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgstring](../p/pq_getmsgstring.md) (extract string fields from protocol message)
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md) (retrieve named prepared statement)
  - CreatePortal (create new portal for execution)
  - [GetCachedPlan](../G/GetCachedPlan.md) (obtain execution plan from cached plan source)
  - [PortalDefineQuery](../P/PortalDefineQuery.md) (associate query with portal)
  - [PortalStart](../P/PortalStart.md) (initialize portal for execution)
  - [PortalSetResultFormat](../P/PortalSetResultFormat.md) (configure result format specifications)
  - [makeParamList](../m/makeParamList.md) (create parameter list structure)
  - [OidInputFunctionCall](../O/OidInputFunctionCall.md)/OidReceiveFunctionCall (parameter type conversion)
  - [pgstat_report_activity](../p/pgstat_report_activity.md) (activity monitoring)
  - [check_log_duration](../c/check_log_duration.md) (duration logging)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (main message processing loop)

## Notes and Other Information
- Supports both text (format code 0) and binary (format code 1) parameter formats
- Implements comprehensive parameter logging for debugging when configured
- Handles encoding conversion for text parameters using pg_client_to_server
- Performs transaction state validation, rejecting non-COMMIT/ROLLBACK commands in aborted transactions
- Memory management ensures parameter data is stored in the portal's memory context
- Integrates with PostgreSQL's error callback system for detailed parameter error reporting
- Sends BindComplete message to client upon successful completion
- Supports parameter value truncation for logging when log_parameter_max_length_on_error is configured