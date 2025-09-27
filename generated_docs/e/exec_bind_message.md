# exec_bind_message

## Location
[src/backend/tcop/postgres.c:1630-2100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L1630-L2100)

## Overview
Processes a "Bind" message to create a portal from a prepared statement, binding parameter values and format specifications for subsequent execution.

## Definition

```c
structure
	 * that expects to run inside a valid transaction.  We also disallow
	 * binding any parameters, since we can't risk calling user-defined I/O
	 * functions.
	 */
	if (IsAbortedTransactionBlockState() &&
		(!(psrc->raw_parse_tree &&
		   IsTransactionExitStmt(psrc->raw_parse_tree->stmt)) ||
		 numParams != 0))
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block"),
				 errdetail_abort()));
```
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
  - [CreatePortal](../C/CreatePortal.md) (create new portal for execution)
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

## Simplified Source

```c
// Simplified version of exec_bind_message
static void
exec_bind_message(StringInfo input_message)
{
    const char *portal_name;
    const char *stmt_name;
    int numParams;
    CachedPlanSource *psrc;
    CachedPlan *cplan;
    Portal portal;
    ParamListInfo params = NULL;
    bool snapshot_set = false;

    // Step 1: Extract portal and statement names from message
    portal_name = pq_getmsgstring(input_message);
    stmt_name = pq_getmsgstring(input_message);

    // Step 2: Find the prepared statement
    if (stmt_name[0] != '\0') {
        PreparedStatement *pstmt = FetchPreparedStatement(stmt_name, true);
        psrc = pstmt->plansource;
    } else {
        psrc = unnamed_stmt_psrc;  // Use unnamed statement
        if (!psrc)
            ereport(ERROR, "unnamed prepared statement does not exist");
    }

    // Step 3: Setup monitoring and transaction context
    pgstat_report_activity(STATE_RUNNING, psrc->query_string);
    start_xact_command();

    // Step 4: Parse parameter format codes
    int numPFormats = pq_getmsgint(input_message, 2);
    int16 *pformats = NULL;
    if (numPFormats > 0) {
        pformats = palloc_array(int16, numPFormats);
        for (int i = 0; i < numPFormats; i++)
            pformats[i] = pq_getmsgint(input_message, 2);
    }

    // Step 5: Validate parameter count
    numParams = pq_getmsgint(input_message, 2);
    if (numParams != psrc->num_params)
        ereport(ERROR, "parameter count mismatch");

    // Step 6: Check transaction state for safety
    if (IsAbortedTransactionBlockState() &&
        (!IsTransactionExitStmt(psrc->raw_parse_tree->stmt) || numParams != 0))
        ereport(ERROR, "transaction aborted, commands ignored");

    // Step 7: Create portal
    if (portal_name[0] == '\0')
        portal = CreatePortal(portal_name, true, true);   // unnamed portal
    else
        portal = CreatePortal(portal_name, false, false); // named portal

    // Step 8: Setup snapshot if needed for parameter processing
    if (numParams > 0 || analyze_requires_snapshot(psrc->raw_parse_tree)) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    // Step 9: Process parameters if any
    if (numParams > 0) {
        params = makeParamList(numParams);

        for (int paramno = 0; paramno < numParams; paramno++) {
            Oid ptype = psrc->param_types[paramno];
            int32 plength = pq_getmsgint(input_message, 4);
            bool isNull = (plength == -1);

            // Determine parameter format (text=0, binary=1)
            int16 pformat = (numPFormats > 1) ? pformats[paramno] :
                           (numPFormats > 0) ? pformats[0] : 0;

            if (!isNull) {
                char *pvalue = pq_getmsgbytes(input_message, plength);

                if (pformat == 0) {  // Text format
                    // Convert encoding and call input function
                    char *pstring = pg_client_to_server(pvalue, plength);
                    Datum pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);
                    params->params[paramno].value = pval;
                } else {  // Binary format
                    // Call binary receive function
                    StringInfo bufptr = create_buffer_from_bytes(pvalue, plength);
                    Datum pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);
                    params->params[paramno].value = pval;
                }
            }

            params->params[paramno].isnull = isNull;
            params->params[paramno].ptype = ptype;
        }
    }

    // Step 10: Parse result format codes
    int numRFormats = pq_getmsgint(input_message, 2);
    int16 *rformats = NULL;
    if (numRFormats > 0) {
        rformats = palloc_array(int16, numRFormats);
        for (int i = 0; i < numRFormats; i++)
            rformats[i] = pq_getmsgint(input_message, 2);
    }

    // Step 11: Get cached plan and define portal
    cplan = GetCachedPlan(psrc, params, NULL, NULL);
    PortalDefineQuery(portal, stmt_name, psrc->query_string,
                     psrc->commandTag, cplan->stmt_list, cplan);

    // Step 12: Cleanup snapshot and start portal execution
    if (snapshot_set)
        PopActiveSnapshot();

    PortalStart(portal, params, 0, InvalidSnapshot);
    PortalSetResultFormat(portal, numRFormats, rformats);

    // Step 13: Send completion message to client
    if (whereToSendOutput == DestRemote)
        pq_putemptymessage(PqMsg_BindComplete);
}
```

Key simplifications made:
- Removed detailed error handling callbacks and context management for clarity
- Consolidated parameter format determination logic
- Abstracted complex memory context switching operations
- Simplified parameter processing loop by focusing on core conversion logic
- Removed verbose logging and statistics collection code
- Focused on the main execution path while preserving essential validation
- Eliminated platform-specific optimizations and detailed encoding handling
- Streamlined the overall flow into clear, numbered steps