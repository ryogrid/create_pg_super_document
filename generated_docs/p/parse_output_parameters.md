# parse_output_parameters

## Location
[src/backend/replication/pgoutput/pgoutput.c:283-424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L283-L424)

## Overview
A static helper function that parses and validates configuration parameters for the pgoutput logical replication plugin during startup.

## Definition
```c
static void parse_output_parameters(List *options, PGOutputData *data)
```

## Detailed Description
This function processes the parameter list passed to the pgoutput plugin and populates the `PGOutputData` structure with the parsed configuration values. It handles all supported pgoutput plugin options including protocol version, publication names, binary mode, message handling, streaming mode, two-phase commit support, and origin filtering. The function performs comprehensive validation including duplicate parameter detection, range checking for numeric values, and syntax validation for complex parameters like publication names. It sets appropriate defaults for optional parameters and ensures required parameters are provided.

## Parameters / Member Variables
- `options`: List of `DefElem` structures containing the configuration options passed to the plugin
- `data`: Pointer to `PGOutputData` structure that will be populated with parsed parameter values

## Dependencies
- Functions called/Symbols referenced:
  - [PGOutputData](../P/PGOutputData.md) (output plugin data structure)
  - LOGICALREP_STREAM_OFF (streaming mode constant)
  - [DefElem](../D/DefElem.md) (definition element structure)
  - [String](../S/String.md) (PostgreSQL string node type)
  - PG_UINT32_MAX (maximum uint32 value constant)
  - [SplitIdentifierString](../S/SplitIdentifierString.md) (utility function to parse comma-separated identifiers)
  - [defGetBoolean](../d/defGetBoolean.md) (utility function to extract boolean values from DefElem)
  - [defGetStreamingMode](../d/defGetStreamingMode.md) (utility function to extract streaming mode from DefElem)
  - [defGetString](../d/defGetString.md) (utility function to extract string values from DefElem)
  - LOGICALREP_ORIGIN_NONE (origin filtering constant for no origin)
  - LOGICALREP_ORIGIN_ANY (origin filtering constant for any origin)
- Called from:
  - [pgoutput_startup](pgoutput_startup.md) (plugin startup function)

## Notes and Other Information
- Validates that required parameters "proto_version" and "publication_names" are provided
- Prevents duplicate parameter specifications by tracking which options have been given
- Supports protocol version validation with range checking
- Handles complex publication_names syntax using comma-separated identifier parsing
- Provides boolean options for binary, messages, and two_phase features
- Supports streaming mode configuration with multiple possible values
- Implements origin filtering with "none" and "any" options
- Uses PostgreSQL's standard error reporting mechanisms for validation failures
- Sets sensible defaults: binary=false, streaming=off, messages=false, two_phase=false

## Simplified Source

```c
static void parse_output_parameters(List *options, PGOutputData *data) {
    ListCell *lc;
    bool flags[7] = {false}; // Track duplicate options

    // Set defaults
    data->binary = false;
    data->streaming = LOGICALREP_STREAM_OFF;
    data->messages = false;
    data->two_phase = false;

    // Parse each option
    foreach(lc, options) {
        DefElem *defel = (DefElem *) lfirst(lc);

        if (strcmp(defel->defname, "proto_version") == 0) {
            if (flags[0]) ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                         errmsg("conflicting or redundant options")));
            flags[0] = true;
            // Parse and validate protocol version
            unsigned long parsed = strtoul(strVal(defel->arg), &endptr, 10);
            if (errno != 0 || *endptr != '\0' || parsed > PG_UINT32_MAX)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("invalid proto_version")));
            data->protocol_version = (uint32) parsed;
        }
        else if (strcmp(defel->defname, "publication_names") == 0) {
            if (flags[1]) ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                         errmsg("conflicting or redundant options")));
            flags[1] = true;
            if (!SplitIdentifierString(strVal(defel->arg), ',', &data->publication_names))
                ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
                               errmsg("invalid publication_names syntax")));
        }
        // Handle other boolean options (binary, messages, streaming, two_phase, origin)
        // ... similar pattern for each option type
        else {
            elog(ERROR, "unrecognized pgoutput option: %s", defel->defname);
        }
    }

    // Validate required options
    if (!flags[0] || !flags[1])
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("required option missing")));
}
```