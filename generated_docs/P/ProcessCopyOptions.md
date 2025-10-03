# ProcessCopyOptions

## Location
[src/backend/commands/copy.c:463-895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copy.c#L463-L895)

## Overview
ProcessCopyOptions processes and validates the complete option list for COPY statements, parsing individual DefElem options into a structured CopyFormatOptions output while performing comprehensive compatibility and consistency checking.

## Definition

```c
void
ProcessCopyOptions(ParseState *pstate,
				   CopyFormatOptions *opts_out,
				   bool is_from,
				   List *options)
```
## Detailed Description
ProcessCopyOptions is the central option processing function for COPY statements, handling the parsing, validation, and normalization of all COPY options. It iterates through a list of DefElem options, extracting values and storing them in a CopyFormatOptions structure. The function performs extensive validation including: detecting conflicting option specifications, enforcing format-specific restrictions (e.g., CSV-only options), validating character constraints (single-byte requirements, forbidden characters), and ensuring directional compatibility (COPY FROM vs COPY TO restrictions). It also sets appropriate defaults for omitted options and performs cross-option validation to ensure the final configuration is internally consistent and operationally valid.

## Parameters / Member Variables
- `*pstate`: ParseState for generating error messages with precise source location information
- `*opts_out`: Output CopyFormatOptions structure to populate with processed option values (can be NULL for external validation)
- `is_from`: Boolean flag indicating COPY FROM (true) vs COPY TO (false) for directional option validation
- `*options`: List of DefElem structures containing the raw COPY option specifications from the parser
## Dependencies
- Functions called/Symbols referenced:
  - [defGetString](../d/defGetString.md)
  - [defGetBoolean](../d/defGetBoolean.md)  
  - [defGetCopyHeaderChoice](../d/defGetCopyHeaderChoice.md)
  - [defGetCopyOnErrorChoice](../d/defGetCopyOnErrorChoice.md)
  - [defGetCopyLogVerbosityChoice](../d/defGetCopyLogVerbosityChoice.md)
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md)
  - [pg_char_to_encoding](../p/pg_char_to_encoding.md)
  - ereport/parser_errposition
  - [palloc0](../p/palloc0.md)
  - strlen/strchr/strncmp
- Called from (representative examples):
  - [BeginCopyFrom](../B/BeginCopyFrom.md)
  - [BeginCopyTo](../B/BeginCopyTo.md)

## Notes and Other Information
- Supports external API usage by allowing opts_out to be NULL for option validation without result storage
- Enforces strict single-byte character requirements for delimiters, quotes, and escape characters
- Prohibits newline and carriage return characters in delimiter, null, and default representations
- Implements comprehensive format-mode restrictions (binary mode limitations, CSV-only options)
- Sets intelligent defaults: tab delimiter for text mode, comma for CSV; backslash-N for text null, empty string for CSV null
- Validates character conflicts: delimiter cannot appear in null/default strings, quote character restrictions
- Contains undocumented 'convert_selectively' option for internal use in binary format processing
- Performs both individual option validation and cross-option compatibility checking to ensure operational consistency

## Simplified Source
```c
void ProcessCopyOptions(ParseState *pstate, CopyFormatOptions *opts_out,
                       bool is_from, List *options) {
    bool format_specified = false;
    bool freeze_specified = false;
    bool header_specified = false;
    bool on_error_specified = false;
    bool log_verbosity_specified = false;
    ListCell *option;

    // Initialize output structure if needed
    if (opts_out == NULL) {
        opts_out = (CopyFormatOptions *) palloc0(sizeof(CopyFormatOptions));
    }
    opts_out->file_encoding = -1;

    // Process each option in the list
    foreach(option, options) {
        DefElem *defel = lfirst_node(DefElem, option);

        if (strcmp(defel->defname, "format") == 0) {
            // Handle format option (text, csv, binary)
            if (format_specified) errorConflictingDefElem(defel, pstate);
            format_specified = true;
            char *fmt = defGetString(defel);
            if (strcmp(fmt, "csv") == 0) opts_out->csv_mode = true;
            else if (strcmp(fmt, "binary") == 0) opts_out->binary = true;
            // ... other format processing
        }
        else if (strcmp(defel->defname, "delimiter") == 0) {
            // Handle delimiter option
            if (opts_out->delim) errorConflictingDefElem(defel, pstate);
            opts_out->delim = defGetString(defel);
        }
        else if (strcmp(defel->defname, "null") == 0) {
            // Handle null representation option
            if (opts_out->null_print) errorConflictingDefElem(defel, pstate);
            opts_out->null_print = defGetString(defel);
        }
        // ... process other options (quote, escape, force_quote, etc.)
        else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("option \"%s\" not recognized", defel->defname),
                           parser_errposition(pstate, defel->location)));
        }
    }

    // Validate option compatibility
    if (opts_out->binary && opts_out->delim) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("cannot specify %s in BINARY mode", "DELIMITER")));
    }
    // ... other compatibility checks

    // Set defaults for omitted options
    if (!opts_out->delim) {
        opts_out->delim = opts_out->csv_mode ? "," : "\t";
    }
    if (!opts_out->null_print) {
        opts_out->null_print = opts_out->csv_mode ? "" : "\\N";
    }
    opts_out->null_print_len = strlen(opts_out->null_print);

    // Validate character constraints (single-byte, no newlines, etc.)
    if (strlen(opts_out->delim) != 1) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("COPY delimiter must be a single one-byte character")));
    }
    // ... additional validation checks
}
```