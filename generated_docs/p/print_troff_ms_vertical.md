# print_troff_ms_vertical

## Location
[src/fe_utils/print.c:2919-3038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2919-L3038)

## Overview
A function that renders tabular data in vertical format using troff -ms markup, displaying each record as a series of field-value pairs rather than traditional horizontal rows.

## Definition
```c
static void print_troff_ms_vertical(const printTableContent *cont, FILE *fout)
```

## Detailed Description
This function formats tabular data in a vertical layout using troff -ms macros, where each record is displayed as a list of field-value pairs. Unlike horizontal table formatting, this approach is better suited for wide tables or detailed record inspection. The function handles:

1. **Record grouping**: Each record is labeled with "Record N" headers in italics
2. **Field-value pairing**: Each data cell is paired with its corresponding column header
3. **Dynamic formatting**: Uses .T& (table continuation) directives to change table formats mid-stream
4. **Border handling**: Supports different border styles with appropriate separator lines
5. **Format tracking**: Maintains state to optimize troff table format changes

The vertical format uses two-column troff tables where the first column contains field names (headers) and the second contains the corresponding values. Record boundaries are clearly marked, and formatting adapts based on border settings.

## Parameters / Member Variables
- `cont`: Pointer to printTableContent structure containing table data, formatting options, headers, cells, and metadata
- `fout`: Output file stream where the troff-formatted vertical table will be written

## Dependencies
- Functions called/Symbols referenced:
  - [troff_ms_escaped_print](../t/troff_ms_escaped_print.md) (for escaping text content)
  - fputs, fputc, fprintf (standard C library functions)
- Called from (representative examples):
  - [printTable](printTable.md) (at src/fe_utils/print.c:3525)

## Notes and Other Information
- This is a static function, accessible only within src/fe_utils/print.c
- Uses a state machine approach with `current_format` to track table format changes (0=none, 1=header, 2=body)
- Record numbering starts from `cont->opt->prior_records + 1` to support pagination
- Handles three border levels: 0 (no borders), 1 (field separators), 2 (full box with record separators)
- Uses troff table continuation (.T&) to dynamically change column specifications
- Field names and values are both escaped using troff_ms_escaped_print
- Supports cancellation via global `cancel_pressed` variable
- Column specifications: "c l" (center, left), "c s" (center, span), "c | l" (center, vertical bar, left)
- Part of PostgreSQL's frontend printing subsystem for generating readable formatted output
- Particularly useful for displaying wide tables or detailed record examination

## Simplified Source

```c
static void print_troff_ms_vertical(const printTableContent *table_content, FILE *output_file) {
    bool tuples_only = table_content->opt->tuples_only;
    unsigned short border_level = table_content->opt->border;
    unsigned long record_number = table_content->opt->prior_records + 1;
    unsigned short format_state = 0;  // 0=none, 1=header, 2=body

    // Limit border level to maximum of 2
    if (border_level > 2) border_level = 2;

    if (table_content->opt->start_table) {
        // Print table title if present
        if (!tuples_only && table_content->title) {
            fputs(".LP\\n.DS C\\n", output_file);
            troff_ms_escaped_print(table_content->title, output_file);
            fputs("\\n.DE\\n", output_file);
        }

        // Start table with appropriate borders
        fputs(".LP\\n.TS\\n", output_file);
        if (border_level == 2) {
            fputs("center box;\\n", output_file);  // full borders
        } else {
            fputs("center;\\n", output_file);      // no borders
        }

        // Set basic format for tuples-only mode
        if (tuples_only) {
            fputs("c l;\\n", output_file);  // center, left alignment
        }
    } else {
        format_state = 2;  // assume already in body format
    }

    // Process each data cell, grouping by records
    for (unsigned int i = 0; table_content->cells[i]; i++) {

        // Start of new record
        if (i % table_content->ncolumns == 0) {
            if (!tuples_only) {
                // Format record header
                if (format_state != 1) {
                    if (border_level == 2 && record_number > 1) {
                        fputs("_\\n", output_file);  // record separator
                    }
                    if (format_state != 0) {
                        fputs(".T&\\n", output_file);  // table continuation
                    }
                    fputs("c s.\\n", output_file);  // center span format
                    format_state = 1;
                }
                fprintf(output_file, "\\\\fIRecord %lu\\\\fP\\n", record_number++);
            }
            if (border_level >= 1) {
                fputs("_\\n", output_file);  // field separator
            }
        }

        // Format field-value pairs
        if (!tuples_only && format_state != 2) {
            if (format_state != 0) {
                fputs(".T&\\n", output_file);  // table continuation
            }
            // Set column format based on border preference
            if (border_level != 1) {
                fputs("c l.\\n", output_file);  // center, left
            } else {
                fputs("c | l.\\n", output_file);  // center, separator, left
            }
            format_state = 2;
        }

        // Output field name and value
        troff_ms_escaped_print(table_content->headers[i % table_content->ncolumns], output_file);
        fputc('\\t', output_file);  // tab separator
        troff_ms_escaped_print(table_content->cells[i], output_file);
        fputc('\\n', output_file);
    }

    // Print table footers if requested
    if (table_content->opt->stop_table) {
        fputs(".TE\\n.DS L\\n", output_file);  // end table, start left display

        if (table_content->footers && !tuples_only) {
            for (printTableFooter *footer = table_content->footers; footer; footer = footer->next) {
                troff_ms_escaped_print(footer->data, output_file);
                fputc('\\n', output_file);
            }
        }

        fputs(".DE\\n", output_file);  // end display
    }
}
```

This simplified version preserves the core functionality:
- Displays records vertically as field-value pairs instead of horizontal rows
- Tracks formatting state to optimize troff table format changes
- Handles record boundaries with proper separators and numbering
- Uses appropriate troff table continuation (.T&) directives
- Maintains border handling and title/footer support
- Preserves essential vertical layout algorithm while removing cancellation checks