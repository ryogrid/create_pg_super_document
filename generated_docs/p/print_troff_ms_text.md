# print_troff_ms_text

## Location
[src/fe_utils/print.c:2827-2918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2827-L2918)

## Overview
A function responsible for rendering tabular data in troff -ms format, handling table structure, headers, data cells, borders, and footers with proper troff markup.

## Definition
```c
static void print_troff_ms_text(const printTableContent *cont, FILE *fout)
```

## Detailed Description
This function formats and outputs tabular data using troff -ms macro package syntax. It handles the complete table lifecycle including:

1. **Table setup**: Creates troff table structure (.TS directive) with proper alignment and border specifications
2. **Title formatting**: Displays table titles using .DS C (display centered) directives
3. **Header processing**: Formats column headers with italic formatting (\\fI...\\fP)
4. **Data cell rendering**: Outputs table data with proper escaping and tab separation
5. **Footer handling**: Displays table footers using .DS L (display left-aligned) directives

The function respects various formatting options including borders (0-2 levels), tuple-only output, and handles user cancellation gracefully. All text content is properly escaped through troff_ms_escaped_print to prevent troff interpretation issues.

## Parameters / Member Variables
- `cont`: Pointer to printTableContent structure containing table data, formatting options, headers, cells, and metadata
- `fout`: Output file stream where the troff-formatted table will be written

## Dependencies
- Functions called/Symbols referenced:
  - [troff_ms_escaped_print](../t/troff_ms_escaped_print.md) (for escaping text content)
  - [footers_with_default](../f/footers_with_default.md) (for retrieving table footers)
  - fputs, fputc (standard C library functions)
- Called from (representative examples):
  - [printTable](printTable.md) (at src/fe_utils/print.c:3527)

## Notes and Other Information
- This is a static function, accessible only within src/fe_utils/print.c
- Handles border levels 0 (no borders), 1 (column separators), and 2 (full box borders)
- Uses troff -ms macros: .LP (left paragraph), .TS/.TE (table start/end), .DS/.DE (display start/end)
- Column alignment is determined by the `aligns` field in the content structure
- Supports cancellation via global `cancel_pressed` variable
- Headers are formatted in italics using troff font commands (\\fI for italic, \\fP for previous font)
- Tab characters are used as column separators in troff table format
- Part of PostgreSQL's frontend printing subsystem for generating formatted output

## Simplified Source

```c
static void print_troff_ms_text(const printTableContent *table_content, FILE *output_file) {
    bool tuples_only = table_content->opt->tuples_only;
    unsigned short border_level = table_content->opt->border;

    // Limit border level to maximum of 2
    if (border_level > 2) border_level = 2;

    if (table_content->opt->start_table) {
        // Print table title if present
        if (!tuples_only && table_content->title) {
            fputs(".LP\\n.DS C\\n", output_file);
            troff_ms_escaped_print(table_content->title, output_file);
            fputs("\\n.DE\\n", output_file);
        }

        // Start table with borders based on border level
        fputs(".LP\\n.TS\\n", output_file);
        if (border_level == 2) {
            fputs("center box;\\n", output_file);  // full borders
        } else {
            fputs("center;\\n", output_file);      // no borders
        }

        // Set column alignments and separators
        for (unsigned int i = 0; i < table_content->ncolumns; i++) {
            fputc(*(table_content->aligns + i), output_file);
            if (border_level > 0 && i < table_content->ncolumns - 1) {
                fputs(" | ", output_file);  // column separators
            }
        }
        fputs(".\\n", output_file);

        // Print column headers in italics
        if (!tuples_only) {
            for (unsigned int i = 0; i < table_content->ncolumns; i++) {
                if (i != 0) fputc('\\t', output_file);
                fputs("\\\\fI", output_file);  // italic formatting
                troff_ms_escaped_print(table_content->headers[i], output_file);
                fputs("\\\\fP", output_file);  // end italic
            }
            fputs("\\n_\\n", output_file);  // header separator line
        }
    }

    // Print table data cells
    for (unsigned int i = 0; table_content->cells[i]; i++) {
        troff_ms_escaped_print(table_content->cells[i], output_file);

        // End of row: newline, otherwise tab separator
        if ((i + 1) % table_content->ncolumns == 0) {
            fputc('\\n', output_file);
        } else {
            fputc('\\t', output_file);
        }
    }

    // Print table footers if requested
    if (table_content->opt->stop_table) {
        fputs(".TE\\n.DS L\\n", output_file);  // end table, start left-aligned display

        if (!tuples_only) {
            printTableFooter *footers = footers_with_default(table_content);
            for (printTableFooter *footer = footers; footer; footer = footer->next) {
                troff_ms_escaped_print(footer->data, output_file);
                fputc('\\n', output_file);
            }
        }

        fputs(".DE\\n", output_file);  // end display
    }
}
```

This simplified version preserves the core functionality:
- Sets up troff table structure with proper borders and alignment
- Handles table title, headers (with italic formatting), and footers
- Processes data cells with proper tab/newline separation
- Uses troff_ms_escaped_print for proper text escaping
- Maintains essential table formatting logic while removing cancellation checks for clarity