# _align2string

## Location
[src/bin/psql/command.c:4447-4489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L4447-L4489)

## Overview
A utility function in psql that converts enumerated print format values to their corresponding string representations for display and configuration purposes.

## Definition
```c
static const char *_align2string(enum printFormat in)
```

## Detailed Description
The `_align2string` function serves as a conversion utility that maps enum values from the `printFormat` enumeration to human-readable string names. This function is essential for psql's output formatting system, allowing the conversion of internal format representations to user-friendly names that can be displayed in help text, configuration output, or error messages. The function covers all supported output formats in psql including traditional text formats, markup formats like HTML and LaTeX, and structured formats like CSV.

## Parameters / Member Variables
- `in`: An enum value of type `printFormat` representing the output format to be converted to a string

## Dependencies
- Functions called/Symbols referenced:
  - printFormat (enum type for output formats)
  - PRINT_NOTHING, PRINT_ALIGNED, PRINT_ASCIIDOC, PRINT_CSV, PRINT_HTML, PRINT_LATEX, PRINT_LATEX_LONGTABLE, PRINT_TROFF_MS, PRINT_UNALIGNED, PRINT_WRAPPED (enum values)
- Called from (representative examples):
  - [printPsetInfo](../p/printPsetInfo.md) (for displaying current format settings)
  - [pset_value_string](../p/pset_value_string.md) (for getting format setting values as strings)

## Notes and Other Information
- The function is declared as static, limiting its scope to the command.c compilation unit
- Returns a default value of "unknown" for any enum values not explicitly handled in the switch statement
- All supported psql output formats are covered: aligned tables, CSV, HTML, LaTeX variants, troff, unaligned, wrapped, and asciidoc
- The returned strings are const and should not be modified by the caller
- Used primarily for user interface purposes where format names need to be displayed

## Simplified Source

```c
static const char *_align2string(enum printFormat in) {
    switch (in) {
        case PRINT_NOTHING:         return "nothing";
        case PRINT_ALIGNED:         return "aligned";
        case PRINT_ASCIIDOC:        return "asciidoc";
        case PRINT_CSV:             return "csv";
        case PRINT_HTML:            return "html";
        case PRINT_LATEX:           return "latex";
        case PRINT_LATEX_LONGTABLE: return "latex-longtable";
        case PRINT_TROFF_MS:        return "troff-ms";
        case PRINT_UNALIGNED:       return "unaligned";
        case PRINT_WRAPPED:         return "wrapped";
    }
    return "unknown";
}
```