# parseLocalRelOptions

## Location
[src/backend/access/common/reloptions.c:1550-1577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1550-L1577)

## Overview
Static function that parses local unregistered relation options from a local_relopts structure, creating a relopt_value array for locally-defined options.

## Definition
```c
static relopt_value *parseLocalRelOptions(local_relopts *relopts, Datum options, bool validate)
```

## Detailed Description
This function handles the parsing of locally-defined relation options that are not part of the standard PostgreSQL relation option catalog. It takes a local_relopts structure containing a list of local option definitions, allocates a relopt_value array sized to match the number of local options, and initializes each entry with the corresponding option definition. If options are provided via the Datum parameter, it delegates the actual parsing to parseRelOptionsInternal() to populate the values. This function is specifically designed for extensions or custom code that defines their own relation options.

## Parameters / Member Variables
- `relopts`: Pointer to local_relopts structure containing locally-defined option definitions
- `options`: Datum containing relation options in text-array format (can be 0/NULL if no options provided)
- `validate`: Boolean flag to enable validation and error reporting during parsing

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md)
  - [palloc](palloc.md)
  - foreach (macro)
  - lfirst (macro)
  - [parseRelOptionsInternal](parseRelOptionsInternal.md)
- Called from (representative examples):
  - [build_local_reloptions](../b/build_local_reloptions.md)

## Notes and Other Information
- Designed specifically for handling locally-registered (non-standard) relation options
- Uses PostgreSQL's List infrastructure to iterate through local option definitions
- Memory allocation is based on the count of options in the local_relopts list
- All options are initially marked as unset (isset=false) before parsing
- Can handle the case where no options are provided (options == 0)

## Simplified Source

```c
static relopt_value *parseLocalRelOptions(local_relopts *relopts, Datum options, bool validate) {
    // Allocate array for option values
    int nopts = list_length(relopts->options);
    relopt_value *values = palloc(sizeof(*values) * nopts);

    // Initialize each option entry
    ListCell *lc;
    int i = 0;
    foreach(lc, relopts->options) {
        local_relopt *opt = lfirst(lc);

        values[i].gen = opt->option;
        values[i].isset = false;  // Initially unset
        i++;
    }

    // Parse actual option values if provided
    if (options != (Datum) 0) {
        parseRelOptionsInternal(options, validate, values, nopts);
    }

    return values;
}
```