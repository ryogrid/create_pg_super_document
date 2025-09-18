# get_rte_attribute_name

## Location
src/backend/parser/parse_relation.c: 3253 - 3290

## Overview
This function retrieves an attribute name from a RangeTblEntry, using aliases when available and handling various relation types including subselects and joins.

## Definition
```c
char *get_rte_attribute_name(RangeTblEntry *rte, AttrNumber attnum)
```

## Detailed Description
The `get_rte_attribute_name` function provides a flexible way to obtain column names from range table entries. Unlike `get_attname()` which only works on real relations, this function can handle subselects, joins, and other virtual relations by utilizing alias information when available. The function prioritizes user-defined column aliases over system catalog names, making it suitable for query processing where user-specified names should be preserved.

The function follows a hierarchical approach: first checking for user-written column aliases, then consulting system catalogs for real relations, and finally falling back to the eref (external reference) column names. Special handling is provided for the case where attnum is InvalidAttrNumber, which represents a whole tuple reference.

## Parameters / Member Variables
- `rte`: RangeTblEntry containing the relation information and column metadata
- `attnum`: AttrNumber specifying which attribute to retrieve (InvalidAttrNumber for "*")

## Dependencies
- Functions called/Symbols referenced:
  - [list_nth](../l/list_nth.md)
  - [get_attname](get_attname.md)
  - strVal (macro)
  - list_length
  - elog
  - InvalidAttrNumber
  - RTE_RELATION
- Called from (representative examples):
  - print_expr
  - [check_ungrouped_columns_walker](../c/check_ungrouped_columns_walker.md)
  - [get_variable](get_variable.md)
  - [get_name_for_var_field](get_name_for_var_field.md)

## Notes and Other Information
- Returns "*" when attnum is InvalidAttrNumber, representing a whole tuple reference
- Prioritizes user-defined aliases over system catalog names for better user experience
- For real relations (RTE_RELATION), consults system catalogs to get current column names, handling cases where columns may have been renamed
- Caller is responsible for ensuring the attribute is not dropped, as the function may return unexpected results for dropped columns
- Will throw an ERROR if given an invalid attribute number that exceeds the available columns
- Essential for query deparsing and error message generation where user-friendly column names are important