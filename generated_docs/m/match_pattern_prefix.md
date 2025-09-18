# match_pattern_prefix

## Location
[src/backend/utils/adt/like_support.c:241-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L241-L485)

## Overview
Generates indexable range conditions from LIKE and regex patterns by extracting fixed prefixes and converting them into optimized equality or range constraints for efficient index scanning.

## Definition
```c
static List *match_pattern_prefix(Node *leftop, Node *rightop, Pattern_Type ptype, Oid expr_coll, Oid opfamily, Oid indexcollation)
```

## Detailed Description
The `match_pattern_prefix` function is a core optimization component that transforms pattern matching operations into index-friendly conditions. It analyzes patterns from LIKE, ILIKE, and regex operations to extract fixed prefixes that can be used with B-tree or SP-GiST indexes.

The function works by:
1. **Pattern Analysis**: Extracts fixed prefix portions from patterns using `pattern_fixed_prefix`
2. **Operator Selection**: Chooses appropriate comparison operators based on data type and index operator family
3. **Constraint Generation**: Creates equality constraints for exact matches or range constraints (>= and <) for prefix matches
4. **Collation Handling**: Ensures compatibility with index collation requirements, particularly for C-locale optimizations

This optimization enables PostgreSQL to use indexes efficiently for pattern queries like `name LIKE 'John%'` by converting them to range scans equivalent to `name >= 'John' AND name < 'JohO'`.

## Parameters / Member Variables
- `leftop`: The indexed column expression (left side of the comparison)
- `rightop`: The pattern constant (right side of the comparison)  
- `ptype`: The type of pattern matching operation (`Pattern_Type` enum)
- `expr_coll`: The collation OID for the expression
- `opfamily`: The operator family OID of the index
- `indexcollation`: The collation OID used by the index

## Dependencies
- Functions called/Symbols referenced:
  - [pattern_fixed_prefix](../p/pattern_fixed_prefix.md): Extracts fixed prefix from patterns
  - [get_collation_isdeterministic](../g/get_collation_isdeterministic.md): Checks if collation is deterministic
  - [op_in_opfamily](../o/op_in_opfamily.md): Verifies operator support in index operator family
  - `make_opclause`: Creates operator expression nodes
  - [make_greater_string](make_greater_string.md): Generates upper bound for range constraints
  - [lc_collate_is_c](../l/lc_collate_is_c.md): Checks for C locale collation
  - [fmgr_info](../f/fmgr_info.md): Gets function manager info for operators
- Called from (representative examples):
  - [like_regex_support](../l/like_regex_support.md): Main pattern support dispatcher

## Notes and Other Information
- Returns NIL if no optimization is possible (e.g., no fixed prefix, unsupported data type)
- Supports TEXT, NAME, BPCHAR (char), and BYTEA data types
- Handles both regular and pattern-specific operator families (e.g., `text_pattern_ops`)
- For exact matches, generates simple equality conditions
- For prefix matches, creates range constraints with >= and < operators
- Requires C-locale or collation-insensitive indexes for reliable range optimization
- Special handling for SP-GiST indexes that support direct prefix operators
- Automatically coerces prefix constants to match target data types when needed