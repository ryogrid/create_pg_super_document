# like_regex_support

## Location
src/backend/utils/adt/like_support.c: 156 - 240

## Overview
A common support function that provides selectivity estimation and index condition optimization for pattern matching operations including LIKE, ILIKE, regex, and text prefix operations.

## Definition


## Detailed Description
The `like_regex_support` function serves as a unified backend for PostgreSQL's pattern matching support infrastructure. It handles two primary types of support requests:

1. **Selectivity Estimation**: When called with a `SupportRequestSelectivity`, it estimates how selective a pattern matching operation will be, which helps the query planner choose optimal execution strategies.

2. **Index Condition Optimization**: When called with a `SupportRequestIndexCondition`, it attempts to convert pattern matching operations into index-scannable conditions, enabling efficient index usage for pattern queries.

The function is designed to work with different pattern types (LIKE, ILIKE, regex, etc.) through the `ptype` parameter, making it a reusable foundation for various text pattern matching operations in PostgreSQL.

## Parameters / Member Variables
- `rawreq`: A `Node*` representing either a selectivity estimation request or an index condition request
- `ptype`: A `Pattern_Type` enum value specifying the type of pattern matching operation (LIKE, ILIKE, regex, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - `[patternsel_common](../p/patternsel_common.md)`: For selectivity estimation calculations
  - `[match_pattern_prefix](../m/match_pattern_prefix.md)`: For converting patterns to index conditions
  - `[is_opclause](../i/is_opclause.md)`: To check if the node is an operator expression
  - `[is_funcclause](../i/is_funcclause.md)`: To check if the node is a function call expression
  - `lsecond`: To access the second argument in argument lists
- Called from (representative examples):
  - `[textlike_support](../t/textlike_support.md)`: LIKE operator support
  - `[texticlike_support](../t/texticlike_support.md)`: ILIKE operator support  
  - `[textregexeq_support](../t/textregexeq_support.md)`: Regex match operator support
  - `[texticregexeq_support](../t/texticregexeq_support.md)`: Case-insensitive regex support
  - `[text_starts_with_support](../t/text_starts_with_support.md)`: Text prefix support

## Notes and Other Information
- This is a static function internal to the like_support.c module
- For join selectivity estimation, it currently uses a default value (`DEFAULT_MATCH_SEL`) as a fallback
- The function only handles cases where the indexed column is the left argument (indexarg == 0)
- Returns NULL if the request cannot be handled or optimized
- Forms the core infrastructure that enables PostgreSQL to efficiently execute pattern matching queries with proper cost estimation and index usage