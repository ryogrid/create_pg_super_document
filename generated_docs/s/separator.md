# separator

## Location
src/include/fe_utils/print.h: 105 - 110

## Overview
A simple structure that holds separator string information with an optional zero-termination flag, used for various string parsing and formatting operations throughout PostgreSQL.

## Definition


## Detailed Description
The separator structure is a utility data type used across PostgreSQL's codebase for string manipulation and formatting operations. It encapsulates a separator string along with a boolean flag indicating whether the separator should be treated as zero-terminated. This structure is commonly used in parsing operations, output formatting, and string splitting functions where consistent separator handling is required.

## Parameters / Member Variables
- : Pointer to the separator string/character sequence
- : Boolean flag indicating whether the separator should be treated as zero-terminated

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple data structure)
- Called from (representative examples):
  - [dataSplitPageInternal](../d/dataSplitPageInternal.md) (src/backend/access/gin/gindatapage.c:1267)
  - [entrySplitPage](../e/entrySplitPage.md) (src/backend/access/gin/ginentrypage.c:611)
  - [show_memoize_info](show_memoize_info.md) (src/backend/commands/explain.c:3333)
  - get_reloptions (src/backend/utils/adt/ruleutils.c:13271)
  - SplitIdentifierString (src/backend/utils/adt/varlena.c:3457)
  - SplitDirectoriesString (src/backend/utils/adt/varlena.c:3584)
  - [SplitGUCList](../S/SplitGUCList.md) (src/backend/utils/adt/varlena.c:3705)
  - config_enum_get_options (src/backend/utils/misc/guc.c:3075)
  - fmt (src/bin/psql/command.c:4765)
  - [printPsetInfo](../p/printPsetInfo.md) (src/bin/psql/command.c:4941)
  - [print_separator](../p/print_separator.md) (src/fe_utils/print.c:379)
  - [printTableOpt](../p/printTableOpt.md) (src/include/fe_utils/print.h:132)

## Notes and Other Information
This structure is widely used throughout PostgreSQL for various string manipulation tasks including GIN index operations, explain output formatting, configuration option parsing, psql command processing, and table printing utilities. The separator_zero flag provides flexibility in handling different types of separator scenarios, particularly useful when dealing with multi-character separators or special termination requirements.