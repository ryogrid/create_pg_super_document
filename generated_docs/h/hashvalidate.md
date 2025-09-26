# hashvalidate

## Location
[src/backend/access/hash/hashvalidate.c:47-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashvalidate.c#L47-L274)

## Overview
The hashvalidate function is a validator for hash operator classes that checks the consistency and completeness of hash access method operator families, ensuring all required operators and support functions are properly defined and have correct signatures.

## Definition
```c
bool hashvalidate(Oid opclassoid)
```

## Detailed Description
The hashvalidate function performs comprehensive validation of a hash operator class by examining its associated operator family. It validates that:

1. **Support Functions**: All hash functions (HASHSTANDARD_PROC, HASHEXTENDED_PROC) have matching left/right types and correct signatures, and options functions (HASHOPTIONS_PROC) have proper signatures.

2. **Operators**: All operators have valid strategy numbers (1 to HTMaxStrategyNumber), proper boolean return signatures, and no unsupported ORDER BY specifications.

3. **Completeness**: The operator family contains hash functions for all data types that have operators, and all possible combinations of supported data types have corresponding operators.

4. **Cross-type Support**: Built-in hash operator families should have complete cross-type operator coverage.

The function reports validation errors as INFO messages and returns false if any issues are found, making it useful for system integrity checks and debugging operator family definitions.

## Parameters / Member Variables
- `opclassoid`: The OID of the hash operator class to validate

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - SearchSysCacheList1
  - [check_hash_func_signature](../c/check_hash_func_signature.md)
  - [check_amoptsproc_signature](../c/check_amoptsproc_signature.md)
  - [check_amop_signature](../c/check_amop_signature.md)
  - [identify_opfamily_groups](../i/identify_opfamily_groups.md)
  - [list_append_unique_oid](../l/list_append_unique_oid.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [format_procedure](../f/format_procedure.md)
  - [format_operator](../f/format_operator.md)
  - [format_type_be](../f/format_type_be.md)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [hashhandler](hashhandler.md) (in hash access method interface)

## Notes and Other Information
- The validation covers the entire operator family, so some checks are redundant when validating multiple operator classes in the same family, but this duplication is accepted to keep the amvalidate API simple.
- The function expects hash operator families to be complete with all cross-type operators for built-in types.
- [Hash](../H/Hash.md) access method only supports equality operators (strategy number 1) and does not support ORDER BY operations.
- Located in src/backend/access/hash/hashvalidate.c:47-274.