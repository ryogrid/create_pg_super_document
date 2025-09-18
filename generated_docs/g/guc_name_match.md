guc_name_match

Overview
A dynahash match function adapter that enables guc_name_compare to be used with PostgreSQL dynamic hash tables for GUC parameter lookups.

Definition
static int guc_name_match(const void *key1, const void *key2, Size keysize)

Detailed Description
This function serves as an adapter between the dynahash interface and the guc_name_compare function. It extracts GUC parameter names from the hash table key pointers and delegates the actual comparison to guc_name_compare. The function follows the dynahash match function interface, which requires specific parameter types and calling conventions for use with PostgreSQL dynamic hash table implementation.

The function performs the necessary pointer dereferencing to extract the actual parameter name strings from the hash table keys, then uses the standard GUC name comparison logic. This ensures consistent case-insensitive matching behavior throughout the GUC hash table operations.

Parameters / Member Variables
- key1: First hash table key containing a GUC parameter name pointer
- key2: Second hash table key containing a GUC parameter name pointer  
- keysize: Size parameter required by dynahash interface (not used)

Dependencies
- Functions called/Symbols referenced:
  - guc_name_compare
- Called from (representative examples):
  - build_guc_variables (used in hash table configuration)

Notes and Other Information
- This is a static function internal to the GUC system
- Provides the dynahash interface wrapper around guc_name_compare
- Essential for proper functioning of GUC parameter hash tables
- Must return zero for matching keys, non-zero for non-matching keys
- Part of the hash table infrastructure used for fast GUC parameter lookups
- Location: src/backend/utils/misc/guc.c:1356-1375