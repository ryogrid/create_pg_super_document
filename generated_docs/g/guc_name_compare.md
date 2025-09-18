guc_name_compare

Overview
A case-insensitive string comparison function specifically designed for comparing GUC parameter names with stable hash mapping across locale changes.

Definition
int guc_name_compare(const char *namea, const char *nameb)

Detailed Description
This function performs case-insensitive comparison of GUC parameter names using a custom implementation that avoids strcasecmp() to maintain hash stability across setlocale() calls. It implements ASCII-only case conversion by manually converting uppercase letters A-Z to lowercase a-z during the comparison process. The function returns standard comparison semantics: negative if namea < nameb, zero if equal, positive if namea > nameb.

The function is critical for maintaining consistent behavior in GUC hash tables and sorting operations regardless of system locale settings. It processes characters one by one, converting uppercase ASCII characters to lowercase before comparison, ensuring that parameter names like "shared_buffers" and "SHARED_BUFFERS" are treated as equivalent.

Parameters / Member Variables
- namea: First GUC parameter name to compare
- nameb: Second GUC parameter name to compare

Dependencies
- Functions called/Symbols referenced:
  - (No external dependencies - uses only basic character operations)
- Called from (representative examples):
  - find_option
  - guc_var_compare
  - guc_name_match
  - convert_GUC_name_for_parameter_acl
  - replace_auto_config_value
  - GetPGVariable
  - GetPGVariableResultDesc

Notes and Other Information
- Implements custom ASCII-only case conversion to avoid locale dependencies
- Critical for maintaining hash table stability across locale changes
- Returns standard strcmp-style comparison results (-1, 0, 1)
- Used throughout the GUC system for parameter name lookups and comparisons
- Location: src/backend/utils/misc/guc.c:1302-1331