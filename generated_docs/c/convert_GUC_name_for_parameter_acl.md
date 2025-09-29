convert_GUC_name_for_parameter_acl

Overview
Converts GUC parameter names to their canonical form for storage and lookup in the pg_parameter_acl system catalog, handling case normalization and obsolete name mapping.

Definition
char *convert_GUC_name_for_parameter_acl(const char *name)

Detailed Description
This function performs two key transformations on GUC parameter names to create canonical forms suitable for the parameter ACL system. First, it maps any obsolete GUC names to their modern equivalents using the map_old_guc_names array, ensuring that old parameter names are automatically converted to current names when stored in new PostgreSQL versions. Second, it applies the same ASCII-only case-folding used by guc_name_compare, converting all uppercase letters A-Z to lowercase a-z.

The function is essential for maintaining consistency in the pg_parameter_acl catalog, where parameter names must be stored in a standardized format. It ensures that equivalent parameter names (differing only in case or using obsolete names) are treated identically in the ACL system. The result is a newly allocated string that the caller must eventually free.

Parameters / Member Variables
- name: The input GUC parameter name to be converted

Dependencies
- Functions called/Symbols referenced:
  - guc_name_compare (for obsolete name mapping comparison)
  - map_old_guc_names (external array mapping obsolete to current names)
  - pstrdup (for string duplication)
- Called from (representative examples):
  - pg_parameter_aclmask
  - ParameterAclLookup  
  - ParameterAclCreate
  - EmitWarningsOnPlaceholders

Notes and Other Information
- Returns a pallocd string that must be freed by the caller

## Simplified Source

```c
char *convert_GUC_name_for_parameter_acl(const char *name)
{
    char *result;

    // Map obsolete GUC names to current names
    for (int i = 0; map_old_guc_names[i] != NULL; i += 2) {
        if (guc_name_compare(name, map_old_guc_names[i]) == 0) {
            name = map_old_guc_names[i + 1];
            break;
        }
    }

    // Create a copy and convert to lowercase for case-insensitive storage
    result = pstrdup(name);
    for (char *ptr = result; *ptr != '\0'; ptr++) {
        char ch = *ptr;

        if (ch >= 'A' && ch <= 'Z') {
            ch += 'a' - 'A';  // Convert uppercase to lowercase
            *ptr = ch;
        }
    }

    return result;
}
```