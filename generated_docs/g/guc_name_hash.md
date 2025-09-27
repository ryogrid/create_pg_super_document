guc_name_hash

Overview
A hash function for GUC parameter names that is compatible with guc_name_compare, ensuring consistent case-insensitive hashing behavior.

Definition
static uint32 guc_name_hash(const void *key, Size keysize)

Detailed Description
This function generates hash values for GUC parameter names while maintaining compatibility with the guc_name_compare function. It performs the same ASCII-only case-folding as guc_name_compare, converting uppercase letters A-Z to lowercase a-z during hash computation. The function uses a simple but effective hash algorithm that rotates the accumulated hash value left by 5 bits and XORs it with each character.

The hash function is designed to ensure that GUC parameter names that compare as equal under guc_name_compare will produce identical hash values. This consistency is crucial for proper functioning of hash tables used in the GUC system. The implementation deliberately uses a simple hash algorithm since performance is not as critical as correctness and consistency.

Parameters / Member Variables
- key: Pointer to a GUC parameter name (as a char* pointer)
- keysize: Size parameter (not used in this implementation)

Dependencies
- Functions called/Symbols referenced:
  - pg_rotate_left32
- Called from (representative examples):
  - build_guc_variables (used in hash table operations)

Notes and Other Information
- This is a static function internal to the GUC system
- Uses the same case-folding logic as guc_name_compare for consistency
- The hash algorithm rotates left by 5 bits and XORs with each character
- Must produce identical hash values for names that guc_name_compare considers equal
- Used in GUC hash table implementations for fast parameter lookup
- Location: src/backend/utils/misc/guc.c:1332-1355

## Simplified Source

```c
// Simplified version of guc_name_hash
static uint32 guc_name_hash(const void *key, Size keysize) {
    uint32 hash_result = 0;
    const char *parameter_name = *(const char *const *) key;

    // Process each character in the parameter name
    while (*parameter_name) {
        char current_char = *parameter_name++;

        // Convert uppercase to lowercase (same as guc_name_compare)
        if (current_char >= 'A' && current_char <= 'Z') {
            current_char += 'a' - 'A';
        }

        // Update hash: rotate left 5 bits and XOR with character
        hash_result = pg_rotate_left32(hash_result, 5);
        hash_result ^= (uint32) current_char;
    }

    return hash_result;
}
```

Key simplifications made:
- Used more descriptive variable names (hash_result, parameter_name, current_char)
- Added clear comments for each logical step
- Preserved the essential hash algorithm and case-folding logic
- Maintained compatibility with guc_name_compare function