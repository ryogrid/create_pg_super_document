#!/usr/bin/env python3
"""
Validator for PostgreSQL symbol documentation markdown files.

Usage:
    python validate_docs.py <path_to_generated_docs>

Output:
    validation_failures.txt  -- written to CWD, one failed file path per line
"""

import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUIRED_SECTIONS = [
    "Location",
    "Overview",
    "Definition",
    "Detailed Description",
    "Parameters / Member Variables",
    "Dependencies",
    "Notes and Other Information",
]

# Regex: start of a valid C declaration
# Matches known keywords, or any line that contains '(' (function signature)
C_KEYWORD_RE = re.compile(
    r"^\s*("
    r"typedef\b|"
    r"struct\s+|"
    r"enum\s+|"
    r"union\s+|"
    r"#\s*define\s+|"
    r"extern\s+|"
    r"static\s+|"
    r"inline\s+|"
    r"const\s+|"
    r"volatile\s+"
    r")"
)

# Regex: bullet item missing symbol name before colon  e.g. "  - : description"
MISSING_KEY_RE = re.compile(r"^\s+- :\s")

# Regex: fenced code block content  (```...``` or ```c...```)
CODE_BLOCK_RE = re.compile(r"```(?:\w+)?\n(.*?)```", re.DOTALL)

# Regex: H2 section heading
H2_RE = re.compile(r"^## (.+)$")

# Regex: H1 title
H1_RE = re.compile(r"^# (.+)$", re.MULTILINE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_sections(content: str) -> dict[str, str]:
    """Return {section_name: section_body} for all H2 sections."""
    sections: dict[str, str] = {}
    current_name: str | None = None
    current_lines: list[str] = []

    for line in content.splitlines():
        m = H2_RE.match(line)
        if m:
            if current_name is not None:
                sections[current_name] = "\n".join(current_lines)
            current_name = m.group(1).strip()
            current_lines = []
        elif current_name is not None:
            current_lines.append(line)

    if current_name is not None:
        sections[current_name] = "\n".join(current_lines)

    return sections


def extract_h1_title(content: str) -> str | None:
    m = H1_RE.search(content)
    return m.group(1).strip() if m else None


def extract_code_blocks(text: str) -> list[str]:
    return CODE_BLOCK_RE.findall(text)


def first_meaningful_code_lines(block: str, n: int = 3) -> list[str]:
    """Return up to n non-empty, non-comment lines from the start of a code block."""
    result = []
    for line in block.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("/*") or stripped.startswith("*") or stripped.startswith("//"):
            continue
        result.append(line)
        if len(result) >= n:
            break
    return result


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

def validate_file(content: str) -> list[str]:
    """Return a list of error descriptions (empty list = valid)."""
    errors: list[str] = []

    sections = parse_sections(content)
    symbol_name = extract_h1_title(content)

    # Rule 1: Required sections present
    for sec in REQUIRED_SECTIONS:
        if sec not in sections:
            errors.append(f"Missing required section: ## {sec}")

    # Rule 2: Location has a GitHub link
    if "Location" in sections:
        if "github.com" not in sections["Location"]:
            errors.append("Location section has no GitHub link")

    # Rules 3, 4a, 4b: Definition code block checks
    if "Definition" in sections:
        def_content = sections["Definition"]
        code_blocks = extract_code_blocks(def_content)

        # Rule 3: Code block must exist
        if not code_blocks:
            errors.append("Definition section has no fenced code block")
        else:
            first_block = code_blocks[0]

            # Rule 4a: Symbol name must appear in the code block
            if symbol_name and symbol_name not in first_block:
                errors.append(
                    f"Definition code block does not contain the symbol name '{symbol_name}'"
                )

            # Rule 4b: First meaningful lines must look like a C declaration.
            # Check up to 3 non-comment lines to handle multi-line signatures
            # where the return type is on one line and '(' is on the next.
            head_lines = first_meaningful_code_lines(first_block, n=3)
            if head_lines:
                is_keyword = bool(C_KEYWORD_RE.match(head_lines[0]))
                has_paren = any("(" in l for l in head_lines)
                if not (is_keyword or has_paren):
                    errors.append(
                        f"Definition code block first line does not look like a C declaration: "
                        f"{head_lines[0].strip()[:80]!r}"
                    )

    # Rule 5: Dependencies — no items missing symbol name before colon
    if "Dependencies" in sections:
        for line in sections["Dependencies"].splitlines():
            if MISSING_KEY_RE.match(line):
                errors.append(
                    f"Dependencies section has item with missing symbol name: {line.strip()!r}"
                )
                break  # report once per file

    # Rule 6: Parameters / Member Variables — no items missing name before colon
    if "Parameters / Member Variables" in sections:
        for line in sections["Parameters / Member Variables"].splitlines():
            if MISSING_KEY_RE.match(line):
                errors.append(
                    f"Parameters section has item with missing name: {line.strip()!r}"
                )
                break  # report once per file

    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <generated_docs_dir>", file=sys.stderr)
        sys.exit(1)

    docs_dir = Path(sys.argv[1])
    if not docs_dir.is_dir():
        print(f"Error: '{docs_dir}' is not a directory", file=sys.stderr)
        sys.exit(1)

    # Collect only symbol files: .md files inside single-letter subdirectories
    md_files = sorted(
        f
        for f in docs_dir.rglob("*.md")
        if len(f.parent.name) == 1  # single letter: a-z or A-Z
    )

    failed_files: list[str] = []
    for filepath in md_files:
        try:
            content = filepath.read_text(encoding="utf-8")
        except Exception as e:
            print(f"Warning: could not read {filepath}: {e}", file=sys.stderr)
            continue

        errors = validate_file(content)
        if errors:
            failed_files.append(str(filepath))

    output_file = Path("validation_failures.txt")
    output_file.write_text(
        "\n".join(failed_files) + ("\n" if failed_files else ""),
        encoding="utf-8",
    )

    print(f"Checked  : {len(md_files):,} files")
    print(f"Failures : {len(failed_files):,}")
    print(f"Output   : {output_file.resolve()}")


if __name__ == "__main__":
    main()
