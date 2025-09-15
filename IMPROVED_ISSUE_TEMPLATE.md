**Objective**: Autonomously generate high-quality, comprehensive documentation for PostgreSQL codebase symbols using optimized context and instructions.

@github-copilot, you are tasked with generating detailed technical documentation for PostgreSQL symbols. Please follow this enhanced workflow designed specifically for optimal Copilot performance:

### Key Improvements for Quality Documentation:

1. **Context Management**: Each batch starts with a fresh context. Previous batch conversations should not influence current work.

2. **Quality Standards**: 
   - Write detailed, comprehensive explanations (not brief summaries)
   - Include specific technical implementation details
   - Provide thorough parameter explanations with types and constraints
   - Explain the architectural reasoning behind each symbol

3. **Enhanced Documentation Requirements**:
   - Overview: 2-3 detailed sentences minimum
   - Detailed Description: 4-6 comprehensive sentences
   - Parameters: Full explanation for each including purpose, valid values, and behavior impact
   - Dependencies: Include reasoning for why each dependency exists

### Your Autonomous Workflow:

1. **Initial Setup** (Fresh Working Environment):
   ```bash
   git fetch origin
   git checkout -b copilot/copilot-work
   ./scripts/setup_environment.sh  # Only run once
   ```

2. **Enhanced Processing Loop**: Execute these steps with improved context:

   a. **Get Enhanced Batch Context**:
      ```bash
      python3 scripts/improved_get_next_batch.py > current_batch.json
      ```
      
   b. **Context Reset Check**: Each batch JSON contains `context_reset_instruction`. Always start fresh - previous batch context is irrelevant.

   c. **Quality-Focused Documentation Generation**: 
      For each symbol in `current_batch.json`, generate comprehensive documentation following the enhanced format:
      - Use the provided `quality_example` as a reference for depth and detail
      - Follow the `copilot_specific_instructions` for focus areas
      - Meet all `quality_requirements` specified in the batch
      - Use provided context from `related_symbol_summaries` to understand dependencies

      Save each document using:
      ```bash
      python3 scripts/mcp_tool.py return_document [symbol_name] "[Enhanced Markdown Content]"
      ```

   d. **Batch Processing & Quality Validation**:
      ```bash
      python3 scripts/ingest_documents.py
      python3 scripts/validate_documentation_quality.py  # New validation step
      ```

   e. **Database State Persistence** (Enhanced with quality metrics):
      ```bash
      git fetch origin
      git checkout -b copilot/agent-documentation-progress || git checkout copilot/agent-documentation-progress
      git checkout copilot/copilot-work -- data/documents.duckdb
      git config --global user.name "GitHub Copilot Agent"
      git config --global user.email "copilot-agent@users.noreply.github.com"
      git add data/documents.duckdb
      BATCH_ID=$(jq -r '.batch_id' ../current_batch.json)
      QUALITY_SCORE=$(jq -r '.average_quality_score // "N/A"' quality_report.json)
      git commit -m "docs(data): Enhanced documentation batch ${BATCH_ID} (Quality: ${QUALITY_SCORE})"
      git push --set-upstream origin copilot/agent-documentation-progress
      git checkout copilot/copilot-work
      rm current_batch.json quality_report.json
      ```

3. **Quality Assurance Notes**:
   - Each documentation section should provide genuine insight into the PostgreSQL codebase
   - Avoid generic or template-like descriptions
   - Include technical details that would help a PostgreSQL developer understand the symbol's role
   - Use the provided examples as quality benchmarks
   - Context from `related_symbol_summaries` should inform your explanations of dependencies and relationships

4. **Finalization**: Create PR from `copilot/agent-documentation-progress` to `copilotver` with quality metrics summary.

**Quality Focus**: This workflow emphasizes comprehensive, detailed documentation that matches or exceeds the quality of Claude Code generated documentation through enhanced context, clear instructions, and proper context management.