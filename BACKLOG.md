# Project Backlog

## 🔄 Current Issues

### High Priority
1. **Fix Essential Processors Classification Logic**
   - **Problem**: 294 "essential" processors after pruning suggests insufficient filtering
   - **Issues Found**:
     - Inconsistent LLM generation decisions (ListHDFS=No, FetchHDFS=Yes for similar operations)
     - Simple file operations (PutS3Object, FetchHDFS) getting LLM treatment unnecessarily
     - ExecuteStreamCommand with basic `mv` commands marked as "LLM: No" but may need HDFS→DBFS path translation
     - 195 data transformation processors seems too high after "infrastructure pruning"
   - **Root Cause**: Classification logic isn't aggressive enough in filtering infrastructure vs business logic
   - **Impact**: Over-complicated migrations, unnecessary LLM API calls, inconsistent code generation

<!-- 2. **Background Migration Progress Lost on Page Navigation**
   - **Problem**: When user navigates away during migration, background process continues but results are lost
   - **Technical Issue**: Streamlit execution model causes page context loss during navigation
   - **Current Behavior**:
     - Migration starts → Python process runs in background
     - User navigates away → Page execution stops, process continues
     - Migration completes → Results try to update non-existent UI context
     - User returns → New page execution has no knowledge of completed migration
   - **Impact**: Users lose migration results and have to re-run analysis
   - **Current Mitigation**: Navigation buttons disabled during processing (partial solution) -->

### Medium Priority
1. want to seperate the asset and migration to different app page?
2. adding a new page for displaying all the prosseors result form delta table analysis form sasha?
3. maybe still keep the classifiers from the rule and llm?
4. need to modify the page name, remove the migration keyword
5. **Test and Validate Dependencies Function**
   - **Problem**: New comprehensive dependency analysis system needs thorough testing
   - **Current State**: Dependencies functionality implemented but needs validation across different NiFi workflows
   - **Testing Needed**:
     - Variable dependency extraction accuracy
     - Connection mapping completeness
     - Property dependency detection reliability
     - Performance with large workflows (100+ processors)
     - UI responsiveness and error handling
   - **Validation Areas**:
     - Compare dependency results with manual analysis
     - Test circular dependency detection
     - Verify impact analysis accuracy
     - Validate exported reports and data formats
   - **Benefits**: Ensure reliability before production deployment
6. **Separate Classification and Pruning Operations**
   - **Problem**: Current classification page combines processor analysis and pruning in single operation
   - **Current State**: Single "Classify Processors" button runs both classification and pruning together
   - **Enhancement Needed**:
     - Split into two separate operations: "Classify Processors" and "Prune Infrastructure"
     - Allow users to review classification results before deciding to prune
     - Enable different pruning strategies (conservative vs aggressive)
     - Show before/after comparison of processor counts
     - Let users selectively exclude processors from pruning
   - **UI Design**:
     - Step 1: "Analyze & Classify" → Show all processors with classifications
     - Step 2: "Configure Pruning" → Let users choose pruning level and exclusions
     - Step 3: "Apply Pruning" → Show final essential processors list
   - **Benefits**: More control over pruning decisions, better transparency, reversible operations

### Low Priority
1. **~~Complete Processor Dependency Analysis Tool~~** ✅ **COMPLETED**
   - **Status**: Fully implemented in `tools/dependency_extraction.py` and `pages/02_Processor_Dependencies.py`
   - **Features Delivered**:
     - ✅ **Variable Dependencies**: Track all `${variable}` definitions and usages across processors
     - ✅ **Connection Dependencies**: Map processor-to-processor flow connections comprehensively
     - ✅ **Property Dependencies**: Identify processors that reference other processors' outputs
     - ✅ **Configuration Dependencies**: Find shared configuration values and references
     - ✅ **Impact Analysis**: High-impact processors, isolated processors, dependency chains
     - ✅ **Circular Dependency Detection**: Identify problematic dependency cycles
     - ✅ **Interactive UI**: 5-tab Streamlit interface with filtering and downloads
   - **Available Functions**:
     ```python
     def extract_all_processor_dependencies(xml_path: str) -> Dict
     def find_variable_dependencies(processors: List) -> Dict
     def map_processor_connections(processors: List, connections: List) -> Dict
     def generate_dependency_report(dependencies: Dict) -> str
     ```
   - **Current State**: Hidden in production (`main` branch), fully functional in development (`trace_data_dependency` branch)
   - **Next**: Move to Medium Priority for testing and validation
<!-- 5. **Improve Table Lineage Connections Display**
   - **Problem**: Table Lineage page shows "Connections: 671" metric but no detailed breakdown
   - **Current State**: Only summary metric displayed, no way to explore what the 671 connections represent
   - **Enhancement Needed**:
     - Add detailed connections table showing processor-to-processor relationships
     - Display connection source, target, and relationship types
     - Enable filtering and sorting of connection data
     - Help users understand the NiFi workflow structure better
   - **Implementation**: Add expandable "View Connections Details" section with tabular data -->

6. **Build Data Lineage Graph Visualization**
   - **Problem**: Table lineage results are only displayed in CSV/table format, hard to visualize data flow
   - **Current State**: Static table showing source→target relationships without visual context
   - **Enhancement Needed**:
     - graph visualization of table-to-table data flows
     - Node-and-edge diagram showing data lineage relationships
     - Visual representation of processor chains and hop counts
     - Ability to trace data flow paths visually
     - Filter graph by schema, table types, or hop distance
   - **Technical Options**:
     - Streamlit-compatible graph libraries (Plotly, NetworkX + Matplotlib, Graphviz)
     - Interactive features: zoom, pan, node selection, path highlighting
     - Export graph as image or interactive HTML
   - **Benefits**: Better understanding of complex data flows, easier impact analysis

## 🎯 **Potential Solutions**

### For Classification Issues
- **Better LLM vs Template Logic**: Simple file operations should use templates, not LLM generation
- **More Aggressive Pruning**: Infrastructure operations should be filtered out more thoroughly
- **Consistent Classification**: Similar processor types need consistent LLM generation decisions
- **Path Translation Awareness**: File movement operations need HDFS→DBFS conversion logic
- **Threshold Tuning**: 294 essential processors is likely too many - need stricter essential criteria

### For Background Migration Progress
- **Option 1: Global Status Tracker**: Add migration status visible across all pages when background process running
- **Option 2: Result Polling System**: Implement checking for completed results when returning to migration page
- **Option 3: Enhanced Navigation Protection**: Continue current approach with improved user messaging
- **Recommended**: Combination of Option 3 (current) with Option 1 fallback for status tracking
