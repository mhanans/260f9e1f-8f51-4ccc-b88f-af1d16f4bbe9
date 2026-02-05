# Data Discovery & Classification Enhancement Plan

## Objective
Enhance the existing `engine` and `connectors` to support detailed personal data identification from structured and unstructured sources, with context-aware scanning (filenames, column names) and granular object-level reporting.

## Key Requirements
1.  **Detailed Identification**: Identify PII from structured/unstructured data.
2.  **Context Awareness**: Use file contents, filenames, and column names to improve identification info.
3.  **Detailed Reporting**:
    *   Unique Identifiers (CIF, Phone, etc).
    *   Total Count.
    *   Precise Location (DB/Table/Field or File/Sheet/Row).
4.  **Object-Level Capture**: Ability to pinpoint specific rows or object entities.

## Proposed Changes

### 1. Update `CustomPIIScanner` in `engine/scanner.py`
-   Add specific recognizers for **CIF** (Customer Information File).
-   Improve **Indonesian Phone Number** detection.
-   Refactor `analyze_text` and `analyze_dataframe` to accept and utilize `context` (filenames, column headers) to boost scores of weak matches.

### 2. Create `ScanResultAggregator` (New Module or Class)
-   Responsible for taking raw scan stream and aggregating it into the requested format.
-   Key Data Structure:
    ```python
    @dataclass
    class PIIFinding:
        type: str           # e.g., PHONE_NUMBER, CIF
        value: str          # The actual text found (masked optionally)
        score: float
        location: DataLocation # Polymorphic (FileLocation | DBLocation)

    @dataclass
    class DataLocation:
        source_type: str    # 'file' | 'database'
        path: str           # File path or DB Connection String/Schema
        container: str      # Sheet Name or Table Name
        field: str          # Column Name (if structured)
        position: str       # Row index, Line number, or Page number
    ```

### 3. Enhance `FileScanner` in `connectors/file_scanner.py`
-   Ensure it passes `filename` down to the scanner.
-   When extracting content (e.g., from Excel), keeping track of Row/Column is crucial. The current implementation does this (`chunks` have metadata), but we need to ensure this metadata flows all the way to the final report.

### 4. Implementation Steps
1.  **Refactor `scan_object` (new generic entry point)**: A function that takes an object (File or DB Row), extracts text/context, runs Presidio with context, and returns fully populated `PIIFinding` objects.
2.  **Add `CIF` Regex**: Define a pattern for CIF (e.g., 6-10 digits usually).
3.  **Update `verify_scan.py`**: Create a robust test case with a mock Excel/CSV file containing ambiguous data that requires context (like a header "Phone") to be correctly identified.

