Here’s a **full, detailed requirement document for the second automation (Stage Updater)**:

---

# **Requirement Document – Automation B: Stage Updater**

## **1. Overview**

The purpose of this automation is to **update the pipeline stage of opportunities in GoHighLevel (via LeadConnector API)** using data from a CSV file. The CSV will be generated from **Automation A (Exporter)** and will have a defined schema. The process involves:

1. Uploading the CSV.
2. For each row:

   * Identify the correct sub-account (via `Account Id`).
   * Fetch all pipelines and their stages for that sub-account.
   * Match the pipeline and stage names from the CSV to actual IDs from the API.
   * Update the opportunity’s pipeline and stage using LeadConnector's **Update Opportunity API**.
3. Provide a summary report of updates and skipped rows.

---

## **2. Objectives**

* Read a CSV file with the following schema:

  ```
  Opportunity Name, Contact Name, phone, email, pipeline, stage, Created on, Updated on,
  Opportunity ID, Contact ID, Pipeline Stage ID, Pipeline ID, Account Id,
  note1, note2, note3, note4
  ```
* Use:

  * `Opportunity ID` → to update the opportunity.
  * `pipeline` (name) and `stage` (name) → to find correct pipeline and stage IDs dynamically.
  * `Account Id` → to get location-specific pipelines and authenticate with the correct sub-account token.
* Perform **stage update** without modifying other opportunity attributes (unless required for pipeline-stage alignment).
* Support **dry run** mode (default true in development).
* Output:

  * Total rows processed
  * Updated rows
  * Skipped rows with reasons

---

## **3. Data Flow**

### **Step 1: CSV Upload**

* User uploads a CSV through the FastAPI portal.
* Validate:

  * Required columns: `Opportunity ID`, `pipeline`, `stage`, `Account Id`.
  * File encoding: UTF-8 (with or without BOM).
  * Header match (case-insensitive).

### **Step 2: Pipeline Data Fetch**

* For each unique `Account Id`:

  * Fetch all pipelines using **LeadConnector API**:

    ```
    GET https://services.leadconnectorhq.com/opportunities/pipelines?locationId=<AccountId>
    ```
  * Headers:

    ```
    Authorization: Bearer <token>
    Version: 2021-07-28
    ```
  * Cache this result for the duration of the job.

### **Step 3: Matching Logic**

* Normalize names:

  * Convert to lowercase.
  * Trim spaces.
  * Collapse multiple spaces into one.
* Map:

  * `pipelineNameToId`: normalized pipeline name → pipelineId.
  * For each pipeline: `stageNameToId`: normalized stage name → stageId.
* Resolve:

  * If direct match fails, try fuzzy match (Levenshtein distance ≤ 2 or startswith).
  * If multiple matches, mark row as ambiguous → skip with reason.

### **Step 4: Update Opportunity**

* API:

  ```
  PUT https://services.leadconnectorhq.com/opportunities/:id
  ```
* Headers:

  ```
  Authorization: Bearer <token>
  Version: 2021-07-28
  Content-Type: application/json
  ```
* Payload (only required fields):

  ```json
  {
    "pipelineId": "<resolvedPipelineId>",
    "pipelineStageId": "<resolvedStageId>"
  }
  ```
* Do not modify:

  * Name
  * Status
  * Monetary value
  * Assigned user
  * Custom fields

### **Step 5: Summary**

* Return a JSON response:

  ```json
  {
    "totalRows": 100,
    "updated": 90,
    "skipped": 10,
    "skippedDetails": [
      {"row": 12, "reason": "Pipeline not found"},
      {"row": 25, "reason": "Stage ambiguous"}
    ]
  }
  ```
* Optionally provide downloadable log file (CSV or JSON).

---

## **4. API Details (External)**

### **Get Pipelines**

* **Endpoint:**
  `GET https://services.leadconnectorhq.com/opportunities/pipelines`
* **Params:**
  `locationId=<AccountId>`
* **Headers:**
  `Authorization: Bearer <token>`
  `Version: 2021-07-28`

**Response:**

```json
{
  "pipelines": [
    {
      "id": "bCkKGpDsyPP4peuKowkG",
      "name": "Transfer Portal",
      "stages": [
        {"id": "7915dedc-8f18-44d5-8bc3-77c04e994a10", "name": "Needs to be Fixed"},
        {"id": "abc-123", "name": "Closed Won"}
      ]
    }
  ]
}
```

### **Update Opportunity**

* **Endpoint:**
  `PUT https://services.leadconnectorhq.com/opportunities/:id`
* **Headers:**
  `Authorization: Bearer <token>`
  `Version: 2021-07-28`
* **Body:**

  ```json
  {
    "pipelineId": "bCkKGpDsyPP4peuKowkG",
    "pipelineStageId": "7915dedc-8f18-44d5-8bc3-77c04e994a10"
  }
  ```

---

## **5. Technical Requirements**

### **Backend**

* Framework: FastAPI
* Key components:

  * CSV parser (`pandas` or `csv` module).
  * Async HTTP client: `httpx`.
  * Job queue for large uploads (optional: `RQ` or `Celery`).

### **Frontend**

* File upload page with:

  * File input
  * Dry-run checkbox
  * Submit button
  * Progress UI + summary

### **Environment Variables**

```
CENTERS_JSON={"Center1":{"token":"TOKEN","locationId":"LOC_ID"}}
LC_API_BASE=https://services.leadconnectorhq.com
LC_API_VERSION=2021-07-28
```

---

## **6. Matching Rules**

* Normalize all names before comparison.
* Handle common mismatches:

  * Case-insensitivity
  * Extra spaces
  * Hyphen/underscore differences
* Use fallback fuzzy match if exact fails:

  * Levenshtein distance ≤ 2
* If still unresolved → skip row with reason.

---

## **7. Error Handling**

* Missing pipeline or stage → skip row.
* API errors:

  * 401/403 → mark all rows for that account as failed.
  * 429 → retry with exponential backoff (1, 2, 4, 8 sec).
  * 500 → retry up to 3 times.
* Log all failures for download.

---

## **8. Output**

* JSON summary via API.
* Optional CSV log for failures:

  ```
  row_number, reason
  12, Pipeline not found
  25, Stage ambiguous
  ```

---

## **9. Security**

* Tokens stored in environment, never exposed in UI or logs.
* Uploaded CSV deleted after processing.
* Temporary files expire after 1 hour.

---

## **10. Acceptance Criteria**

✅ Upload valid CSV → all valid rows processed, summary generated.
✅ Dry run mode → no API updates, only validation and summary.
✅ Pipeline or stage name mismatch → row skipped with correct reason.
✅ Error handling → retries and logs for 429 and 500 responses.

