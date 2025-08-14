Got it ✅ — you want a **requirement document** for your FastAPI automation project that integrates with GoHighLevel (GHL), fetches data across sub-accounts, and outputs a CSV. Here’s a structured **requirements specification draft** you can use for building this automation:

---

# **Requirement Document – GHL Pipeline & Notes Export Automation**

## **1. Overview**

We are building a FastAPI-based automation portal that connects to GoHighLevel (GHL) APIs to fetch pipeline opportunities and contact notes from multiple sub-accounts. The user will be able to select one or more centers (sub-accounts) from a dropdown UI, and for each selected sub-account, the system will extract pipeline and opportunities data along with the latest notes, then generate a downloadable CSV file.

---

## **2. Objectives**

* Provide a portal UI where users can:

  * Select sub-accounts (loaded from environment configuration).
  * Select multiple centers via multi-select dropdown.
* For each selected sub-account:

  * Retrieve all pipeline IDs.
  * Retrieve all opportunities from each pipeline.
  * For each opportunity, fetch **latest 4 notes** from the related contact.
* Generate a unified CSV report where:

  * Each row = 1 opportunity.
  * Contact’s **latest 4 notes** appear in separate columns (`Note 1`, `Note 2`, `Note 3`, `Note 4`).
* Provide a **download button** for the CSV.

---

## **3. Data Flow**

1. **Load Sub-Accounts**

   * Read sub-account tokens from `.env` file.
   * Populate dropdown with center names.

2. **Fetch Pipelines**

   * API: `GET /v1/pipelines/`
   * Headers: `Authorization: Bearer <token>`
   * Output: List of pipelines (`id`, `name`).

3. **Fetch Opportunities**

   * API: `GET /v1/pipelines/:pipelineId/opportunities?limit=100` (loop with pagination).
   * Fields extracted:

     * Opportunity ID
     * Name
     * Monetary Value
     * Status
     * Source
     * CreatedAt / UpdatedAt
     * Contact info (`id`, `name`, `companyName`, `email`, `phone`, `tags`)

4. **Fetch Notes (Latest 4)**

   * API: `GET /v1/contacts/:contactId/notes/`
   * Sort by `createdAt DESC`, pick 4 latest.
   * Map to columns: `Note 1`, `Note 2`, `Note 3`, `Note 4`.

5. **Generate CSV**

   * Columns Example:

     ```
     SubAccount, PipelineId, PipelineName, OpportunityId, OpportunityName, 
     MonetaryValue, Status, Source, ContactId, ContactName, ContactEmail, ContactPhone, 
     Note 1, Note 2, Note 3, Note 4
     ```
   * File is downloadable via frontend.

---

## **4. API Details**

### **Pipelines**

* Endpoint: `GET https://rest.gohighlevel.com/v1/pipelines/`
* Headers:

  * `Authorization: Bearer <token>`
* Response: List of pipeline IDs.

### **Opportunities**

* Endpoint: `GET https://rest.gohighlevel.com/v1/pipelines/:pipelineId/opportunities`
* Params:

  * `limit=100` (max per page).
  * `startAfterId` + `startAfter` (pagination).
* Response: Opportunities with contact info.

### **Notes**

* Endpoint: `GET https://rest.gohighlevel.com/v1/contacts/:contactId/notes/`
* Response: Array of notes.
* Requirement: Pick latest 4.

---

## **5. Technical Requirements**

### **Backend (FastAPI)**

* Routes:

  1. `/subaccounts` → Return available sub-accounts (from `.env`).
  2. `/pipelines/{subaccount}` → Fetch pipelines.
  3. `/opportunities/{subaccount}/{pipelineId}` → Fetch opportunities.
  4. `/notes/{contactId}` → Fetch last 4 notes.
  5. `/export` → Generate CSV and return file.

* Environment Variables:

  ```
  GHL_ACCOUNTS = {"Center1": "TOKEN1", "Center2": "TOKEN2", "Center3": "TOKEN3"}
  ```

### **Frontend (FastAPI + Jinja2 or React/Vue)**

* Multi-select dropdown for centers.
* Button: “Generate Report”.
* Download link for CSV.

### **CSV Generation**

* Use `pandas` or `csv` module.
* Ensure notes fill separate columns.
* Empty string if less than 4 notes available.

---

## **6. Output Example (CSV)**

| SubAccount | PipelineId | OpportunityId | OpportunityName | ContactName | ContactEmail                        | Status | Note 1         | Note 2     | Note 3             | Note 4 |
| ---------- | ---------- | ------------- | --------------- | ----------- | ----------------------------------- | ------ | -------------- | ---------- | ------------------ | ------ |
| Center1    | abc123     | opp789        | Deal A          | John Deo    | [john@deo.com](mailto:john@deo.com) | open   | Call scheduled | Sent quote | Follow-up tomorrow | —      |

---

## **7. Error Handling**

* Missing/expired token → return error response.
* No pipelines → log warning.
* No notes → leave note columns empty.
* Handle pagination until all opportunities fetched.


