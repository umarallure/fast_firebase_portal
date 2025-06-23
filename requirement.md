Perfect, Umar! Based on your updated requirements, here's the revised version of your **`requirements.md`** with Firebase Auth and single Excel export logic for multi-account/pipeline selection.

---

# 📘 Project Requirements Document (Updated)

## 🚀 Project Name: GHL Automation Portal

---

## 🧩 Overview

**GHL Automation Portal** is a secure, Firebase-authenticated FastAPI web application that allows authorized users to run automation scripts across multiple GoHighLevel (GHL) subaccounts.

The key automation is to fetch and export opportunity data per selected pipelines for each subaccount and generate **one consolidated Excel file**.

---

## 🎯 Core Features

### ✅ Firebase Authentication

* User authentication via Firebase JWT
* User identity verified in FastAPI middleware
* Tokens validated using Firebase Admin SDK
* No local user DB needed (can cache user metadata optionally)

### ✅ Subaccount & Pipeline Selection

* Allow user to link/manage multiple GHL subaccounts (via stored API keys)
* User selects one or more subaccounts
* For each selected subaccount, show its pipelines
* User picks pipelines per subaccount
* Save choices temporarily in session/state (or pass via request)

### ✅ Automation Execution

* For each selected subaccount-pipeline combo:

  * Fetch all opportunities
  * Flatten/transform the data
* Combine all rows into **one Excel sheet**
* Provide download link to generated file

### ✅ Execution Logs

* Store:

  * Firebase UID
  * Timestamp
  * Subaccounts used
  * Pipelines selected
  * Row count
  * File path
  * Status (Success/Failed)
* Optional: UI endpoint for viewing past runs

---

## 🏗️ Architecture

### 📦 Backend: FastAPI + Firebase

```
app/
├── main.py
├── auth/firebase.py         # Firebase admin SDK setup + dependency
├── api/automation.py        # Routes for pipeline selection and export
├── services/ghl_export.py   # Logic to fetch and format GHL data
├── services/excel_writer.py # Generate single Excel from combined rows
├── models/schemas.py        # Pydantic request models
├── db/logs.py               # Execution logs (SQLite or Firestore)
└── config.py
```

---

## 🔐 Authentication with Firebase

### 🔑 Flow

1. Frontend authenticates user via Firebase
2. Sends Firebase JWT in `Authorization: Bearer <token>` header
3. FastAPI middleware validates and extracts user UID
4. All endpoints gated via verified Firebase identity

### 🔧 Dependencies

```bash
pip install firebase-admin python-dotenv fastapi uvicorn httpx pandas openpyxl
```

---

## 📥 API Endpoints (Updated)

| Method | Route                     | Description                              |
| ------ | ------------------------- | ---------------------------------------- |
| POST   | `/auth/verify-token`      | Verifies Firebase token (optional route) |
| GET    | `/subaccounts/`           | List user's GHL subaccounts              |
| GET    | `/pipelines/{account_id}` | Get pipelines for a subaccount           |
| POST   | `/automation/export`      | Fetch data and generate Excel            |
| GET    | `/logs`                   | List execution logs                      |

---

## 📊 Input Request Schema

```json
{
  "selections": [
    {
      "account_id": "account_1",
      "api_key": "GHL_TOKEN_1",
      "pipelines": ["pipeline_id_1", "pipeline_id_2"]
    },
    {
      "account_id": "account_2",
      "api_key": "GHL_TOKEN_2",
      "pipelines": ["pipeline_id_3"]
    }
  ]
}
```

---

## 📤 Output

* One Excel file with all opportunity rows from all selected pipelines
* Sheet name: `GHL Opportunities Export`
* File downloadable or emailed (optional)

---

## 📄 Execution Log Schema

| Field         | Type     |
| ------------- | -------- |
| id            | UUID     |
| firebase\_uid | string   |
| subaccounts   | JSON     |
| pipelines     | JSON     |
| record\_count | int      |
| created\_at   | datetime |
| file\_path    | string   |
| status        | string   |

---

## ⚡ Performance Plan

* Use `httpx.AsyncClient` to fetch each pipeline concurrently
* Respect GHL API rate limits
* Batch transformations using `pandas`
* Use in-memory buffer (`BytesIO`) or temp folder for Excel output

---

## 🧪 Sample Workflow

1. User logs in via Firebase frontend
2. Token sent with request to FastAPI
3. User selects subaccounts and pipelines
4. FastAPI fetches, transforms, and combines data
5. One Excel file is returned for download
6. Execution log is saved

---

## 🧰 Tools

| Category     | Tool/Lib           |
| ------------ | ------------------ |
| Auth         | Firebase Admin SDK |
| Backend      | FastAPI            |
| HTTP Client  | httpx              |
| Excel Export | pandas, openpyxl   |
| Logging      | SQLite / Firestore |
| Background   | asyncio            |

---

## 📄 To-Do List

* [x] Firebase Admin SDK setup
* [ ] Secure API key storage per user (optional: Firestore/Firebase DB)
* [ ] Async fetch & Excel export logic
* [ ] Excel merging & download endpoint
* [ ] Execution logging DB model
* [ ] API documentation with Swagger

---

Would you like me to now:

1. Scaffold the project with base code (`main.py`, `firebase.py`, and `/export` route)?
2. Show how to fetch pipelines and opps concurrently for multiple accounts?
3. Generate the Excel file in memory with `pandas` and return it via FastAPI?

Let me know how you'd like to proceed.
