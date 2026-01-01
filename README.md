# AI_engineer_Screening_project

# MongoDB Retriever - Shipment Natural Language Query API

This project provides a solution for ingesting shipment data from an Excel file into MongoDB and querying it using a Natural Language Query API built with FastAPI.

## Project Structure

- `Import_data.py`: A script to read shipment data from `shipment_data.xlsx` and ingest it into a MongoDB collection.
- `app.py`: A FastAPI application that exposes a `/query` endpoint to interpret natural language queries and retrieve data from MongoDB.
- `requirements.txt`: List of Python dependencies.
- `shipment_data.xlsx`: Source data file (Excel format).

## Prerequisites

- Python 3.8+
- MongoDB Atlas cluster (Connection URI is currently configured in the scripts)

## Installation

1. Navigate to the project directory.

2. Create a virtual environment (recommended):
   ```
   python -m venv venv
   venv\Scripts\activate
   ```

3. Install the dependencies:
   ```
   pip install -r requirements.txt
   pip install fastapi uvicorn  # Required for the API (if not in requirements.txt)
   ```

## Usage

### 1. Import Data

Before running the API, you need to populate the database with shipment data.

Run the ingestion script:
```
python Import_Data.py
```
This will:
- Read `shipment_data.xlsx`.
- Normalize column names.
- Clean data (handle missing values/dates).
- Insert records into the MongoDB `shipments` collection in `shipments_db`.

### 2. Run the API

Start the FastAPI server:
```
uvicorn app:app --reload
```
The API will be available at `http://127.0.0.1:8000`.

## API Endpoints

### Health Check
- **URL**: `/`
- **Method**: `GET`
- **Response**: `{"status": "ok", "message": "Shipment API running"}`

### Query Shipments
- **URL**: `/query`
- **Method**: `POST`
- **Body**:
  ```json
  {
    "query": "how many shipments this month"
  }
  ```
- **Supported Query Patterns**:
  - "how many ... month" (Count shipments in current month)
  - "total ... cost ... month" (Sum of discounted cost in current month)
  - "group ... status" (Group by shipment status)
  - "top ... expensive" (Top 5 most expensive shipments)
  - "last ... 7 ... day" (Shipments from the last 7 days)

## Notes

- The MongoDB connection string is currently embedded in the source code. For production use, it is recommended to move this to environment variables.
- The "Natural Language" processing is currently rule-based (regex/keyword matching).
****
