# ivolatility-equities-eod-ivs

This repository contains a **Python-based ETL** job for retrieving **iVolatility** data from the endpoint `"/equities/eod/ivs"` and loading it into an **Azure SQL** database. The code supports:

1. **Concurrent API requests** for multiple symbols (using Python `ThreadPoolExecutor`).
2. **Secure Azure SQL connections** via **Azure Managed Identity** (no passwords required).
3. **Deleting old rows** (based on date range + symbol) before inserting fresh data.
4. Automatic **Docker** packaging and sample **Kubernetes CronJob** setup.

---

## Table of Contents

1. [Overview](#overview)  
2. [Prerequisites](#prerequisites)  
3. [Project Structure](#project-structure)  
4. [Setup & Usage](#setup--usage)  
   - [Local Testing](#local-testing)  
   - [Docker Build & Run](#docker-build--run)  
   - [Kubernetes CronJob](#kubernetes-cronjob)  
5. [Environment Variables](#environment-variables)  
6. [Table Schema](#table-schema)  
7. [Contributing](#contributing)  
8. [License](#license)  

---

## Overview

This ETL job:

1. **Selects** a list of symbols from an **Azure SQL** table (using a user-provided T-SQL query).  
2. **Deletes** any existing rows in the target table for each symbol/date range.  
3. **Fetches** new data from the iVolatility `"/equities/eod/ivs"` API for each symbol.  
4. **Renames** columns (if needed) to align with the SQL schema.  
5. **Inserts** the fetched data into your **Azure SQL** table.  

It can be run **locally**, as a **Docker** container, or on a **Kubernetes** cluster (e.g., AKS) via a CronJob.

---

## Prerequisites

- **Python 3.9+** (if running locally without Docker).
- **Azure SQL** database with a user for your **Managed Identity**:
  ```sql
  CREATE USER [YourAksManagedIdentityName] FROM EXTERNAL PROVIDER;
  ALTER ROLE db_datareader ADD MEMBER [YourAksManagedIdentityName];
  ALTER ROLE db_datawriter ADD MEMBER [YourAksManagedIdentityName];
  ```
- **iVolatility** account & API key for the `"/equities/eod/ivs"` endpoint.
- **Docker** (if containerizing the app).
- (Optionally) **Kubernetes** (e.g., AKS) for CronJob scheduling.

---

## Project Structure

```
ivolatility-eod-ivs-etl/
├── etl_ivol_ivs.py      # Main Python ETL script
├── requirements.txt     # Python dependencies
├── Dockerfile           # Docker build configuration
├── .gitignore           # (Recommended) Git ignore patterns
├── .dockerignore        # (Recommended) Docker ignore patterns
└── README.md            # Project documentation (this file)
```

---

## Setup & Usage

### Local Testing

1. **Clone** the repository:
   ```bash
   git clone https://github.com/YourOrg/ivolatility-eod-ivs-etl.git
   cd ivolatility-eod-ivs-etl
   ```
2. **Install dependencies** (Python 3.9+ and pip):
   ```bash
   pip install -r requirements.txt
   ```
3. **Set environment variables** and **run**:
   ```bash
   export IVOL_API_KEY="YOUR_IVOL_API_KEY"
   export DB_SERVER="your-azure-sql.database.windows.net"
   export DB_NAME="YourDatabase"
   export TARGET_TABLE="etl.ivolatility_ivs"
   export DATE_FROM="2021-12-10"
   export DATE_TO="2021-12-17"
   export TICKER_SQL="SELECT symbol FROM MySymbols"
   # etc...

   python etl_ivol_ivs.py
   ```
4. The script will:
   - Delete rows in `[etl].[ivolatility_ivs]` for each symbol/date range.
   - Fetch new data from iVolatility and insert it.

### Docker Build & Run

1. **Build** the Docker image:
   ```bash
   docker build -t ivol-ivs-etl:latest .
   ```
2. **Run** the container, passing environment variables:
   ```bash
   docker run --rm      -e IVOL_API_KEY="YOUR_IVOL_API_KEY"      -e DB_SERVER="your-azure-sql.database.windows.net"      -e DB_NAME="YourDatabase"      -e TARGET_TABLE="etl.ivolatility_ivs"      -e DATE_FROM="2021-12-10"      -e DATE_TO="2021-12-17"      -e TICKER_SQL="SELECT symbol FROM MySymbols"      -e MAX_WORKERS="12"      ivol-ivs-etl:latest
   ```

### Kubernetes CronJob

Below is a **sample CronJob** that runs the ETL daily at 3 AM UTC on **AKS**:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ivol-ivs-etl-job
spec:
  schedule: "0 3 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: ivol-ivs-etl
              image: <your-registry>/ivol-ivs-etl:latest
              env:
                - name: IVOL_API_KEY
                  valueFrom:
                    secretKeyRef:
                      name: ivol-secret
                      key: IVOL_API_KEY
                - name: DB_SERVER
                  value: "your-azure-sql.database.windows.net"
                - name: DB_NAME
                  value: "YourDatabase"
                - name: TARGET_TABLE
                  value: "etl.ivolatility_ivs"
                - name: DATE_FROM
                  value: "2021-12-10"
                - name: DATE_TO
                  value: "2021-12-17"
                - name: TICKER_SQL
                  value: "SELECT symbol FROM MySymbols"
                - name: MAX_WORKERS
                  value: "12"
```

---

## Environment Variables

| Variable       | Description                                                                            | Required | Default                 |
|----------------|----------------------------------------------------------------------------------------|----------|-------------------------|
| `IVOL_API_KEY` | Your iVolatility API key.                                                              | **Yes**  | —                       |
| `DB_SERVER`    | Azure SQL server (e.g. `myserver.database.windows.net`).                                | **Yes**  | —                       |
| `DB_NAME`      | Name of the DB (e.g. `MyDatabase`).                                                    | **Yes**  | —                       |
| `TARGET_TABLE` | Target table name (e.g. `etl.ivolatility_ivs`).                                        | **No**   | `etl.ivolatility_ivs`   |
| `DATE_FROM`    | Start date for the API call (e.g. `YYYY-MM-DD`).                                       | **No**   | (script may default)    |
| `DATE_TO`      | End date for the API call.                                                             | **No**   | (script may default)    |
| `TICKER_SQL`   | T-SQL query returning symbols (and optionally region).                                 | **No**   | `SELECT symbol FROM MySymbols` |
| `MAX_WORKERS`  | Number of threads for parallel API fetching.                                           | **No**   | `12`                    |
| `OTM_FROM`     | `out-of-the-money` lower bound (int).                                                 | **No**   | `0`                     |
| `OTM_TO`       | `out-of-the-money` upper bound (int).                                                 | **No**   | `0`                     |
| `REGION`       | Default region if not provided in the ticker SQL.                                      | **No**   | `USA`                   |
| `PERIOD_FROM`  | Lower bound for period (int).                                                          | **No**   | `90`                    |
| `PERIOD_TO`    | Upper bound for period (int).                                                          | **No**   | `90`                    |

---

## Table Schema

```sql
CREATE TABLE [etl].[ivolatility_ivs] (
    [record_no]  INT            NOT NULL,
    [symbol]     NVARCHAR(50)   NULL,
    [exchange]   NVARCHAR(50)   NULL,
    [date]       DATE           NULL,
    [period]     INT            NULL,
    [strike]     DECIMAL(18,2)  NULL,
    [OTM]        INT            NULL,
    [Call_Put]   NVARCHAR(10)   NULL,
    [IV]         DECIMAL(10,6)  NULL,
    [delta]      DECIMAL(10,6)  NULL,

    CONSTRAINT PK_ivolatility_ivs
        PRIMARY KEY ([record_no])
);
```

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository and create a new branch for your feature or fix.  
2. Write tests if necessary and ensure the code passes.  
3. Submit a PR with a clear description of your changes.

---

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)

```
Copyright 2025 pvotio

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at 

   http://www.apache.org/licenses/LICENSE-2.0 

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" BASIS, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
See the License for the specific language governing permissions and 
limitations under the License.
```
