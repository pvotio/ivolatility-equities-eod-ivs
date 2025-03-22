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

1. **Fetches ticker + region** from an **Azure SQL** table (via `SELECT TOP 100 Stock_ticker AS symbol, region FROM ivolatility_underlying_info WHERE Status = 'Active'`).
2. **Uses the first row** to retrieve:
   - A **T-SQL query** to fetch symbols + regions
   - A **default region** fallback (if not provided per symbol)
3. **Deletes** any existing rows in the target table for each symbol/date range.  
4. **Fetches** new data from the iVolatility `"/equities/eod/ivs"` API.  
5. **Renames** columns to align with the SQL schema.  
6. **Inserts** the fetched data into your **Azure SQL** table.  

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
├── main.py              # Main Python ETL script
├── requirements.txt     # Python dependencies
├── Dockerfile           # Docker build configuration
├── .gitignore           # Git ignore patterns
├── .dockerignore        # Docker ignore patterns
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
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Set environment variables** and **run**:
   ```bash
   export IVOL_API_KEY="YOUR_IVOL_API_KEY"
   export DB_SERVER="your-azure-sql.database.windows.net"
   export DB_NAME="YourDatabase"
   export TARGET_TABLE="etl.ivolatility_ivs"
   python main.py
   ```

### Docker Build & Run

1. **Build** the Docker image:
   ```bash
   docker build -t ivol-ivs-etl:latest .
   ```
2. **Run** the container:
   ```bash
   docker run --rm      -e IVOL_API_KEY="YOUR_IVOL_API_KEY"      -e DB_SERVER="your-azure-sql.database.windows.net"      -e DB_NAME="YourDatabase"      -e TARGET_TABLE="etl.ivolatility_ivs"      ivol-ivs-etl:latest
   ```

### Kubernetes CronJob

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
```

---

## Environment Variables

| Variable       | Description                                                  | Required | Default                 |
|----------------|--------------------------------------------------------------|----------|-------------------------|
| `IVOL_API_KEY` | Your iVolatility API key.                                    | **Yes**  | —                       |
| `DB_SERVER`    | Azure SQL server hostname.                                   | **Yes**  | —                       |
| `DB_NAME`      | Azure SQL database name.                                     | **Yes**  | —                       |
| `TARGET_TABLE` | Target table for inserts.                                    | **Yes**  | —                       |
| `DATE_FROM`    | Start date (optional, defaults to yesterday).                | No       | Yesterday               |
| `DATE_TO`      | End date (optional, defaults to today).                      | No       | Today                   |
| `MAX_WORKERS`  | Number of threads for concurrent API calls.                  | No       | 12                      |
| `OTM_FROM`     | Out-of-the-money lower bound.                                | No       | 0                       |
| `OTM_TO`       | Out-of-the-money upper bound.                                | No       | 0                       |
| `PERIOD_FROM`  | Period lower bound (days).                                   | No       | 90                      |
| `PERIOD_TO`    | Period upper bound (days).                                   | No       | 90                      |

---

## Table Schema

```sql
CREATE TABLE [etl].[ivolatility_ivs] (
    [record_no]  INT            NOT NULL,
    [symbol]     NVARCHAR(50),
    [region]     NVARCHAR(50),
    [date]       DATE,
    [Call_Put]   NVARCHAR(10),
    [OTM]        INT,
    [IV]         DECIMAL(10,6),
    [delta]      DECIMAL(10,6),
    CONSTRAINT PK_ivolatility_ivs PRIMARY KEY ([record_no])
);
```

---

## Contributing

We welcome contributions! Please:

1. Fork this repository.
2. Create a new branch (`git checkout -b feature/my-feature`).
3. Make your changes.
4. Commit (`git commit -am 'Add new feature'`).
5. Push (`git push origin feature/my-feature`) and create a Pull Request.

---

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)

```
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
