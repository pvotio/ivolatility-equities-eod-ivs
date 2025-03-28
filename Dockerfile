FROM python:3.13.2-slim-bullseye

# Install system dependencies + MS ODBC driver
RUN apt-get update && apt-get install -y \
    curl apt-transport-https gnupg && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list \
        > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y \
    msodbcsql18 \
    unixodbc-dev \
    && apt-get clean

# Copy requirements and install Python libs
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL script
COPY main.py .

# Default command
ENTRYPOINT ["python", "main.py"]
