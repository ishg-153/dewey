import os
import requests
import pandas as pd
from sqlalchemy import create_engine
import urllib
from io import StringIO

# Get credentials from environment variables
CANVAS_API_TOKEN = os.getenv('CANVAS_API_TOKEN')
CANVAS_BASE_URL = os.getenv('CANVAS_BASE_URL')  # e.g., "https://utexas.instructure.com"
COURSE_ID = os.getenv('CANVAS_COURSE_ID')
SQL_CONNECTION_STRING = os.getenv('SQL_CONNECTION_STRING')

print("Starting Canvas to SQL pipeline...")

# Canvas API headers
headers = {
    'Authorization': f'Bearer {CANVAS_API_TOKEN}'
}

# Get list of files in the course
print(f"📚 Fetching files from Canvas course {COURSE_ID}...")
files_url = f"{CANVAS_BASE_URL}/api/v1/courses/{COURSE_ID}/files"
response = requests.get(files_url, headers=headers)
files = response.json()

# Filter for CSV files only
csv_files = [f for f in files if f['filename'].endswith('.csv')]
print(f"Found {len(csv_files)} CSV files")

# Create SQL engine
print("🔌 Connecting to Azure SQL Database...")
connection_parts = SQL_CONNECTION_STRING.split(';')
server = next(p.split('=')[1] for p in connection_parts if 'Server=' in p)
database = next(p.split('=')[1] for p in connection_parts if 'Initial Catalog=' in p or 'Database=' in p)
username = next(p.split('=')[1] for p in connection_parts if 'User ID=' in p)
password = next(p.split('=')[1] for p in connection_parts if 'Password=' in p)

connection_string = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
)

params = urllib.parse.quote_plus(connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
print("✓ Database connection established")

# Download and load each CSV file
print("\n📊 Loading Canvas CSV files into SQL Database...")
for file_info in csv_files:
    try:
        file_name = file_info['filename']
        download_url = file_info['url']
        table_name = file_name.replace('.csv', '').replace('-', '_').replace(' ', '_').lower()
        
        print(f"  Loading {file_name}...", end=' ')
        
        # Download CSV content
        file_response = requests.get(download_url, headers=headers)
        csv_content = file_response.text
        
        # Read CSV into DataFrame
        df = pd.read_csv(StringIO(csv_content))
        
        # Replace NaN with None for SQL compatibility
        df = df.where(pd.notnull(df), None)
        
        # Insert into SQL database
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"✓ {len(df)} rows")
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")

print("\n🎓 Canvas data pipeline complete!")
```
