import os
import requests
import pandas as pd
from sqlalchemy import create_engine
import urllib
from io import StringIO

# Get credentials from environment variables
CANVAS_API_TOKEN = os.getenv('CANVAS_API_TOKEN')
CANVAS_BASE_URL = os.getenv('CANVAS_BASE_URL')
COURSE_ID = os.getenv('CANVAS_COURSE_ID')
SQL_CONNECTION_STRING = os.getenv('SQL_CONNECTION_STRING')

print("Starting Canvas to SQL pipeline...")
print(f"Course: {CANVAS_BASE_URL}/courses/{COURSE_ID}")

# Canvas API headers
headers = {
    'Authorization': f'Bearer {CANVAS_API_TOKEN}'
}

# Step 1: Get list of folders in the course
print(f"\n📁 Finding 'data' folder in Canvas course {COURSE_ID}...")
folders_url = f"{CANVAS_BASE_URL}/api/v1/courses/{COURSE_ID}/folders"

try:
    folders_response = requests.get(folders_url, headers=headers)
    folders_response.raise_for_status()
    folders = folders_response.json()
    
    print(f"Found {len(folders)} folders in course:")
    for folder in folders:
        print(f"  - {folder['name']} (ID: {folder['id']})")
    
except Exception as e:
    print(f"❌ Error fetching folders: {str(e)}")
    exit(1)

# Find the "data" folder
data_folder = None
for folder in folders:
    if folder['name'].lower() == 'data':
        data_folder = folder
        break

if not data_folder:
    print("❌ Error: 'data' folder not found in course")
    print("Available folders:", [f['name'] for f in folders])
    exit(1)

print(f"✓ Found 'data' folder (ID: {data_folder['id']})")

# Step 2: Get files from the data folder
print(f"\n📚 Fetching files from 'data' folder...")
files_url = f"{CANVAS_BASE_URL}/api/v1/folders/{data_folder['id']}/files"

try:
    files_response = requests.get(files_url, headers=headers)
    files_response.raise_for_status()
    files = files_response.json()
    
    # Filter for CSV files only
    csv_files = [f for f in files if f['filename'].endswith('.csv')]
    
    print(f"Found {len(csv_files)} CSV file(s) in 'data' folder:")
    for f in csv_files:
        print(f"  - {f['filename']}")
    
    if len(csv_files) == 0:
        print("⚠️ No CSV files found in data folder")
        exit(0)
        
except Exception as e:
    print(f"❌ Error fetching files: {str(e)}")
    exit(1)

# Step 3: Connect to SQL Database
print("\n🔌 Connecting to Azure SQL Database...")

try:
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
    
except Exception as e:
    print(f"❌ Error connecting to database: {str(e)}")
    exit(1)

# Step 4: Download and load each CSV file
print("\n📊 Loading Canvas CSV files into SQL Database...")
success_count = 0
error_count = 0

for file_info in csv_files:
    try:
        file_name = file_info['filename']
        download_url = file_info['url']
        
        # Clean up table name
        table_name = file_name.replace('.csv', '').replace('-', '_').replace(' ', '_').lower()
        
        # Shorten very long table names
        if len(table_name) > 50:
            table_name = table_name[:50]
        
        print(f"  Loading {file_name} → {table_name}...", end=' ')
        
        # Download CSV content
        file_response = requests.get(download_url, headers=headers)
        file_response.raise_for_status()
        csv_content = file_response.text
        
        # Read CSV into DataFrame
        df = pd.read_csv(StringIO(csv_content))
        
        # Replace NaN with None for SQL compatibility
        df = df.where(pd.notnull(df), None)
        
        # Insert into SQL database
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"✓ {len(df)} rows")
        success_count += 1
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        error_count += 1

print(f"\n🎓 Canvas data pipeline complete!")
print(f"   ✓ {success_count} file(s) loaded successfully")
if error_count > 0:
    print(f"   ✗ {error_count} file(s) failed")
