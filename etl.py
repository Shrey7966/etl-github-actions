import requests
import pandas as pd
import boto3
import os
from datetime import datetime

# Load environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

# Step 1: Extract data from API
API_URL = "https://disease.sh/v3/covid-19/countries"
response = requests.get(API_URL)
data = response.json()

# Step 2: Transform data (convert JSON to DataFrame & clean)
df = pd.DataFrame(data)[["country", "cases", "deaths", "recovered", "active"]]
df["date"] = datetime.utcnow().strftime("%Y-%m-%d")

# Save transformed data as CSV
csv_filename = "covid_data.csv"
df.to_csv(csv_filename, index=False)

# Step 3: Load data to AWS S3
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)
s3_client.upload_file(csv_filename, S3_BUCKET, csv_filename)

print(f"File {csv_filename} uploaded to {S3_BUCKET}")
