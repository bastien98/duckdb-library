# DuckDBWrapper

DuckDBWrapper is a Python class that provides a convenient interface for working with DuckDB, including operations with S3 and local files.

## Installation

```bash
pip install duckdb boto3 pandas
```

## Usage

### Initializing the Wrapper

```python
from duckdbwrapper import DuckDBWrapper

# For in-memory database
db = DuckDBWrapper()

# For persistent database
# Note: The file does not need to exist; DuckDB will create it if it doesn't
db = DuckDBWrapper("path/to/your/database.db")

# Using with context manager
with DuckDBWrapper() as db:
    # Your code here
```

### Connecting to S3

```python
db.create_duckdb_connection()
db.connect_to_s3()
```

### Reading from S3

```python
# Read Parquet file from S3
# Note: To read entire folder user: "s3://your-bucket/path/to/*.parquet"
file = "s3://your-bucket/path/to/file.parquet"
data = db.read_file(file)

# Read CSV file from S3
# Note: To read entire folder user: "s3://your-bucket/path/to/*.csv"
file = "s3://your-bucket/path/to/file.csv"
data = db.read_file(file)

# Read specific columns and apply filters
columns = ["column1", "column2"]
filters = ["column1 > 10", "column2 != 'value'"]
data = db.read_file(file, select_columns=columns, filter_conditions=filters)
```

### Storing Data as a Table

```python
# Assuming 'data' is a list of dictionaries
db.create_table("my_table", data)

# You can now query this table
result = db.execute_query("SELECT * FROM my_table WHERE column1 > 10")
```

### Performing Transformations

```python
# Example: Aggregating data
query = """
    SELECT column1, AVG(column2) as avg_col2
    FROM my_table
    GROUP BY column1
    HAVING AVG(column2) > 50
"""
aggregated_data = db.execute_query(query)

# Example: Joining tables
join_query = """
    SELECT t1.column1, t2.column2
    FROM table1 t1
    JOIN table2 t2 ON t1.id = t2.id
    WHERE t1.column1 > 100
"""
joined_data = db.execute_query(join_query)
```

### Saving Back to S3

```python
# Save query results as Parquet to S3
s3_output_path = "s3://your-bucket/path/to/output.parquet"
db.save_parquet(aggregated_data, s3_output_path)

# Save a table as Parquet to S3
db.save_table_as_parquet("my_table", "s3://your-bucket/path/to", "output_file")
```

### Executing Custom Queries

```python
# Execute a simple query
result = db.execute_query("SELECT * FROM my_table LIMIT 10")

# Execute a parameterized query
params = [100, 'category']
result = db.execute_query("SELECT * FROM my_table WHERE value > ? AND category = ?", params)

# Execute a query and fetch results one at a time
for row in db.execute_query_fetch_one("SELECT * FROM large_table"):
    process_row(row)
```

## Closing the Connection

When you're done with the DuckDB connection, make sure to close it:

```python
db.close()
```

Or, if you're using a context manager, it will be closed automatically when you exit the `with` block.

## Note on S3 Credentials

The `connect_to_s3()` method uses boto3 to fetch AWS credentials. Make sure you have properly configured your AWS credentials (e.g., through environment variables, AWS CLI configuration, or IAM roles if running on AWS infrastructure).

