from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict, Optional
import asyncio
from api.clickhouse_client import TextToSQLClient  
import json

app = FastAPI()

# Initialize the TextToSQLClient instance
client = TextToSQLClient()

@app.get("/tables", response_model=List[str])
async def list_tables():
    """
    Endpoint to list all available tables in the database.
    """
    try:
        tables = await client.list_tables()
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching tables: {str(e)}")


@app.post("/select_table")
async def select_table(table_name: str):
    """
    Endpoint to select a table and retrieve its schema.
    """
    try:
        table_schema = await client.get_table_schema(table_name)
        if not table_schema:
            raise HTTPException(status_code=404, detail=f"Schema for table '{table_name}' not found.")
        
        # Store selected table and schema in the client instance
        client.selected_table = table_name
        client.table_schema = table_schema
        
        return {"message": f"Table '{table_name}' selected successfully.", "schema": table_schema}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error selecting table: {str(e)}")


@app.post("/query")
async def execute_query(user_query: str):
    """
    Endpoint to process a natural language query and execute it on the selected table.
    """
    try:
        # Ensure a table is selected
        if not hasattr(client, 'selected_table') or not hasattr(client, 'table_schema'):
            raise HTTPException(status_code=400, detail="No table has been selected for this session.")
        
        # Process the query
        result = await client.process_query_for_selected_table(user_query)
        
        # Handle errors in processing
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        
        # Parse the result JSON string to extract only the rows
        result_json = json.loads(result["result"].split("\n\n", 1)[1])
        rows = result_json.get("rows", [])
        
        return {
            "sql_query": result.get("sql_query", result.get("final_sql_query")),
            "result": rows
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")