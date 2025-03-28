import asyncio
import os
import json
import yaml
from openai import OpenAI
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

class TextToSQLClient:
    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.server_params = StdioServerParameters(
            command="python",
            args=["clickhouse_server.py"],
            env=None,
        )
        # Load prompts from YAML file
        with open("prompts_clickhouse.yaml", "r") as file:
            self.prompts = yaml.safe_load(file)
    
    async def list_tables(self):
        """List all available tables in the database"""
        async with stdio_client(self.server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                resources = await session.list_resources()
                return [resource.name for resource in resources.resources]

    async def get_table_schema(self, table_name):
        """Retrieve the schema for a specific table"""
        async with stdio_client(self.server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tool_result = await session.call_tool(
                    "get_table_schema", 
                    {"table_name": table_name}
                )
                if hasattr(tool_result, 'content'):
                    for content_item in tool_result.content:
                        if hasattr(content_item, 'text'):
                            return content_item.text
                return None

    async def ainput(self, prompt: str = "") -> str:
        """Asynchronous input function"""
        with ThreadPoolExecutor(1, "AsyncInput") as executor:
            return await asyncio.get_event_loop().run_in_executor(executor, input, prompt)

    async def rephrase_query(self, user_query):
        """Rephrase and analyze the user query using OpenAI"""
        prompt_template = self.prompts["prompts"]["rephrase_user_query"]["template"]
        prompt = prompt_template.replace("{{ user_query }}", user_query)
        
        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an expert at analyzing queries and returning results in JSON format."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        
        if result["query_type"] == "wrong":
            raise ValueError("Invalid or irrelevant query. Please ask a database-related question.")
        
        rephrased_query = result["queries"][0] if result["queries"] else user_query
        print(f"Rephrased query: {rephrased_query}")
        return rephrased_query
    
    async def generate_sql(self, rephrased_query, table_name, table_schema):
        """Generate SQL query using the rephrased query and table schema information"""
        prompt_template = self.prompts["prompts"]["query_generation_prompt"]["template"]
        
        prompt = prompt_template.replace("{{ table_name }}", table_name).replace("{{ schema }}", table_schema)
        prompt += f"\n\nQuestion: {rephrased_query}"
        
        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.0
        )
        
        sql_query = response.choices[0].message.content.strip()
        print(f"Generated SQL: {sql_query}")
        return sql_query
    
    async def retry_failed_query(self, error_message, original_query, table_name, table_schema, rephrased_query):
        """Generate a corrected SQL query based on the error message and original query"""
        prompt_template = self.prompts["prompts"]["retry_query_prompt"]["template"]
        
        prompt = (prompt_template
                 .replace("{{ error_message }}", error_message)
                 .replace("{{ original_query }}", original_query)
                 .replace("{{ table_name }}", table_name)
                 .replace("{{ schema }}", table_schema)
                 .replace("{{ rephrased_query }}", rephrased_query))
        
        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.0
        )
        
        corrected_sql_query = response.choices[0].message.content.strip()
        print(f"Corrected SQL: {corrected_sql_query}")
        return corrected_sql_query
    
    async def execute_query(self, sql_query, table_name):
        """Execute SQL query using MCP server"""
        async with stdio_client(self.server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                tool_result = await session.call_tool(
                    "execute_custom_query", 
                    {"query": sql_query, "table_name": table_name}
                )
                
                if hasattr(tool_result, 'content'):
                    for content_item in tool_result.content:
                        if hasattr(content_item, 'text'):
                            return content_item.text
                        elif isinstance(content_item, dict) and 'text' in content_item:
                            return content_item['text']
                
                return f"Query executed, but couldn't parse the result: {tool_result}"
    
    async def summarize_results(self, user_query, sql_output):
        """Summarize SQL query results in relation to the original user query"""
        prompt_template = self.prompts["prompts"]["summarization_prompt"]["template"]
        prompt = (prompt_template
                .replace("{{ user_query }}", user_query)
                .replace("{{ sql_output }}", sql_output))
        
        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.0
        )
        
        summary = response.choices[0].message.content.strip()
        print(f"Generated summary: {summary}")
        return summary

    async def process_query_for_selected_table(self, user_query, max_retries=3):
        """Process a query for the already selected table"""
        try:
            if not hasattr(self, 'selected_table') or not hasattr(self, 'table_schema'):
                return {"error": "No table has been selected for this session"}
                
            rephrased_query = await self.rephrase_query(user_query)
            sql_query = await self.generate_sql(rephrased_query, self.selected_table, self.table_schema)
            
            current_query = sql_query
            original_query = sql_query
            retry_count = 0
            error_messages = []
            
            while retry_count < max_retries:
                result = await self.execute_query(current_query, self.selected_table)
                
                if "Error executing custom query:" in result:
                    print(f"Query execution failed (attempt {retry_count + 1}/{max_retries}). Attempting to correct...")
                    
                    error_message = result.split("Error executing custom query:")[1].strip()
                    error_messages.append(error_message)
                    
                    corrected_sql = await self.retry_failed_query(
                        error_message, 
                        current_query,
                        self.selected_table,
                        self.table_schema,
                        rephrased_query
                    )
                    
                    current_query = corrected_sql
                    retry_count += 1
                    
                    if retry_count == max_retries:
                        final_result = await self.execute_query(current_query, self.selected_table)
                        
                        if "Error executing custom query:" in final_result:
                            return {
                                "user_query": user_query,
                                "rephrased_query": rephrased_query,
                                "selected_table": self.selected_table,
                                "original_sql_query": original_query,
                                "error_messages": error_messages,
                                "final_sql_query": current_query,
                                "result": "All query correction attempts failed. Please try rephrasing your question."
                            }
                        else:
                            summary = await self.summarize_results(user_query, final_result)
                            return {
                                "user_query": user_query,
                                "rephrased_query": rephrased_query,
                                "selected_table": self.selected_table,
                                "original_sql_query": original_query,
                                "error_messages": error_messages,
                                "final_sql_query": current_query,
                                "result": final_result,
                                "summary": summary
                            }
                else:
                    summary = await self.summarize_results(user_query, result)
                    
                    if retry_count == 0:
                        return {
                            "user_query": user_query,
                            "rephrased_query": rephrased_query,
                            "selected_table": self.selected_table,
                            "sql_query": sql_query,
                            "result": result,
                            "summary": summary
                        }
                    else:
                        return {
                            "user_query": user_query,
                            "rephrased_query": rephrased_query,
                            "selected_table": self.selected_table,
                            "original_sql_query": original_query,
                            "error_messages": error_messages,
                            "final_sql_query": current_query,
                            "result": result,
                            "summary": summary
                        }
        except Exception as e:
            return {
                "user_query": user_query,
                "error": str(e)
            }

async def main():
    client = TextToSQLClient()
    
    print("\n" + "="*50)
    print("Text-to-SQL Query Executor")
    print("="*50)
    
    # Get available tables at the start of the session
    tables = await client.list_tables()
    print("Available tables:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    # Let user select a table for the entire session
    selected_table = None
    while not selected_table:
        try:
            selection = int(await client.ainput("Enter the number of the table you want to query: "))
            if 1 <= selection <= len(tables):
                selected_table = tables[selection - 1]
                print(f"Selected table: {selected_table}")
                
                # Get and store schema for the selected table
                table_schema = await client.get_table_schema(selected_table)
                if not table_schema:
                    print(f"Error: Could not retrieve schema for table {selected_table}")
                    return
                
                # Store the selected table and schema in the client
                client.selected_table = selected_table
                client.table_schema = table_schema
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Please enter a valid number.")
    
    # Now proceed with queries on the selected table
    while True:
        print("\n" + "="*50)
        print(f"Querying table: {selected_table}")
        print("="*50)
        print("Enter your query in natural language (or 'exit' to quit):")
            
        user_query = input("> ")
        if user_query.lower() == 'exit':
            break
            
        print("\nProcessing your query...")
        result = await client.process_query_for_selected_table(user_query)
            
        if "error" in result:
            print(f"\nError: {result['error']}")
        else:
            print("\nRephrased Query:")
            print(result["rephrased_query"])
            
            if "original_sql_query" in result and "final_sql_query" in result:
                print("\nOriginal SQL Query (Failed):")
                print(result["original_sql_query"])
                
                print("\nError Messages:")
                for i, error in enumerate(result["error_messages"]):
                    print(f"Attempt {i+1}: {error}")
                
                print("\nFinal SQL Query:")
                print(result["final_sql_query"])
            else:
                print("\nSQL Query:")
                print(result["sql_query"])
            
            print("\nResult:")
            print(result["result"])
            
            if "summary" in result:
                print("\nSummary:")
                print(result["summary"])

if __name__ == "__main__":
    asyncio.run(main())