import anyio
import click
import mcp.types as types
from mcp.server.lowlevel import Server
import clickhouse_connect
import pandas as pd
import logging
import os
import json
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

client = clickhouse_connect.get_client(
    host=os.getenv('CLICKHOUSE_HOST'),
    user=os.getenv('CLICKHOUSE_USER'),
    password=os.getenv('CLICKHOUSE_PASSWORD'),
    secure=os.getenv('CLICKHOUSE_SECURE')
)

@click.command()
@click.option("--port", default=8000, help="Port to listen on for SSE")
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse"]),
    default="stdio",
    help="Transport type",
)
def main(port: int, transport: str) -> int:
    app = Server("clickhouse-explorer")

    @app.list_resources()
    async def list_resources() -> list[types.Resource]:
        try:
            # Query to get all tables from the current database
            query = "SHOW TABLES"
            result = client.query(query)
            
            resources = []
            for row in result.result_set:
                table_name = row[0]
                resources.append(types.Resource(
                    uri=f"clickhouse:table:{table_name}",
                    name=table_name,
                    description=f"ClickHouse table: {table_name}",
                    mime_type="application/x-clickhouse-table"
                ))
            
            return resources
        except Exception as e:
            logger.error(f"Error listing resources: {str(e)}")
            return []

    @app.read_resource()
    async def read_resource(uri: str) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        try:
            # Parse the URI to extract table name
            if uri.startswith("clickhouse:table:"):
                table_name = uri.split("clickhouse:table:")[1]
                
                # Get column information
                query = f"DESCRIBE TABLE {table_name}"
                result = client.query(query)
                
                if not result.result_set:
                    return [types.TextContent(
                        type="text", 
                        text=f"Table {table_name} not found or has no columns."
                    )]
                
                # Format the schema in the requested JSON structure
                schema_json = {
                    table_name: {
                        "row_data": {}
                    }
                }
                
                for row in result.result_set:
                    column_name = row[0]
                    data_type = row[1]
                    schema_json[table_name]["row_data"][column_name] = {
                        "type": data_type
                    }
                
                # Get sample data (first 5 rows)
                sample_query = f"SELECT * FROM {table_name} LIMIT 5"
                try:
                    sample_result = client.query(sample_query)
                    if sample_result.result_set:
                        df = pd.DataFrame(sample_result.result_set, columns=sample_result.column_names)
                        sample_text = f"\n\nSample data (first 5 rows):\n\n{df.to_string()}"
                        
                        # Add the sample data as a separate text content
                        return [
                            types.TextContent(
                                type="text", 
                                text=json.dumps(schema_json, indent=2)
                            ),
                            types.TextContent(
                                type="text", 
                                text=sample_text
                            )
                        ]
                except Exception as e:
                    logger.error(f"Error getting sample data: {str(e)}")
                
                return [types.TextContent(
                    type="text", 
                    text=json.dumps(schema_json, indent=2)
                )]
            else:
                return [types.TextContent(
                    type="text", 
                    text=f"Unknown resource URI format: {uri}"
                )]
        except Exception as e:
            logger.error(f"Error reading resource: {str(e)}")
            return [types.TextContent(
                type="text", 
                text=f"Error reading resource {uri}: {str(e)}"
            )]

    @app.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="get_table_schema",
                description="Retrieve the schema (column definitions) for a specific ClickHouse table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {"type": "string", "description": "Name of the table to get schema for"}
                    },
                    "required": ["table_name"]
                }
            ),
            types.Tool(
                name="execute_custom_query",
                description="Execute a custom SQL query on ClickHouse database",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Custom SQL query to execute"},
                        "table_name": {"type": "string", "description": "Name of the table being queried"}
                    },
                    "required": ["query"]
                }
            )
        ]

    @app.call_tool()
    async def call_tool(
        name: str,
        arguments: dict
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        if name == "get_table_schema":
            table_name = arguments["table_name"]

            try:
                logger.info(f"Retrieving schema for table: {table_name}")
                # Get column information
                query = f"DESCRIBE TABLE {table_name}"
                result = client.query(query)

                if not result.result_set:
                    return [types.TextContent(
                        type="text", 
                        text=f"Table {table_name} not found or has no columns."
                    )]

                # Format the schema in the requested JSON structure
                schema_json = {
                    table_name: {
                        "row_data": {}
                    }
                }

                for row in result.result_set:
                    column_name = row[0]
                    data_type = row[1]
                    schema_json[table_name]["row_data"][column_name] = {
                        "type": data_type
                    }

                # Convert to formatted JSON string
                formatted_json = json.dumps(schema_json, indent=2)

                return [types.TextContent(
                    type="text", 
                    text=formatted_json
                )]
            except Exception as e:
                logger.error(f"Error retrieving schema: {str(e)}")
                return [types.TextContent(
                    type="text", 
                    text=f"Error retrieving schema for table {table_name}: {str(e)}"
                )]
        elif name == "execute_custom_query":
            query = arguments["query"]
            table_name = arguments.get("table_name", "")  # Optional parameter

            try:
                logger.info(f"Executing query on table {table_name}: {query}")
                result = client.query(query)

                if not result.result_set:
                    return [types.TextContent(
                        type="text", 
                        text="Query executed successfully, but returned no data."
                    )]

                # Convert the result set into structured JSON
                result_json = {
                    "columns": [
                        {"name": col, "type": "unknown"} for col in result.column_names
                    ],
                    "rows": [
                        dict(zip(result.column_names, row)) for row in result.result_set
                    ]
                }

                # Convert to formatted JSON string
                formatted_json = json.dumps(result_json, indent=2)

                return [types.TextContent(
                    type="text", 
                    text=f"Query executed successfully:\n\n{formatted_json}"
                )]
            except Exception as e:
                logger.error(f"Error executing query: {str(e)}")
                return [types.TextContent(
                    type="text", 
                    text=f"Error executing custom query: {str(e)}"
                )]

        raise ValueError(f"Tool not found: {name}")

    if transport == "sse":
        from mcp.server.sse import SseServerTransport
        from starlette.applications import Starlette
        from starlette.routing import Mount, Route

        sse = SseServerTransport("/messages/")

        async def handle_sse(request):
            async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
                await app.run(
                    streams[0],
                    streams[1],
                    app.create_initialization_options()
                )

        starlette_app = Starlette(
            debug=True,
            routes=[
                Route("/sse", endpoint=handle_sse),
                Mount("/messages/", app=sse.handle_post_message),
            ],
        )
        import uvicorn
        logger.info(f"Starting ClickHouse explorer server on port {port} with SSE transport")
        uvicorn.run(starlette_app, host="0.0.0.0", port=port)
    else:
        from mcp.server.stdio import stdio_server

        async def arun():
            logger.info("Starting ClickHouse explorer server with stdio transport")
            async with stdio_server() as streams:
                await app.run(
                    streams[0],
                    streams[1],
                    app.create_initialization_options()
                )
        anyio.run(arun)

    return 0

if __name__ == '__main__':
    main()