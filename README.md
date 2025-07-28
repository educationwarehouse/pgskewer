
# pgskewer

A minimalistic pipeline functionality built on top of [pgqueuer](https://github.com/educationwarehouse/pgqueuer), providing enhanced task orchestration capabilities for PostgreSQL-based job queues.

> The name "pgskewer" is chosen due to its metaphorical representation of how it "skewers" tasks together in a pipeline; 
> rhymes with "pgqueuer" and contains "EW" (Education Warehouse) in its name.

## Features

- **Sequential and Parallel Task Pipelines**: Define complex workflows with a mix of sequential and parallel task execution
- **Result Storage**: Automatically store and pass around task results in sequence
- **Cancelable Tasks**: Gracefully cancel tasks when needed
- **Improved Error Handling**: Better error propagation and handling in pipelines
- **Real-time Log Streaming**: Stream logs from blocking functions in real-time
- **Type Annotations**: Comprehensive typing support for better IDE integration

## Installation

```bash
uv pip install pgskewer
```

## Requirements

- Python ≥ 3.13
- PostgreSQL database
- Dependencies:
  - pgqueuer
  - asyncpg
  - anyio
  - uvloop

## Quick Start

```python
import asyncio
import asyncpg
from pgqueuer import Job
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries
from pgskewer import ImprovedQueuer, parse_payload, TaskResult

async def main():
    # Initialize the queuer with your database connection
    connection = await asyncpg.connect("postgresql://user:password@localhost/dbname")
    driver = AsyncpgDriver(connection)
    pgq = ImprovedQueuer(driver)
    
    # Define some tasks as entrypoints
    @pgq.entrypoint("fetch_data")
    async def fetch_data(job):
        # Fetch some data
        return {"data": "example data"}
    
    @pgq.entrypoint("process_data")
    async def process_data(job: Job):
        # Process the data from the previous step
        payload = parse_payload(job.payload)
        data = payload["tasks"]["fetch_data"]["result"]["data"]
        return {"processed": data.upper()}
    
    @pgq.entrypoint("store_results")
    async def store_results(job: Job):
        # Store the processed data
        payload = parse_payload(job.payload)
        processed = payload["tasks"]["process_data"]["result"]["processed"]
        # Store the processed data somewhere
        return {"status": "completed", "stored": processed}
    
    # Create a pipeline that runs these tasks in sequence
    pgq.entrypoint_pipeline(
        "my_pipeline",
        # start steps as a mix of entrypoint names and function references:
        fetch_data,
        "process_data",
        store_results
    )
    
    # Execute the pipeline (empty initial data)
    job_id = await pgq.qm.queries.enqueue("my_pipeline", b'')
    # when the pipeline completes, pgqueuer_result should have an entry for this job_id:
    result: TaskResult = await pgq.result(job_id, timeout=None)
    
    
if __name__ == "__main__":
    asyncio.run(main())
```

## Advanced Usage

### Creating Pipelines with Parallel Tasks

You can define pipelines with a mix of sequential and parallel tasks:

```python
# Define a pipeline with parallel tasks
pipeline = pgq.pipeline([
    "task_1",                    # Run task_1 first
    ["task_2a", "task_2b"],      # Then run task_2a and task_2b in parallel
    "task_3"                     # Finally run task_3 after both task_2a and task_2b complete
])
```

### Register a Pipeline as an Entrypoint

You can register a pipeline as an entrypoint for reuse:

```python
# Register the pipeline as an entrypoint
pgq.entrypoint_pipeline(
    "data_processing_pipeline",
    "fetch_data",
    ["validate_data", "normalize_data"],
    "store_data"
)

# Now you can enqueue this pipeline like any other task
job_id = await pgq.enqueue("data_processing_pipeline", {"source": "api"})
```

### Running Blocking Functions Asynchronously

pgskewer provides utilities to run blocking functions asynchronously with real-time log streaming:

```python
from pgskewer import unblock

def cpu_intensive_task(data):
    # This is a blocking function
    print("Processing data...")
    result = process_data(data)
    print("Processing complete!")
    return result

# Run the blocking function asynchronously with log streaming
result = await unblock(cpu_intensive_task, data)
```

### Pipeline Result Structure

The pipeline returns a structured result with information about each task:

```python
{
    "initial": {
        # The initial payload provided to the pipeline
    },
    "tasks": {
        "task_1": {
            "status": "successful",
            "ok": true,
            "result": {
                # Task 1's result data
            }
        },
        "task_2a": {
            "status": "successful",
            "ok": true,
            "result": {
                # Task 2a's result data
            }
        },
        # ... other tasks
    }
}
```

## Error Handling

If any task in a pipeline fails:
- In sequential execution, the pipeline stops and no further tasks are executed
- In parallel execution, sibling tasks are terminated

## License

`pgskewer` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

## Credits

Developed by [Education Warehouse](https://educationwarehouse.nl/).