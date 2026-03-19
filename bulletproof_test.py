from coreason_etl_faers.config import FaersExtractionPolicy
from coreason_etl_faers.main import execute_faers_etl_transmutation_task
from coreason_etl_faers.utils.logger import logger

# Point the pipeline at our fake local FDA website
policy = FaersExtractionPolicy(
    source_quarter="2023q4",
    base_url="http://localhost:8000/index.html"
)

# Connect to your Docker Postgres instance on port 5435
connection_uri = "postgresql://admin:postgrespassword@localhost:5435/faers_db"

if __name__ == "__main__":
    logger.info("Starting Bulletproof Local Test...")
    execute_faers_etl_transmutation_task(policy, connection_uri)
    logger.info("Pipeline Execution Complete!")
