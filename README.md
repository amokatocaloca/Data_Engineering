# Weather Data Pipeline

This project is a simple weather data ingestion pipeline. It fetches real-time weather data from Tomorrow.io for Almaty using Apache Spark and writes the data to a PostgreSQL database. The entire pipeline is containerized using Docker and managed with Docker Compose.

## Prerequisites

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- Install the required Python packages by running:

```bash
pip install -r requirements.txt
```

## Project Structure

- **main.py**: Contains the Python script that fetches the weather data, processes it with Spark, and writes it to PostgreSQL.
- **Dockerfile**: Used to build the Docker image for the Spark service.
- **docker-compose.yml**: Defines and manages the multi-container application (Spark and PostgreSQL).
- **requirements.txt**: Lists the Python dependencies for the project.

## Building and Running the Containers

1. **Build and start the containers:**  
   Make sure that your Docker doaemon is open running first!
   Open a terminal in the project directory and run:
   ```bash
   docker-compose up --build
   ```
   This command builds the Docker images (if necessary) and starts the Spark and PostgreSQL containers.

2. **Stopping the containers:**  
   To stop the running containers, either press CTRL+C in the terminal running Docker Compose or execute:
   ```bash
   docker-compose stop
   ```

## Accessing the PostgreSQL Database

To view and query the data in the PostgreSQL database, use the following command:
```bash
docker-compose exec postgres psql -U amira -d weather_data
```
This command opens the PostgreSQL interactive terminal. You can run SQL queries (e.g., `SELECT * FROM tomorrowio_data;`) to view your data.

## Additional Notes

- The pipeline fetches weather data every 180 seconds
- Logs from the Docker containers provide details on the fetching, processing, and writing operations
  
