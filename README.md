
## Project Overview

I solved this project by developing a data pipeline to analyze the price difference of swaps executed via the Uniswap Protocol against market averages. To do this I used AWS for hosting a PostgreSQL database on my private account and Apache Airflow for orchestration. The pipeline fetches, processes, and stores trading and market data. Lastly, a column with price improvement per trade is calculated along with an average of all trade being printed out in the logs.

### Project Components

- **AWS PostgreSQL Database**: Stores all data.
- **Dune Analytics**: Data on swaps are fetched from a Dune query, allowing parameterized fetching to retrieve only the most recent data. [View the query here](https://dune.com/queries/3941831/6630301).
- **Crypto Compare API**: Provides near-real-time market prices to compare against trade execution prices.
- **Airflow**: Orchestrates the pipeline's tasks on a adaily basis.

### Repository Structure

In this repository please explore the following key directories and files:

- `dags/`: Contains Airflow DAGs defining the workflow and Python scripts for fetching data from Dune and Crypto Compare APIs and calculating price difference.
- `db_management/`: Includes scripts for database operations like creating views.
- `Dockerfile, docker-compose.yml, .env and requirements.txt`: These files are used to build the docker container.
- `notebooks`: If you want to try to interact with the code with a jupyter notebook. If so you need to active the .conda virtual environment or build your own using the requirements_local_conda.txt file.

### Running the Project

1. **Environment Setup**: Ensure Docker and VS Code are installed.
2. **Start the Application**:
   - Open the project folder in VS Code.
   - Run `docker compose up` in the terminal. Wait for the services to initialize.
3. **Access Airflow UI**:
   - Navigate to [http://localhost:8080/home](http://localhost:8080/home) in your web browser.
   - Log in with username `airflow` and password `airflow`.
4. **Execute and Monitor Pipeline**:
   - Locate the `price_improvement_dag` and manually trigger the DAG by clicking the "play button".
   - Monitor task progress and also check out the dependencies in the DAG's Graph View by clicking the name of the DAG and then "Graph".
5. **View Results**:
   - Access results from the DAG's execution logs under `logs/dag_id=price_improvement_dag/run_id=manual__[timestamp]/task_id=calculate_price_improvement`. Look for the `attemp=1.log` file in the folder with the latest timestamp. Inside the file please search for 3 hashtags "###" and finde a line like this:
     ```
     [2024-07-29T13:04:29.342+0000] {calculate_price_improvement.py:24} INFO - ### The average price difference for all the swaps was -0.175 percent ###
     ```

### Stopping the Project and clean up

To halt the project and clean up Docker images, use:
```bash
docker compose down --rmi all
```

### Considerations and improvements for production readiness

Unfortunately it Crypto Compare only has the last 7 days of prices on the minute granularity. So I opted for Cryptoo Compare daily closing prices:

https://min-api.cryptocompare.com/documentation?key=Historical&cat=dataHistoday

These are obvioulsly not very acurate, but the idea of the project was to show a working pipeline not so much interesting analytical findings.

In transitioning to a production environment, I would consider the following:

- **Security**: Implement managed secret handling services like AWS Secrets Manager for sensitive data.
- **Scalability**: Utilize Celery with Airflow for distributed task execution.
- **Reliability**: Enhance error handling with alerts. Add comprehensive logging and monitoring.
- **Data Integrity**: Implement data quality checks and validations throughout the data pipeline.
- **Compliance and Documentation**: Maintain thorough documentation and ensure compliance with data protection regulations.
