# Final Project: Lighthouse Adventure Works

## Project Description
The project consists of executing the modern data stack pipeline for the fictitious company Adventure Works, including the configuration of the data warehouse, data transformation, and creation of dashboards using BI tools.

## Requirements
- List of technologies and tools used:
  - dbt Cloud
  - BigQuery
  - Power BI
  - Git/GitHub

## Project Setup
- Step-by-step instructions to clone the repository:
  ```bash
  git clone https://github.com/LorenzoGaribotti/projeto-final-lorenzo
  ```
- Configuring BigQuery with dbt Cloud:
    - Create a service account with a JSON key in BigQuery.
    - Connect dbt Cloud with this JSON key.
    - Connect the repository to dbt Cloud.

- Details 
    - The project's seeds were deleted from the repository after running dbt seed to reduce the time required for dbt run. (The seeds can be obtained from this repository: https://github.com/techindicium/academy-dbt).
## Project Execution
- Important commands:

  - `dbt seed` imports the raw tables into the data warehouse.
    ```bash
    dbt seed
    ```

  - `dbt run` executes all the project's scripts, respecting dependencies.
    ```bash
    dbt run
    ```

  - `dbt test` runs all the tests defined in the .yml files and the 'tests' folder.
    ```bash
    dbt test
    ```

  - `dbt build` runs all scripts and tests for the project.
    ```bash
    dbt build
    ```

- Description of the layers creation:
  - **stg**: In the staging layer, the raw tables were processed by adjusting data types, column positions, and names.
  - **dim**: The dimensions were built to describe the data in the fact table, considering the star schema to enable healthy connections in Power BI.
  - **fact**: The fact table provides the data and answers to our questions. It also includes the necessary IDs for future connections.
  - **aggs**: Aggregated tables were built with both processing efficiency and better analysis for Data Science in mind.

## Visualizations and Dashboards
- Dashboards were created to answer the project's key questions. The dashboards were sent in .pbix format to the designated email.

## Validation and Tests
- All tables created in this project are subject to tests that ensure data quality.
