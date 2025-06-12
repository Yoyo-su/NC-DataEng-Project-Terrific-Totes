# Data Engineering Project - Terrific Totes

## 🤝 Meet The Team  
Funny Silly Crew in Full Action

<table>
  <tr>
    <td><strong>Fidele</strong><br>
      <a href="https://github.com/fmunyaneza">
        <img src="https://img.shields.io/badge/GitHub-000?logo=github&logoColor=white" />
      </a><br>
      <a href="https://www.linkedin.com/in/fidele-munyaneza-b87372328/">
        <img src="https://img.shields.io/badge/LinkedIn-blue?logo=linkedin&logoColor=white" />
      </a>
    </td>
    <td><strong>Sukanya</strong><br>
      <a href="https://github.com/sukansasi">
        <img src="https://img.shields.io/badge/GitHub-000?logo=github&logoColor=white" />
      </a><br>
      <a href="https://www.linkedin.com/in/sukanyasasi15011526">
        <img src="https://img.shields.io/badge/LinkedIn-blue?logo=linkedin&logoColor=white" />
      </a>
    </td>
  </tr>
  <tr>
    <td><strong>Charlotte</strong><br>
      <a href="https://github.com/CharlotteC63">
        <img src="https://img.shields.io/badge/GitHub-000?logo=github&logoColor=white" />
      </a><br>
      <a href="https://www.linkedin.com/in/charlotte-campbell-15323a151/">
        <img src="https://img.shields.io/badge/LinkedIn-blue?logo=linkedin&logoColor=white" />
      </a>
    </td>
    <td><strong>Iohane</strong><br>
      <a href="https://github.com/Yoyo-su">
        <img src="https://img.shields.io/badge/GitHub-000?logo=github&logoColor=white" />
      </a><br>
      <a href="https://www.linkedin.com/in/iohane-annan-07b722a0/">
        <img src="https://img.shields.io/badge/LinkedIn-blue?logo=linkedin&logoColor=white" />
      </a>
    </td>
  </tr>
  <tr>
    <td><strong>Freddie</strong><br>
      <a href="https://github.com/FreddieMoller">
        <img src="https://img.shields.io/badge/GitHub-000?logo=github&logoColor=white" />
      </a><br>
      <a href="https://www.linkedin.com/in/frederick-moller-63a348202/">
        <img src="https://img.shields.io/badge/LinkedIn-blue?logo=linkedin&logoColor=white" />
      </a>
    </td>
    <td><strong>Alyona</strong><br>
      <a href="https://github.com/DDataAly">
        <img src="https://img.shields.io/badge/GitHub-000?logo=github&logoColor=white" />
      </a><br>
      <a href="https://www.linkedin.com/in/alyona-d-410554135/">
        <img src="https://img.shields.io/badge/LinkedIn-blue?logo=linkedin&logoColor=white" />
      </a>
    </td>
  </tr>
</table>

## 🔰 Overview
We were approached by our client, Terrific Totes, a tote bag retailer, to create a data pipeline to Extract, Transform, and Load data on sales from their company’s operational OLTP database into an OLAP data warehouse, for ease of analysis.


## 🔧 Tech Stack
- **Python 3.13** - Primary programming language
  - **Pytest** - Test Driven Development (TDD)
  - **Pandas** - Data transformation
- **Github** - Repository management, CI/CD (Github-Actions), Credentials Security (Github-Secrets)
- **AWS** - S3, Lambda, CloudWatch, Step Functions
- **Terraform** - AWS Deployment (Infrastructure as Code)
- **PostgreSQL** - Relational Database Management
- **Power BI** - Data Visualisation


## 🏛️ Architecture
- Two S3 buckets are used: one for ingested data and one for processed data. Both are well-structured and organised to ensure data is easy to locate and manage.
- A Python application is responsible for continually ingesting all tables from the totesys database and storing the raw data in JSON format. This application:
  - Runs automatically on a scheduled interval
  - Logs progress and activity to AWS CloudWatch
  - Sends email alerts in case of failures
  - Follows security best practices (e.g. protects against SQL injection and secures credentials)
- The same application transforms the ingested data into a predefined schema designed for a data warehouse, and stores the output in Parquet format. It:
  - Runs every 20 minutes to ensure new transactions are reflected in the warehouse within 30 minutes, meeting the client’s requirements
  - Logs transformation processes and errors
  - Populates dimension and fact tables following a single star schema structure
- The transformed data is then loaded into a prepared data warehouse at regular intervals. This step is also logged and monitored for reliability.
- A visual dashboard is included, allowing users to explore and gain insights from the data stored in the warehouse.

All Python code is unit and integration tested, PEP8 compliant, and tested for security vulnerabilities with the `pip-audit` and `bandit` packages. Test coverage exceeds 90%.

The project is deployed automatically using infrastucture-as-code (Terraform) and CI/CD (Github-Actions).

## 📊 The Data

The primary data source for the project is a database called `totesys`, our client's back-end operational OLTP database. Data is inserted and updated into this database several times a day.

The data is remodelled into three overlapping star schemas. You can find the ERDs for these star schemas:

- ["Sales" schema](https://dbdiagram.io/d/637a423fc9abfc611173f637)
- ["Purchases" schema](https://dbdiagram.io/d/637b3e8bc9abfc61117419ee)
- ["Payments" schema](https://dbdiagram.io/d/637b41a5c9abfc6111741ae8)

The overall structure of the resulting data warehouse is shown [here](https://dbdiagram.io/d/63a19c5399cb1f3b55a27eca).

## 👀 Visualisation

We have created a Power BI dashboard to visualise some useful data insights:

<table>
  <tr>
    <td>
      <img src="https://github.com/user-attachments/assets/e1724265-ad54-47fa-a1e9-e3760c86e32c" width="100%" />
    </td>
    <td>
      <img src="https://github.com/user-attachments/assets/c1d81c2f-dc07-4fcb-aa67-76c640bbb42c" width="100%" />
    </td>
  </tr>
  <tr>
    <td>
      <img src="https://github.com/user-attachments/assets/93d74ae8-80cd-4291-8c1a-fea496fd16c0" width="100%" />
    </td>
    <td>
      <img src="https://github.com/user-attachments/assets/25a69137-8f7b-49b3-ae47-5f9d17ea055b" width="100%" />
    </td>
  </tr>
  <tr>
    <td>
      <img src="https://github.com/user-attachments/assets/945dd70f-93ea-464d-851d-d6cb8331623f" width="100%" />
    </td>
    <td>
      <img src="https://github.com/user-attachments/assets/3981d214-4d0e-472c-8884-2be1f2616406" width="100%" />
    </td>
  </tr>
</table>



## 📁 File Structure

```
FSCIFA-project
├── .github
│   └── workflows
│       └── ci.yml          # CI/CD Automated deployment via Github Actions
├── dependencies_db/        # Python dependencies for db connection
├── src                     # Source code for ETL/ELT
│   ├── python
│   │   ├── db              # Python database connection functions
│   │   └── utils           # Python utility functions
│   ├── extract_lambda.py   # ETL - Extract lambda function
│   ├── load_lambda.py      # ETL - Load lambda function
│   └── transform_lambda.py # ETL - Transform lambda function
├── tests/                  # Unit and integration tests
│   ├── data/               # Sample data or data schemas for tests
│   └── test*.py            # Unit and integration tests for python functions (pytest)
├── terraform/              # AWS Deployment
├── .gitignore              # Files not to be pushed to remote repository
├── Makefile                # Automated environment setup & configuration
├── mvp.png                 # Illustration of expected minimum viable product
├── README.md               # Project overview
├── requirements_db.txt     # Third party Python modules for db connection
└── requirements.txt        # Third party Python modules
```

## 🚀 Setup & Deployment

This project uses GitHub Actions for continuous integration and deployment, the workflow automatically runs tests and deploys AWS infrastructure via Terraform.

The CI/CD pipeline is triggered on:
  - Pushes to the main branch
  - Pull requests targeting the main branch


### Continuous Integration  
The run-tests job performs the following steps:

 - Configures the Python environment and installs dependancies
 - Runs python security, format and linting checks
 - Runs pytests and checks test coverage

### Terraform Deployment
The deploy-terraform job runs only after successful tests and performs the following:

- Installs Terraform
- Runs Terraform Init, Plan & Apply


### Required Secrets for AWS and Terraform deployment:

AWS credentials secrets:
 - DEPLOY_USER_AWS_ACCESS_KEY_ID
 - DEPLOY_USER_AWS_SECRET_ACCESS_KEY

Terraform variable secrets:
  - TF_VAR_pg_host
  - TF_VAR_pg_port
  - TF_VAR_pg_user
  - TF_VAR_pg_database
  - TF_VAR_pg_password
  - TF_VAR_dw_host
  - TF_VAR_dw_database
  - TF_VAR_dw_password



##
*A Northcoders Data Engineering Bootcamp Project*
