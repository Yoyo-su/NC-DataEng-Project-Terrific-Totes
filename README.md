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
We have been approached by Terrific Totes to create a data-pipeline to Extract, Transform, and Load data from a prepared source into a data lake and warehouse hosted in AWS.

We will deliver a data platform that extracts data from an operational OLTP database, archives it in a data lake, and makes it available in a remodelled OLAP data warehouse.

## 🔧 Tech Stack
- Github - Repository management, CI/CD (Github-Actions), Credentials Security (Github-Secrets)
- AWS - RDS, Lambda, CloudWatch, S3
- Terraform - AWS Deployment (Infrastructure as Code)
- Python 3.12 - Primary programming language
- Pytest - Test Driven Development (TDD)
- PostgreSQL - Relational Database Management


## 🏛️ Architecture
- Two S3 buckets (one for ingested data and one for processed data). Both buckets are structured and well-organised so that data is easy to find.
- The Python application continually ingests all tables from the `totesys` database and stores the injested data in a json format. The application also:
  - operates automatically on a schedule
  - logs progress to Cloudwatch
  - triggers email alerts in the event of failures
  - follows good security practices (for example, preventing SQL injection and maintaining password security)
- The Python application remodels the data into a predefined schema suitable for a data warehouse and stores the data in Parquet format. The application also:
  - triggers automatically when it detects the completion of an ingested data job
  - adequately logs and monitors
  - populates the dimension and fact tables of a single "star" schema in the warehouse.
- The Python application loads the data into a prepared data warehouse at intervals. This is also logged and monitored.
- Includes a visual presentation that allows users to view useful data in the warehouse.

All Python code is thoroughly tested, PEP8 compliant, and tested for security vulnerabilities with the `pip-audit` and `bandit` packages. Test coverage exceeds 90%.

The project is deployed automatically using infrastucture-as-code (Terraform) and CI/CD (Github-Actions).

Changes to the source database will be reflected in the data warehouse within 30 minutes.

## 📊 The Data

The primary data source for the project is a database called `totesys` which is meant to simulate the back-end data of a commercial application. Data is inserted and updated into this database several times a day.


The data is remodelled into three overlapping star schemas. You can find the ERDs for these star schemas:

- ["Sales" schema](https://dbdiagram.io/d/637a423fc9abfc611173f637)
- ["Purchases" schema](https://dbdiagram.io/d/637b3e8bc9abfc61117419ee)
- ["Payments" schema](https://dbdiagram.io/d/637b41a5c9abfc6111741ae8)

The overall structure of the resulting data warehouse is shown [here](https://dbdiagram.io/d/63a19c5399cb1f3b55a27eca).

## 👀 Visualisation

We have created a BI dashboard to visualise some useful data insights:

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
