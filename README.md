<div id="top"></div>

<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#create-a-google-cloud-project">Create a Google Cloud Project</a></li>
        <li><a href="#set-up-the-infrastructure-on-google-cloud-with-terraform">Set up the infrastructure on Google Cloud with Terraform</a></li>
        <li><a href="#set-up-airflow">Set up Airflow</a></li>
      </ul>
    </li>
    <li>
      <a href="#usage">Usage</a>
      <ul>
        <li><a href="#start-airflow">Start Airflow</a></li>
        <li><a href="#prepare-for-spark-jobs-on-dataproc">Prepare for Spark jobs on Dataproc</a></li>
      </ul>
    </li>
    <li><a href="#help">Help</a></li>
    <li><a href="#roadmap-for-future-development">Roadmap for Future Development</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About The Project

[![Dashboard][dashboard_screenshot]](https://datastudio.google.com/s/mjIjKwWNUQU)

Interested to explore Reddit data for trends, analytics, or just for the fun of it?

This project builds a data pipeline (from data ingestion to visualisation) that stores and preprocess data over any time period that you want.

<p align="right">(<a href="#top">back to top</a>)</p>

### Built With

* Data Ingestion: [Pushshift API](https://github.com/pushshift/api)
* Infrastructure as Code: [Terraform](https://www.terraform.io/)
* Workflow Orchestration: [Airflow](https://airflow.apache.org)
* Data Lake: [Google Cloud Storage](https://cloud.google.com/storage)
* Data Warehouse: [Google BigQuery](https://cloud.google.com/bigquery)
* Batch Processing: [Spark](https://spark.apache.org/) on [Dataproc](https://cloud.google.com/dataproc)
* Visualisation: [Google Data Studio](https://datastudio.google.com/)

![architecture][architecture_diagram]
Cloud infrastructure is set up with Terraform.

Airflow is run on a local docker container.
It orchestrates the following on a weekly schedule:
* Download data (JSON)
* Parquetize the data and store it in a bucket on Google Cloud Storage
* Write data to a table on BigQuery
* Create cluster on Dataproc and submit PySpark job to preprocess parquet files from Google Cloud Storage
* Write preprocessed data to a table on BigQuery

<p align="right">(<a href="#top">back to top</a>)</p>

## Getting Started

I created this project in WSL 2 (Windows Subsystem for Linux) on Windows 10.

### Prerequisites

To get a local copy up and running in the same environment, you'll need to:
* Install Python (3.8 and above)
* Install VSCode
* [Install WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install) if you haven't
* [Install Terraform](https://www.terraform.io/downloads) for Linux
* [Install Docker Desktop](https://docs.docker.com/desktop/windows/install/)
* [Install Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk#deb) for Ubuntu
* Have a Google Cloud Platform account
* Clone this repository locally

### Create a Google Cloud Project
1. Go to [Google Cloud](https://console.cloud.google.com/) and create a new project. I set the id to 'de-r-stocks'.
2. Go to IAM and [create a Service Account](https://cloud.google.com/docs/authentication/getting-started#creating_a_service_account) with these roles:
    * BigQuery Admin
    * Storage Admin
    * Storage Object Admin
    * Viewer
3. Download the Service Account credentials, rename it to `de-r-stocks.json` and store it in `$HOME/.google/credentials/`.
4. On the Google console, enable the following APIs:
    * IAM API
    * IAM Service Account Credentials API
    * Cloud Dataproc API
    * Compute Engine API

### Set up the infrastructure on Google Cloud with Terraform
I recommend executing the following on VSCode.

1. Using VSCode + WSL, open the project folder `de_r-stocks`. 
2. Open `variables.tf` and modify:
    
    * `variable "project"` to your own project id (I think may not be necessary)
    * `variable "region"` to your project region
    * `variable "credentials"` to your credentials path

3. Open the VSCode terminal and change directory to the terraform folder, e.g. `cd terraform`.
4. Initialise Terraform: `terraform init`
5. Plan the infrastructure: `terraform plan`
6. Apply the changes: `terraform apply`

If everything goes right, you now have a bucket on Google Cloud Storage called 'datalake_de-r-stocks' and a dataset on BigQuery called 'stocks_data'.

### Set up Airflow
1. Using VSCode, open `docker-compose.yaml` and look for the `#self-defined` block. Modify the variables to match your setup.
2. Open `stocks_dag.py`. You may need to change the following:

    * `zone` in `CLUSTER_GENERATOR_CONFIG`
    * Parameters in `default_args`

<p align="right">(<a href="#top">back to top</a>)</p>

## Usage

### Start Airflow
1. Using the terminal, change the directory to the airflow folder, e.g. `cd airflow`.
2. Build the custom Airflow docker image: `docker-compose build`
3. Initialise the Airflow configs: `docker-compose up airflow-init`
4. Run Airflow: `docker-compose up`

If the setup was done correctly, you will be able to access the Airflow interface by going to `localhost:8080` on your browser.

Username and password are both `airflow`.

### Prepare for Spark jobs on Dataproc
1. Go to `wordcount_by_date.py` and modify the string value of `BUCKET` to your bucket's id.
2. Store initialisation and PySpark scripts on your bucket. It is required to create the cluster to run our Spark job.
    
    Run in the terminal (using the correct bucket name and region):
    * `gsutil cp gs://goog-dataproc-initialization-actions-asia-southeast1/python/pip-install.sh gs://datalake_de-r-stocks/scripts`
    * `gsutil cp spark/wordcount_by_date.py gs://datalake_de-r-stocks/scripts`

<p align="right">(<a href="#top">back to top</a>)</p>

Now, you are ready to enable the DAG on Airflow and let it do its magic!

![airflow][airflow_screenshot]

When you are done, just stop the airflow services by going to the `airflow` directory with terminal and execute `docker-compose down`.

## Help

Authorisation error while trying to create a Dataproc cluster from Airflow
  1. Go to Google Cloud Platform's IAM
  2. Under the Compute Engine default service account, add the roles 'Editor' and 'Dataproc Worker'.

## Roadmap for Future Development

- [ ] Refactor code for convenient change to `subreddit` and `mode`.
- [ ] Use Terraform to set up tables on BigQuery instead of creating tables as part of the DAG.

<p align="right">(<a href="#top">back to top</a>)</p>

## Contributing

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#top">back to top</a>)</p>

## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

## Contact

[Connect with me on LinkedIn!](https://www.linkedin.com/in/zacharytancs/)

## Acknowledgments

Use this space to list resources you find helpful and would like to give credit to. I've included a few of my favorites to kick things off!

* [Data Engineering Zoomcamp by DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp)
* [Best-README-Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[dashboard_screenshot]: images/dashboard.png
[architecture_diagram]: images/architecture.png
[airflow_screenshot]: images/airflow.png
