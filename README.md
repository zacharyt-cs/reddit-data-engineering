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
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About The Project

[![Dashboard][dashboard_screenshot]](https://datastudio.google.com/s/mjIjKwWNUQU)

Interested to explore Reddit data for trends or analytics?

This project builds a data pipeline (from data ingestion to visualisation) that stores and preprocess data over any time period you want.

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

<!-- GETTING STARTED -->
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
1. Go to [Google Cloud](https://console.cloud.google.com/) and create a new project.
2. Go to IAM and [create a Service Account](https://cloud.google.com/docs/authentication/getting-started#creating_a_service_account) with these roles:
  * BigQuery Admin
  * Storage Admin
  * Storage Object Admin
  * Viewer
3. Download the Service Account credentials, rename it to `google_credentials.json` and store it in `$HOME/.google/credentials/`.
4. On the Google console, enable the following APIs:
  * IAM API
  * IAM Service Account Credentials API
  * Cloud Dataproc API

### Use Terraform to set up the infrastructure on Google Cloud
I recommend executing the following on VSCode.

1. Using VSCode + WSL, open the project folder `de_r-stocks`. 
2. Open `variables.tf` and modify:
    
    `variable "project"` to your own project id (I think may not be necessary)
    
    `variable "region"` to your project region
    
    `variable "credentials"` to your credentials path 
3. Open the VSCode terminal and change directory to the terraform folder, e.g. `cd terraform`.
4. Initialise Terraform: `terraform init`
5. Plan the infrastructure: `terraform plan`
6. Apply the changes: `terraform apply`

If everything goes right, you now have a bucket on Google Cloud Storage called 'datalake_<project-id>' and a dataset on BigQuery called 'stocks_data'.

<p align="right">(<a href="#top">back to top</a>)</p>


<!-- USAGE EXAMPLES -->
## Usage

Use this space to show useful examples of how a project can be used. Additional screenshots, code examples and demos work well in this space. You may also link to more resources.

_For more examples, please refer to the [Documentation](https://example.com)_

<p align="right">(<a href="#top">back to top</a>)</p>


## Help

<!-- ROADMAP -->
## Roadmap

- [x] Add Changelog
- [x] Add back to top links
- [ ] Add Additional Templates w/ Examples
- [ ] Add "components" document to easily copy & paste sections of the readme
- [ ] Multi-language Support
    - [ ] Chinese
    - [ ] Spanish

See the [open issues](https://github.com/othneildrew/Best-README-Template/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Your Name - [@your_twitter](https://twitter.com/your_username) - email@example.com

Project Link: [https://github.com/your_username/repo_name](https://github.com/your_username/repo_name)

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

Use this space to list resources you find helpful and would like to give credit to. I've included a few of my favorites to kick things off!

* [Choose an Open Source License](https://choosealicense.com)
* [GitHub Emoji Cheat Sheet](https://www.webpagefx.com/tools/emoji-cheat-sheet)
* [Malven's Flexbox Cheatsheet](https://flexbox.malven.co/)
* [Malven's Grid Cheatsheet](https://grid.malven.co/)
* [Img Shields](https://shields.io)
* [GitHub Pages](https://pages.github.com)
* [Font Awesome](https://fontawesome.com)
* [React Icons](https://react-icons.github.io/react-icons/search)

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[dashboard_screenshot]: images/dashboard.png
[architecture_diagram]: images/architecture.png
