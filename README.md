# Scraping-online--website-and--moving-data-to-datawarehouse

The primary objective of this project is to collect data from an ecommerce website using web scraping techniques. Once the data has been extracted, it will be stored in a secure SFTP server to ensure confidentiality and data integrity. After the data has been securely stored, it will be transferred to a postgres data warehouse, which will provide a central repository for the data and enable further analysis and reporting. The end-to-end process, which includes scraping, storing, and transferring the data, will be automated to ensure efficiency and accuracy. By implementing this project, we will be able to gain valuable insights into customer behavior and market trends, which will enable us to make data-driven business decisions


## requirements

* python 3.*
* Selenium
* Pandas 
* docker
* airflow


## Project Steps:

### Scraping data:

In this step we scraped the website using selenium webdriver and store the data in a csv file .

### Storing the file in SFTP server:

* Creating an SFTP server on ubuntu  https://linuxhint.com/setup-sftp-server-ubuntu/ 
* using pysftp and cryptography library to establish connection

### Creating a Postgres DataWarehouse:

* Opt for a star schema
![Capture](https://user-images.githubusercontent.com/89319105/228092117-46185fed-9d2e-4e5a-8573-5958e976ac09.PNG)
* using a postgres container
`docker run --name my-postgres -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres`
`docker exec -it my-postgres psql  -U postgres`
