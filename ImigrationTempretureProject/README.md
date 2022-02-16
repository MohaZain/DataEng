# Immigration Project
### Data Engineering Capstone Project

#### Project Summary

* Data is money and in a day with thousand and thousand of data to process, there is some tool that will suit perfectly with a large amount of data such as [spark](https://spark.apache.org/) and [hadoop](https://hadoop.apache.org/) in this project i will be using Spark to transform [I94 Immigration Data](https://www.trade.gov/national-travel-and-tourism-office) and [World Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data) to [star schema](https://searchdatamanagement.techtarget.com/definition/star-schema) and perform the nasscary step To be ready to query multiple analytics questions.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 
* Dataset scope will be in two different datasets with more than one million records, and then create fact and dimension table to be able to analyze the data and find insigit by using spark to help process a large amount of data.
#### Describe and Gather Data 
* `I94 Immigration Data`: This data comes from the US National Tourism and Trade Office This is where the data comes from [here](https://travel.trade.gov/research/reports/i94/historical/2016.html).
* `World Temperature Data`: This dataset came from [Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data) contain monthly average temperature for different cities


### Step 2: Explore and Assess the Data
#### Explore the Data 
* Exploring the data set using pyspark to find any duplicated or null recored to clean them
#### Cleaning Steps

* Remove any duplicated data.
* Change the column name to clearer names.
* Transform SAS date to datetime.

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
* Star schema are work optimally for data warehouses, BI use and OLAP in that help our goal. 
#### 3.2 Mapping Out Data Pipelines
* 1. Extract the data from the provided sources above.
* 2. Clean and preprocess the data.
    * Remove any duplicated data.
    * Change the column name to clearer names.
    * Transform SAS date to datetime.
    * remove null in temperture if both AverageTemperature and AverageTemperatureUncertainty Null.
* 3. Transform imm_df to fact table and dimensional.
* 4. Transform temp_df to dimensional table.
### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Data processing and creating model was done using Spark

#### 4.2 Data Quality Checks
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Source/Count checks to ensure completeness
 * Checks for duplicated record

#### 4.3 Data dictionary 
## Immigration data

#### Fact Immigration (fact_imm)
     |-- id: id
     |-- year: year of the arrival
     |-- arrival_date: arrival date
     |-- departure_date: departure date
     |-- mode: mean of travil by (air, sea, land, or not reported)
     |-- fltno: flight number
     
#### Dim Immigrant (dim_immigrant)
     Contain immgrant personal information
     |-- id: id
     |-- citizen: the country of citizenship. in code
     |-- residence: the country of residence. in code
     |-- gender: person gender
     |-- year_of_birth: year of birth
     
#### Dim Country (dim_country)
     Contain code of the country
     |-- code: code represent spesifc country
     |-- country: country name
     
#### Dim Airline (dim_airline)
     |-- fltno: flight number 
     |-- airline: airline name
     
#### Dim Temperture (dim_temp)
     |-- dt: date 
     |-- AverageTemperature: Average Temperature for month
     |-- AverageTemperatureUncertainty: Average Temperature Uncertainty
     |-- City: city name 
     |-- Country: Country name
     |-- Latitude: Latitude
     |-- Longitude: Longitude
     
![Digram Image](https://github.com/MohaZain/DataEng/blob/main/ImigrationTempretureProject/erdigram1.png?raw=true)

### Step 5: Complete Project Write Up
##### Clearly state the rationale for the choice of tools and technologies for the project.
  * Spark has proven to work great with a large amounts of data, also with the simplicity of use.
    
##### Propose how often the data should be updated and why.
  * Could update monthly, Since the Immigration data get updated monthly.

### Write a description of how you would approach the problem differently under the following scenarios:

##### The data was increased by 100x.
  * With increased of data we can relay on Amazon services such as [EMR](https://aws.amazon.com/emr/).
  
##### The data populates a dashboard that must be updated on a daily basis by 7am every day.
  * Using scheduling tool such as airflow will help autmated and scheduling the data pipeline.
  
##### The database needed to be accessed by 100+ people.
  * Consider Amazon service to handle handred or more request simultaneously such as [redshift](https://aws.amazon.com/redshift/) , [Amazon Aurora](https://aws.amazon.com/rds/aurora/?aurora-whats-new.sort-by=item.additionalFields.postDateTime&aurora-whats-new.sort-order=desc) 
 
 
