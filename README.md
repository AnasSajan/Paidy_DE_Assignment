

# Introduction


The assignment is built as part of the interivew process at Paidy.
This is a PoC for a data ingestion system to make incoming CSV data easy to use / query by the Data Scientists. 

It prepares data for the data scientists to help them understand and build data structures that simplify their work.

# Getting Started


    
## Pre-Installation 

- Python 3.x (3.10.4 used for building the project)
- Docker
- Cron
- Any data management tool( like DBeaver to look at the data)


## Installation
Clone the project

```bash
  git clone https://github.com/AnasSajan/Paidy_DE_Assignment/tree/master
```

Go to the root directory of the project and Install dependencies

```bash
  pip install -r requirements.txt
  ```
## Setting up source file

Go to the source folder from the root directory to get the source file ready

```bash
  cd app/data/source
```

Please rename the postfix of the file to the present date in the format MMDDYYYY.
The script would pick up the file based on the date

Example :  srcdata_06142022 -> srcdata_**06152022**(today's date)

**Important**: The script zips the source file, moves it to archives and removes it from the folder after it runs successfully. Please copy the source file and store it locally before runnning the job in order to not go to the archives to fetch it if you want to run it the second time. 

## Data Ingestion 

Start PostgreSQL and run sql script using:

```bash
  docker compose up --build
```

Login to the PostgreSQL server using::
```
  host: localhost
  dbname: postgres
  user: postgres
  password: postgres
  port: 5463
```

Navigate to the app folder in the directory :

```bash
  cd app
```

Run the script:

```bash
  python preprocessing.py
```




## Scheduling the Pipeline

Please follow the below steps in order to ingest the received files to the database on a regular basis :

* Open up a Terminal window and execute ``` pwd ``` and ```which python3``` commands to get the absolute paths to your script folder and Python

* Once you have these, enter the ```crontab -e ```command to edit a cron file

* Click on the``` I``` key on your keyboard to enter insertion mode. We will have to specify the scheduling pattern, full path to the Python executable, and full path to the script to make scheduling work

* Plese use the following as reference

  ```*/2 * * * * /usr/bin/python3 /users/anassajan/desktop/paidy_de_assignment/app/preprocessing.py```

*For the sake of this assignment, we could make it run every two minutes so that we dont have to wait for a long time for the results.*

* Once done, exit the insert mode and save the crontab file - it will schedule the job

*In an actual production environment, the job would be hosted on a cloud serice platform like AWS or Azure instead of running it locally*
# Assumptions

## Job scheduling : 
- **Event-Driven Scheduling (Scenario 1)** :  If the source file is not generated and available at a fixed time due to business requirements or external dependencies, we could trigger the job to run based on arrival of the file. The job will be triggered as soon as it finds the file in the location

- **SLA- based Scheduling (Scenario 2)** : The job can be scheduled based on the time downstream is expecting the data. We will send automatic alerts and warnings to upstream if the file does not arrive by  SLA deadline 

- **Dependency Scheduling (Scenario 3)** : If the file generation is dependent on a different job (Job A), then the completion time of Job A should trigger our job (Job B). Job B would only run upon successful completion of Job A

   **Error handling**:  
   A notification or an alert would be sent to the production team in all the scenario's if our job fails

## Housekeeping Strategy:
Below is the representation of the directory structure for data storage
```
└───data
    ├───archive 
    ├───failed
    ├───preload
    ├───source
    └───target
```
The script picks up the file from the source folder with the format ``` srcdata_MMDDYYY.csv``` and loads the transformed data in the preload folder before loading it into the table.
- **Success Scenario** : When the job is successful, the data is loaded into the table and the transformed file is generated in the target folder with the format ``` tgtdata_MMDDYYY.csv```. The source file is zipped and moved to archive folder to keep a copy. The existing source file and preload file are then removed
- **Failed Scenario** : When the job fails to load the data in the table, it generates the transformed file in failed folder and removes the preload file. No file is generated in target folder and the source file is not zipped or removed
**Note**: Each folder generates a month-wise subfolder that contains all the files created in that month




# Decisions

## Transformations and column additions: 
- **Dropping unnamed column** :  The CSV file seems to generate an index column. It cannot be used as an index in the table as the file coming on the next day may have similar values. We would drop this column to avoid data violations
- **Primary key column addition** : An auto-increamenting field is added in the first column of the table (APP_ID). This would be different for each record  and is used as a primary key
- **Handling NA values** : The NA values in the file will be treated as null and populated in the table as null values 

- **Rounding decimal places** : Any value having more than 6 decimal places (5.1234567) will be kept only till the 6th decimal place and rest would be truncated (5.123456). The decision is made based on the data avalilable and assuming the business requirement

- **Adding new metadata column** : The field would ingest the timestamp (mm-dd-yyyy hh:mm:ss) on the day the job ran and the record was inserted into the table. I took a stab at calling it 'CREATED_TS'
- **Rename existing columns**: Renamed the source columns in the target table to a standard format
- **Calculate times late in 2 yrs** : When the sum of the two integer columns 'NumberOfTime30-59DaysPastDueNotWorse' and 'NumberOfTime60-89DaysPastDueNotWorse' is greater than 24, then the values for these fields in the record should be inserted as null in the table. The conclusion is derived based on data profiing results and assumptions.










## Shortcuts