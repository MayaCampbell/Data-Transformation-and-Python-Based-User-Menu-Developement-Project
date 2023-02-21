# Perscholas_Capstone_Project
# Overview
The purpose of this data engineering project is to provide the opportuniy to demonstrate and combine the knowledge ans skills I have aquired throughout this course.  
In this project I will be working with multiple technologies to manage an ETL (Extraction, Transformation, Loading) process for a Loan Application dataset and a Credit Card dataset that was provided.
The following technologies that were used in this project were: Python(Pandas, advanced modules, Matplotlib), MariaDB, Apache Spark(Spark Core, Spark SQL), and Python Visualization and Analystics libraries.  

# Credit Card Dataset Overview
The Credit Card System database is a sysyem developed for managing activities such as registering new customers and approving r canceling requests.  Below are the three files that contain the customer's transaction information and inventories in the credit card information that was used in this project:
- CDW_SAPP_CUSTOMER.JSON: This file contains exsisting customer details
- CDW_SAPP_CREDITCARD.JSON: This file contains information about credit card transactions
- CDW_SAPP_BRANCH.JSON: This file contains the information of each branch.
- 
# PREPERATION
Before beginning the ETL process the first thing I did was create a virtual environment in my python terminal.  Once my virtual environment was created I activated it, then created a requirements.txt file that contains all the python libraries that I used in this project.  I also created a gitignore file.

# EXTRACT, TRANSFROM, LOAD
## STEP 1: Extract
I began by imported findspark, pyspark, and pandas so I can read/extract the above JSON files. Next I created a spark session, this can be used to create dataframe, register dataframe as tables, and execute SQL over tables. I used spark to read json files into spark dataframe.

## STEP 2: Transform
Since I wanted to display my knowledge of Pandas and Apache Spark.  I converted CDW_SAPP_CUSTOMER.JSON and CDW_SAPP_CREDITCARD.JSON into a pandas dataframe to then transform this dataset according to the specifications found in the mapping documnet that was provided to me. For CDW_SAPP_BRANCH.JSON I used spark to transformed the dataset according the mapping document.

## STEP 3: LOAD
Once json files were read/extracted into dataframe and transform I then loaded data into MariaDB which is a RDBMS. First I created a database in SQL(MariaDB) named "creditcard_capstone".  Then used pyspark to load/write the "Credit Card System Data" into RDBMS database.

# Functional Requirements - Application Front-End
Goal of this section is to create a console-based Python program to Satisfy the System Requirements

## Transaction Details Module
1- Function 1: Based on user's input of zip code, month, and year it must return transactions made by customers living in given zip code; ordered by day in descending order.

