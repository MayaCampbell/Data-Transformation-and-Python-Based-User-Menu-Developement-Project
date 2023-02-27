# Perscholas_Capstone_Project
# Overview
The purpose of this data engineering project is to provide the opportuniy to demonstrate and combine the knowledge ans skills I have aquired throughout this course.  
In this project I will be working with multiple technologies to manage an ETL (Extraction, Transformation, Loading) process for a Loan Application dataset and a Credit Card dataset that was provided.
The following technologies that were used in this project were: Python(Pandas, advanced modules, Matplotlib), MariaDB, Apache Spark(Spark Core, Spark SQL), and Python Visualization and Analystics libraries.  

# Credit Card Dataset Overview
The Credit Card System database is a system developed for managing activities such as registering new customers and approving r canceling requests.  Below are the three files that contain the customer's transaction information and inventories in the credit card information that was used in this project:
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
1- Function 1: The purpose of this function is to return all the transactions of a customer ordered by month of the transactions in descending order based on the given zip code, month, and year that was entered by the user.  I joing CLEAN_CDW_SAPP_CUSTOMER.JSON and CLEAN_SAPP_CREDIT_CARD.JSON on 'credit_card_no' to get transactions of each customer. To begin imported json into my python script and read/loaded the file. I then created 3 input statements and assigned them to variables.  I then iterated through the file and specfified if the zip code, month, and year matched what the user has given then append the transaction to an empty list.  The list is then sorted by month in descending order.  If the length of list is == 0 then it will print 'No transactions found', else print the sorted list.

2- Function 2: The purpose of this function is to display the number of transactions and its total value based on transaction type.  I started of by following the same steps I did in the previous function. In order to get a sum of all transactions values I first extracted all the values with key 'TRANSACTION_VALUE' and appened it to a list.  Then I used reduce lambda to sum up all the values in that list.  In order to get the count of all the transaction for a given transaction type, I appended all transactions to list with given type and used len() to get the count of all transactions in the list.

3- Function 3: The purpose of this function is to display the number of transactions and its total value for each branch based on state.  Similar to the previous functions my beginning process it the same.  I joined the cleaned CDW_SAPP_CREDIT_CARD.JSON & CDW_SAPP_BRANCH.JSON on 'branch_code' to get transactions for each branch.  Then repeated the steps I did for function 2.

## Customer Details
1- Functions 1: This functions displays customer account details for given customer name.  To begin I read/load cleaned CDW_SAPP_CUSTOMER.JSON into python.  I then assign input statements to variables.  I created a for loop iterating through file and if first name and last name matched what user has given I created multiple print statement printing out their account details, and if name does not match it will print 'customer not found'.  I realized I had to iterate through multiple names before the name that was given i would get multiple 'customer not found' print statements then I would get customer's account details of name I was looking for.  To fix this I added a break after my if statement to prevent this from happening.

2- Function 2: This function modify's customer details of an existing customers.  A challenge I came accross while writing this is after read/load json file and doing the modification in order for it to save I needed to write to that same json file and then json.dump the modifications I made. This will allow me to see the change in the file.  In order to see the change in MYSQL Database I then read file that I modified into a spark dataframe and used the .option() method to ignore corrupt records and keep the schema the same and saved it to a variable.  I then cached the dataframe and created a mysql connection to write the modification to the database using overwrite instead of append.

3- Function 3: This function generates a monthly bill for given credit card number.  For this function I repeated what I did for Function 2 to get the sum of all transaction values.

4- Function 4: Displays all transactions between a certain date range.  I used a for loop to iterate through transactions and appen transactions to an empty list that match the customer's name and transaction date less than or equal to the start date and end date greater than or equal to transaction date.  In order to sort transaction in list by month in descending order sorted function and lambda to sort 'timeid' [:6] index.


# Data Analysis and Visualization
## Graph 1: Find and plot which transaction type has a high rate of transactions.
![image](https://user-images.githubusercontent.com/112674624/221659579-1b3a09fe-eadf-44a6-be3f-80363f746d87.png)

## Graph 2: Find and plot which state has a high number of customers.
![image](https://user-images.githubusercontent.com/112674624/221659688-540dd5ca-877d-4f98-8e84-0038f9503830.png)

## Graph3: Find and plot the sum of all transactions for each customer, and which customer has the highest transaction amount.
![image](https://user-images.githubusercontent.com/112674624/221658019-c467a14a-07d9-4ead-a8c0-bb9ba29859ba.png)

# Loan Application API
## Step 1:
In order to access data from API endpoint, first imported the python module requests thenI assigned url as a string to a variable and passed variable through requests.get() and read data using .json().  I then used .status_code to get the status code of API endpoint.

## Step 2:
I then created a Spark datafram using spark.createDataFrame and passed data from above API endpoint through it.

## Step 3:
Lastly I loaded spark dataframe to MYSQL by creating a MYSQL connection and appending to database.

# Data Analysis and Visualization for Loan Application
## Graph 1: Find and plot the percentage of applications approved for self-employed applicants.
![image](https://user-images.githubusercontent.com/112674624/221660169-add1674a-405e-4ff7-a8dd-f9d1d81c60ff.png)

## Graph 2: Find the percentage of rejection for married male applicants.
![image](https://user-images.githubusercontent.com/112674624/221660235-586b23f1-77f9-4da5-bb78-aeb72e7f2620.png)

## Graph 3: Find and plot the top three months with the largest transaction data.
![image](https://user-images.githubusercontent.com/112674624/221660305-80719a38-146c-452f-a777-ad922b799507.png)

## Graph 4: Find and plot which branch processed the highest total dollar value of healthcare transactions.
![image](https://user-images.githubusercontent.com/112674624/221658727-aa488d73-8786-43d5-b96d-05fc0a075d13.png)

# Creating Front End User Menu
First I imported pyinputplus and used nested while loops and if, elif, annd else statements for users to be able to maneuver through menu options.

