import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import pyinputplus as pyip

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import*

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

#Create Spark Session
spark = SparkSession.builder.master("local[*]").appName("Test SQL app").getOrCreate()

credit_card= open(r'C:\Users\Learner_XZHCG221\Perscholas_Capstone_Project\credit_card.ipynb', 'r')

import os
username= os.environ.get('USER')
my_password=os.environ.get('PASSWORD')

#Transaction Module
def display_transactions_given_zip_month_year():
    import json
    new_credit=open('cleaned_cdw_sapp_credit_customer.json') #open json
    transactions= json.load(new_credit) 

    zip_code = int(input("Enter zip code: "))
    month = input("Enter month (01-12): ")
    year = input("Enter year (YYYY): ")
    transactions_by_zip_month_year = []
    for transaction in transactions:
        if transaction['CUST_ZIP']== zip_code and transaction['TIMEID'][4:6] == month and transaction['TIMEID'][:4] == year:
            transactions_by_zip_month_year.append(transaction)
    sorted_transactions = sorted(transactions_by_zip_month_year, key=lambda x: x["TIMEID"][6:8], reverse=True)
    if len(sorted_transactions) == 0:
        print("No transactions found for the given zip code and month/year.")
    else:
        print(sorted_transactions)
#display_transactions_given_zip_month_year()

def display_transaction_num_and_total_value_given_type():
    import json
    from functools import reduce
    credit=open('cleaned_cdw_sapp_credit.json') #open json
    transactions= json.load(credit)

    transaction_type= input('Type Transaction Type:').upper
    transactions_by_type=[]

    for transaction in transactions:
        if transaction['TRANSACTION_TYPE'] == transaction_type:
            transactions_by_type.append(transaction)
    transaction_values= [value.get('TRANSACTION_VALUE', None) for value in transactions_by_type]
    sum_values= reduce(lambda x, y:x+y,transaction_values)
    if len(transactions_by_type) == 0:
       print('No transactions found for given transaction type')
    else:
       print('Number of transactions:', len(transactions_by_type), f'Total transaction value:{sum_values:.2f}')
#display_transaction_num_and_total_value_given_type()

def display_transaction_num_and_total_value_given_state():
    import json
    from functools import reduce
    #read existing json file into a python object
    branch=open('cleaned_cdw_sapp_branch_credit.json') #open json
    transactions= json.load(branch) #load json into list
    state= input("Type State as 2 characters:").upper()
    transactions_by_state= []

    for transaction in transactions:
        if transaction['BRANCH_STATE'] == state:
            transactions_by_state.append(transaction)
    transaction_values= [value.get('TRANSACTION_VALUE', None) for value in transactions_by_state]
    sum_values= reduce(lambda x, y:x+y,transaction_values)
    if len(transactions_by_state) == 0:
       print('No transactions found for given transaction type')
    else:
       print('Number of transactions:', len(transactions_by_state), f'Total transaction value:{sum_values:.2f}')
#display_transaction_num_and_total_value_given_state()

#Customer Details
def check_account_details():
    import json
    #read existing json file into a python object
    with open('clean_cdw_sapp_customer.json') as cust:
        customers=json.load(cust)
        #prompt user to enter customers first and last name
    customer_firstname= input("Enter Customer's First Name:")
    customer_lastname= input("Enter Customer's Last Name:")
    for customer in customers:
        #check if customer exsits
        if customer['FIRST_NAME'] == customer_firstname and customer['LAST_NAME'] == customer_lastname:
            #print customer's details
            print('Customer Name:', customer['FIRST_NAME'], customer['MIDDLE_NAME'] , customer['LAST_NAME'])
            print('Customer Phone Number:', customer['CUST_PHONE'])
            print('Customer Email Address:', customer['CUST_EMAIL'])
            print('Credit Card Number:',customer['CREDIT_CARD_NO'])
            print('Customer Country:', customer['CUST_COUNTRY'])
            print('Customer Address:', customer['FULL_STREET_ADDRESS'], customer['CUST_CITY'], customer['CUST_STATE'], customer['CUST_ZIP'])
            print('Customer Social Security Number:', customer['SSN'])   
            break
    else:
        #print error message if customer name is not found
        print("Customer's Name not found in our records")          
#check_account_details()


def modify_account_details():
    import json
    #read existing json file into a python object
    with open(r'C:\Users\Learner_XZHCG221\Perscholas_Capstone_Project\clean_cdw_sapp_customer.json', 'r') as cust_file:
        customers = json.load(cust_file)
    #prompt user to input customers first and last name    
    customer_firstname= input("Enter Customer's First Name:")
    customer_lastname= input("Enter Customer's Last Name:")
    for customer in customers:
        #filter to user's name that matches input
        if customer['FIRST_NAME'] == customer_firstname and customer['LAST_NAME'] == customer_lastname:
            #prompt user to select which account detail to modify
            print('Select account detail to modify...')
            print('1. Phone Number')
            print('2. Email Address')
            print('3. Credit Card Number')
            print('4. Address')
            print('5. Social Security')
            #modify the selected account detail of the customer
            choice= int(input('Type number of choice'))
            if choice == 1:
                new_phone_number= input("Enter phone number (XXX)XXX-XXXX:")
                customer['CUST_PHONE']= new_phone_number
                print('Phone number updated successfully')
            elif choice == 2:
                new_email= input('Enter email address:')
                customer['CUST_EMAIL']= new_email
                print('Email updated successfully')
            elif choice == 3:
                new_cc= input('Enter cc number:')
                customer['CREDIT_CARD_NO']= new_cc
                print('Credit Card Number updated successfully')
            elif choice == 4:
                print('Select address detail to modify...')
                print('1. Full Street Address')
                print('2. City')
                print('3. State')
                print('4. Zip Code')
                choice2 = int(input('Type choice:'))
                if choice2 == 1:
                    new_street_address = input('Enter full street address:')
                    customer['FULL_STREET_ADDRESS']=new_street_address
                    print('Street Address updated successfully')
                elif choice2 == 2:
                    new_city= input('Enter City:')
                    customer['CUST_CITY']=new_city
                    print('City updated successfully')
                elif choice2 == 3:
                    new_state= input('Enter State:')
                    if len(new_state) == 2:
                        customer['CUST_STATE']=new_state
                        print('State updated successfully')
                    else:
                        print('Please Enter Customers 2 Sharachter State')
                elif choice2 == 4:
                    new_zipcode= int(input('Enter Zip Code:'))
                    customer['CUST_ZIP']=new_zipcode
                    print('Zip Code updated successfully')
            elif choice == 5:
                new_ss= input('Enter Social Security Number (XXXXXXXXX): ')
                if len(new_ss) == 9:
                    customer['SSN']= new_ss
                    print('Social Security Number updated successfully')
                else:
                    print('Please enter Customers 9 digit Social Security Number')
            else:
                print('Invalid choice')
            break
    else:
        print("Customer's name not found in records")  
    with open(r'C:\Users\Learner_XZHCG221\Perscholas_Capstone_Project\clean_cdw_sapp_customer.json', 'w') as cust_file:
        json.dump(customers, cust_file, indent=4)
    #this will ignore corrupt records and keep originial schema
    modifydf = spark.read.option('mode', 'PERMISSIVE').option('columnnameOfcorruptrecord', '_corrupt_record').json('clean_cdw_sapp_customer.json', multiLine=True)
    modifydf.printSchema()
    modifydf.cache()
    

    modifydf.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable", "cdw_sapp_customer") \
        .option("user", username) \
        .option("password", my_password) \
        .save()
    
#modify_account_details()


def generate_monthly_bill():
    import json
    from functools import reduce
    #read existing json file into a python object
    with open('cleaned_cdw_sapp_credit.json') as credit: #open json
        transactions= json.load(credit)
    # get cc number
    cc_num= input('Enter CC number:')
    #get month & year input
    month= input('Enter month 01-12:')
    year= input('Enter year (YYYY):')
    #create empty list
    total_transactions=[]
    #iterate through transactions
    for transaction in transactions:
        #filter transactions for specified cc # and month/year
        if transaction['CREDIT_CARD_NO'] == cc_num and transaction['TIMEID'][4:6] == month and transaction['TIMEID'][:4] == year:
            total_transactions.append(transaction)
    #iterate through list to get only transaction values
    transaction_values= [value.get('TRANSACTION_VALUE', None) for value in total_transactions]
    #sum transaction value
    total= reduce(lambda x, y:x+y,transaction_values)
    print(f'Monthly bill summary for credit card:{cc_num} in {month}/{year}:')
    #print(sum of transaction values rounded to seconded decimal)
    print(f'Total amount spent: ${total:.2f}')      
#generate_monthly_bill()

def display_transaction_between_two_dates():
    import json
    with open('cleaned_cdw_sapp_credit_customer.json') as credit: #open json
        transactions= json.load(credit)

    fname= input("Enter First Name:")
    lname= input("Enter Last Name:")
    start_date_string= input("Enter Start Date (YYYYMMDD):")
    end_date_string= input("Enter End Date (YYYYMMDD)")
    transaction_dates_list=[]
    for transaction in transactions:
        if transaction['FIRST_NAME']== fname and transaction['LAST_NAME']== lname:
            if start_date_string <= transaction['TIMEID']  and end_date_string >= transaction['TIMEID']:
                transaction_dates_list.append(transaction)
    sorted_transactions = sorted(transaction_dates_list, key=lambda x: x["TIMEID"][:6], reverse=True) 
    if len(sorted_transactions) == 0:
        print("No transactions found for the given Customers Name and month/year.")
    else:
       print(sorted_transactions)
#display_transaction_between_two_dates()

#Visualizations

#Find and plot which transaction type has a high rate of transactions.
def highest_transaction_rate():
    #group transactions by transaction type then count the number of transactions per type useing transaction id
    transaction_count= df_credit.groupby('TRANSACTION_TYPE')['TRANSACTION_ID'].count().reset_index()

    #sort transactions if count from greatest to least
    transaction_count= transaction_count.sort_values('TRANSACTION_ID').reset_index()
    #print(transaction_count)

    print(f"Customer with the highest transaction amount: {transaction_count.iloc[6]['TRANSACTION_ID']}")

    #plot graph
    plt.barh(transaction_count['TRANSACTION_TYPE'], transaction_count['TRANSACTION_ID'])
    ax=plt.gca()#get axis
    ax.set_frame_on(False)#turn off spines
    plt.tick_params(bottom = False, left= False)
    plt.xlabel('Number of Transactions')
    plt.ylabel('Transaction Type')
    plt.title('Transaction Type vs Number of Transactions')
    plt.subplot().set_xlim([6000, 7000]) #zoom in to see the differenct more
    plt.show()
#highest_transaction_rate()


#Find and plot which state has a high number of customers.
def num_customers_per_state():
    customer_count= df_cust.groupby('CUST_STATE')['CREDIT_CARD_NO'].count().reset_index()
    #print(customer_count)
    customer_count=customer_count.sort_values('CREDIT_CARD_NO')
    #print(customer_count)

    #highlight branch with the highest transaction sum
    colors=[]
    for b in customer_count['CUST_STATE']:
        if customer_count['CUST_STATE'][18] ==b :
            colors.append('red')
        else:
            colors.append('lightsteelblue')

    plt.barh(customer_count['CUST_STATE'], customer_count['CREDIT_CARD_NO'], color=colors)
    ax=plt.gca()
#turn off spines
    ax.set_frame_on(False)
    plt.tick_params(bottom = False, left= False)
    plt.xticks(rotation=90)
    plt.xlabel('Number of Customers')
    plt.ylabel('State')
    plt.title('Number of Customers per State')
    plt.margins(y=0) #remove white space
    plt.show()
#num_customers_per_state()


#Find and plot the sum of all transactions for each customer, and which customer has the highest transaction amount. hint(use CUST_SSN).
def total_transactions_per_customer():
    #grouping by ssn and suming all transactions
    transaction_sum=df_credit.groupby('CUST_SSN')['TRANSACTION_VALUE'].sum().reset_index()
    #set ssn to index
    transaction_sum=transaction_sum.set_index('CUST_SSN')
    #sort my transaction values descending
    transaction_sum= transaction_sum.sort_values('TRANSACTION_VALUE', ascending=False)
    print(transaction_sum)
    #get index of highest treansaction sum
    highest_transaction= transaction_sum.iloc[0]
    #convert it to an integer
    highest_transaction= int(highest_transaction)
    #highlight branch with the highest transaction sum
    colors=[]
    for b in transaction_sum['TRANSACTION_VALUE']:
        if transaction_sum.iloc[0]['TRANSACTION_VALUE'] ==b :
            colors.append('red')
        else:
            colors.append('lightsteelblue')
    #change widths
    widths=[]
    for w in transaction_sum['TRANSACTION_VALUE']:
        if transaction_sum.iloc[0]['TRANSACTION_VALUE'] == w:
            widths.append(8.0)
        else:
            widths.append(5.0)

    print(f"Customer with SSN {123451125}, has the highest transaction amount: {transaction_sum.iloc[0]['TRANSACTION_VALUE']:.2f}")

    ax=plt.gca()#get axis
    ax.set_frame_on(False)#turn off spines

    plt.bar(transaction_sum.index, transaction_sum['TRANSACTION_VALUE'], color=colors, width=widths)

    plt.xticks(rotation=90)
    plt.tick_params(bottom = False, left= False) #turn of tick marks
    plt.ticklabel_format(useOffset=False, style='plain') #turns off scientific notation
    plt.subplot().set_xlim([123451125, 123451837])
    plt.xlabel('Customer SSN')
    plt.ylabel('Sum of Transactions')
    plt.title('Sum of Transactions per Customer')
    plt.show()
#total_transactions_per_customer()

#Visualization
#Find and plot the percentage of applications approved for self-employed applicants.
def approved_self_employed_app():
    self_emp_data= loan_data.filter((loan_data['Self_Employed'] == 'Yes' ) & (loan_data['Application_Status'] == 'Y'))
    self_emp_data=self_emp_data.count()

    total_count= loan_data.count()

    #percentage of employees that are self employeed
    percentage= self_emp_data/total_count *100
    sizes=[percentage, 100-percentage]
    labels= 'Approved', 'Not Approved'
    explode= (0,0.1)
    plt.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%', colors= ['red','royalblue'])
    plt.title('Percentage of Approved Self-Employed Applicants')
    plt.show()
#approved_self_employed_app()

#Find the percentage of rejection for married male applicants.
def rejected_male_apps():
    male_data= loan_data.filter((loan_data['Gender']== 'Male') & (loan_data['Married']== 'Yes'))
    total_count_male= male_data.count()

    rejection= loan_data.filter((loan_data['Gender']== 'Male') & (loan_data['Married']== 'Yes') & (loan_data['Application_Status'] == 'N'))
    rejection_count= rejection.count()

    total_count= loan_data.count()

    percentage= rejection_count/total_count_male *100

    #percentage of employees that are self employeed
    sizes=[percentage, 100-percentage]
    labels= 'Approved', 'Not Approved'
    explode= (0,0.1)
    plt.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%', colors= ['red','royalblue'])
    plt.title('Percentage of Rejected Married Male Applicants')
    plt.show()
#rejected_male_apps()

#Find and plot the top three months with the largest transaction data.
def largest_transaction_data():
    #convert column from string to datetime
    df_credit['TIMEID']=pd.to_datetime(df_credit['TIMEID'], format='%Y%m%d')

    #group by month and sum transactions for each month
    monthly_data= df_credit.groupby(df_credit['TIMEID'].dt.month)['TRANSACTION_VALUE'].sum().reset_index()
    sorted_monthly_data= monthly_data.sort_values('TRANSACTION_VALUE',ascending=False)
    #print(sorted_monthly_data)

    plt.barh(sorted_monthly_data['TIMEID'], sorted_monthly_data['TRANSACTION_VALUE'], color=('red','red','red','lightsteelblue','lightsteelblue','lightsteelblue','lightsteelblue','lightsteelblue','lightsteelblue','lightsteelblue','lightsteelblue','lightsteelblue'))
    ax=plt.gca()
    ax.set_frame_on(False)
 
    # Add annotation to bars
    transaction_list= sorted_monthly_data.values.tolist()
    print(transaction_list)
    plt.annotate('202583.89', xy =(202583.89,10),
                    xytext =(202583.89,10))
    plt.annotate('201310.26', xy =(201310.26,5),
                    xytext =(201310.26,5))
    plt.annotate('201251.08', xy =(201251.08,12),
                    xytext =(201251.08,12))            

    plt.tick_params(bottom = False, left= False)
    plt.xticks(rotation=90)
    plt.ylabel('Month')
    plt.xlabel('Transaction Sum')
    plt.title('Top 3 months with the Largest Transaction Data')
    plt.subplot().set_xlim([175000, 210000])
    #plt.margins(y=0) #remove white space
    plt.show()
#largest_transaction_data()

#Find and plot which branch processed the highest total dollar value of healthcare transactions.
def highest_branch_total():
    #filter healthcare transaction types
    healthcare=dfbranch_credit.filter(dfbranch_credit['TRANSACTION_TYPE']== 'Healthcare')
    #convert from spark to pandas df
    healthcaredf=healthcare.toPandas()
    #group by branch code and sum transactions values for respective branch
    branches=healthcaredf.groupby(healthcaredf['BRANCH_CODE'])['TRANSACTION_VALUE'].sum().reset_index()
    #sort values in descending order
    branches=branches.sort_values('TRANSACTION_VALUE', ascending=False).reset_index()
    #print(branches)

    #highlight branch with the highest transaction sum
    colors=[]
    for b in branches['TRANSACTION_VALUE']:
        if branches['TRANSACTION_VALUE'][0] ==b :
            colors.append('red')
        else:
            colors.append('lightsteelblue')

    plt.barh(branches['BRANCH_CODE'], branches['TRANSACTION_VALUE'], color=colors)

    ax=plt.gca()
    ax.set_frame_on(False)
 
    # Add annotation to bars       
    plt.annotate('Branch 25 \n transactions = $4370.18',
                xy =(3700, 30),
                xytext =(3700, 30), 
                fontsize=8)
    
    plt.tick_params(bottom = False, left= False)
    plt.xticks(rotation=90)
    plt.ylabel('Branch Code')
    plt.xlabel('Transaction Sum')
    plt.title('Branch with Highest Healthcare Transactions')
    plt.show()
#def highest_branch_total()

#Front End User Console
print("Menu")

while True:
    menu= pyip.inputMenu(['Banking Transactions', 'Customer Details', 'Visualizations' ,'Exit'], numbered=True)
    if menu == 'Banking Transactions':
        while True:
            banking_menu= pyip.inputMenu(['Customer Transactions by Zip Code', 'Transaction Total for given Type', 'Transaction Total for Branches by State', 'Exit'], numbered =True)
            if banking_menu== 'Customer Transactions by Zip Code':
                display_transactions_given_zip_month_year() 
            elif banking_menu == 'Transaction Total for given Type':
                display_transaction_num_and_total_value_given_type()
            elif banking_menu == 'Transaction Total for Branches by State':
                display_transaction_num_and_total_value_given_state()
            elif banking_menu == 'Exit':
                break
            else:
                print('Please make a valid selection')
    elif menu == 'Customer Details':
        while True:
            customer_menu= pyip.inputMenu(['Check Account Details', 'Modify Existing Account', 'Monthly Bill', 'Display Transactions', 'Exit'], numbered=True)
            if customer_menu == 'Check Account Details':
                check_account_details()
            elif customer_menu == 'Modify Existing Account':
                modify_account_details()
            elif customer_menu == 'Monthly Bill':
                generate_monthly_bill()
            elif customer_menu == 'Display Transactions':
                display_transaction_between_two_dates()
            elif customer_menu == 'Exit':
                break
            else:
                print('Please make a valid selection')
    elif menu == 'Visualizations':
        while True:
            graph_menu= pyip.inputMenu(['Transaction type with Highest Rate', 'Number of Customers per State', 'Sum of Transactions per Customer', 'Approved Self-Employed Applicants', 'Rejected Male Applicants', 'Transactions per Month', 'Healthcare Transactions per Branch','Exit'], numbered=True)
            if graph_menu== 'Transaction type with Highest Rate':
                highest_transaction_rate()
            elif graph_menu == 'Number of Customers per State':
                num_customers_per_state()
            elif graph_menu == 'Sum of Transactions per Customer':
                total_transactions_per_customer()
            elif graph_menu == 'Approved self-employed applicants':
                approved_self_employed_app()
            elif graph_menu == 'Rejected Male Applicants':
                rejected_male_apps()
            elif graph_menu == 'Transactions per Month':
                largest_transaction_data()
            elif graph_menu == 'Healthcare Transactions per Branch':
                highest_branch_total()
            elif graph_menu == 'Exit':
                break
            else:
                print('Please make a valid selection')
    elif menu == 'Exit':
        break