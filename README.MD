creating a database using the files i have gathered in the payroll csv page


these are the columns:
Employee_name
office
city
employee_title
biweekly/hourly_rate
payroll_type
pay_period
pay_period_begin_date
pay_period_end_date
check_date
legislative_entity

first thing is to make a data model that's in third normal form
or I can create a star schema


tables:
employee 
    *employee id (autoincrement primary key)
    *Name
    *title
Office
    *Office_id (autoincrement primary key)
    *Office name
    *City
pay_rates:
    *pay_rate_id(autoincrement primary key)
    *biweekly/hourly_rate
Payroll_type
    *payroll_type_id (autoincrement primary key)
    *payroll_type   
Legislative_entity
    *legilative_entity_id(autoincrement primary key)
    *legislative_entity
payperiod
    *pay_period (primary key)
    *pay_period_begin_date
    *pay_period_end_date
    *check_date


***********************************
Brain storming:
I should create a python function that will make sure the data isn't duplicated

***********************************

EmployeeID***employee_name***office***city***employee_title***biweekly/hourly_rate***payroll_typle***legilative_entity***

***********************************
employeeId***office_id***employee_name***employee_title***///

office_id***office***city***///

legilative_id***legislative_entity***
