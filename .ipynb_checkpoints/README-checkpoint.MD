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


Should create a pandas function that will make sure the data isn't duplicated


I want to have a time date and time seperate


tables:
employee
    -office
    -title
    -

payrole 
    -rate
    -payrole type

payperiod
    -pay_period_begin_date
    -pay_period_end_date
    -check_date
    
Legislative_entity
    -entity
