# DROP TABLES

pay_table_drop = "DROP TABLE IF EXISTS pay"
pay_period_table_drop = "DROP TABLE IF EXISTS pay_period"
payroll_type_table_drop = "DROP TABLE IF EXISTS payroll_type"
office_table_drop = "DROP TABLE IF EXISTS office"
employee_table_drop = "DROP TABLE IF EXISTS employee"
legislative_entity_table_drop = "DROP TABLE IF EXISTS legislative_entity"
city_table_drop = "DROP TABLE IF EXISTS city"

# CREATE TABLES

pay_table_create = ("""
    CREATE TABLE IF NOT EXISTS pay( 
    pay_id SERIAL PRIMARY KEY, 
    pay float NOT NULL, 
    pay_period_key integer, 
    payroll_type_key integer, 
    employee_key integer, 
    office_key integer
    )
""")
pay_period_table_create = ("""
    CREATE TABLE IF NOT EXISTS pay_period(
    pay_period_key integer,
    pay_period_year integer,
    pay_period_begin_date timestamp, 
    pay_period_end_date timestamp, 
    check_date timestamp, 
    PRIMARY KEY(pay_period_key, pay_period_year)
    )
""")
payroll_type_table_create = ("""
    CREATE TABLE IF NOT EXISTS payroll_type( 
    payroll_type_key SERIAL PRIMARY KEY,
    payroll_type text
    )
""")
office_table_create = ("""
    CREATE TABLE IF NOT EXISTS office(
    office_key SERIAL PRIMARY KEY, 
    office_name text, 
    city_key integer
    )
""")
employee_table_create = ("""
    CREATE TABLE IF NOT EXISTS employee(
    employee_key SERIAL PRIMARY KEY, 
    employee_name text, 
    employee_title text, 
    legislative_entity_key integer
    )
""")
legislative_entity_table_create = ("""
    CREATE TABLE IF NOT EXISTS legislative_entity(
    legislative_entity_key SERIAL PRIMARY KEY, 
    legislative_entity text
    )
""")
city_table_create = ("""
    CREATE TABLE IF NOT EXISTS city(
    city_key SERIAL PRIMARY KEY, 
    city text
    )
""")

# INSERT RECORDS

pay_table_insert = ("""
    INSERT INTO pay(
    pay, pay_period_key, payroll_type_key, employee_key, office_key)
    VALUES (%s, %s, %s, %s, %s)
""")
pay_period_table_insert = ("""
    INSERT INTO pay_period(
    pay_period_key, pay_period_year, pay_period_begin_date, pay_period_end_date, check_date)
    VALUES (%s, %s, %s, %s, %s)
""")
payroll_type_table_insert = ("""
    INSERT INTO payroll_type(payroll_type)
    VALUES (%s)
""")
office_table_insert = ("""
    INSERT INTO office(office_name, city_key)
    VALUES (%s, %s)
""")
employee_table_insert = ("""
    INSERT INTO employee(employee_name, employee_title, legislative_entity_key)
    VALUES (%s, %s, %s)
""")
legislative_entity_insert = ("""
    INSERT INTO legislative_entity(legislative_entity)
    VALUES (%s)
""")
city_table_insert = ("""
    INSERT INTO city(city)
    VALUES (%s)
""")

# QUERY LISTS

create_table_queries = [
    pay_table_create, pay_period_table_create, payroll_type_table_create, office_table_create, employee_table_create, legislative_entity_table_create, city_table_create]
drop_table_queries =[
    pay_table_drop, pay_period_table_drop, payroll_type_table_drop, office_table_drop, employee_table_drop, legislative_entity_table_drop, city_table_drop]