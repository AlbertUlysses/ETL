# DROP TABLES

pay_table_drop = "DROP TABLE IF EXISTS pay"
pay_period_table_drop = "DROP TABLE IF EXISTS pay_period"
payroll_type_table_drop = "DROP TABLE IF EXISTS payroll_type"
office_table_drop = "DROP TABLE IF EXISTS office"
employee_table_drop = "DROP TABLE IF EXISTS employee"
legislative_entity_table_drop = "DROP TABLE IF EXISTS legislative_entity"
city_table_drop = "DROP TABLE IF EXISTS"

# CREATE TABLES

pay_table_create = ("CREATE TABLE IF NOT EXISTS pay \
( pay_id serial, pay float, pay_period_key int, payroll_type_key int, )")