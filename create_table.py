"""This file contains sql queries to create the tables
needed to answer the questions"""

from sqlalchemy import create_engine, text

from configuration.config import (
    DATABASE_USER,
    DATABASE_PASSWORD,
    DATABASE_HOST,
    DATABASE_PORT,
    DATABASE_NAME,
)

# Create a database connection with SQLAlchemy (MySQL Server 8.0)
engine = create_engine(
    f"mysql+mysqlconnector://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
)
conn = engine.connect()  # Connection to the database


# ------------- Query 1 -------------
# Create users table
users_table = conn.execute(
    text(
        """
        drop table if exists users;

        create table users (
        user_id int primary key,
        user_name varchar(30) not null,
        email varchar(50));

        insert into users values
        (1, 'Sumit', 'sumit@gmail.com'),
        (2, 'Robin', 'robin@gmail.com'),
        (3, 'Farhana', 'farhana@gmail.com'),
        (4, 'Robin', 'robin@gmail.com'),
        (5, 'Reshma', 'reshma@gmail.com');
        (6, 'Robin', 'robin@gmail.com');
        """
    )
)


# ------------- Query 2 -------------
# Create employee table
epmloyee_table = conn.execute(
    text(
        """
    drop table if exists employee;

    create table employee (
    emp_id int primary key,
    emp_name varchar(50) not null,
    dept_name varchar(50),
    salary int);

    insert into employee values
    (101, 'Mohan', 'Admin', 4000);
    (102, 'Rajkumar', 'HR', 3000);
    (103, 'Akbar', 'IT', 4000);
    (104, 'Dorvin', 'Finance', 6500);
    (105, 'Rohit', 'HR', 3000);
    (106, 'Rajesh',  'Finance', 5000);
    (107, 'Preet', 'HR', 7000);
    (108, 'Maryam', 'Admin', 4000);
    (109, 'Sanjay', 'IT', 6500);
    (110, 'Vasudha', 'IT', 7000);
    (111, 'Melinda', 'IT', 8000);
    (112, 'Komal', 'IT', 10000);
    (113, 'Gautham', 'Admin', 2000);
    (114, 'Manisha', 'HR', 3000);
    (115, 'Chandni', 'IT', 4500);
    (116, 'Satya', 'Finance', 6500);
    (117, 'Adarsh', 'HR', 3500);
    (118, 'Tejaswi', 'Finance', 5500);
    (119, 'Cory', 'HR', 8000);
    (120, 'Monica', 'Admin', 5000);
    (121, 'Rosalin', 'IT', 6000);
    (122, 'Ibrahim', 'IT', 8000);
    (123, 'Vikram', 'IT', 8000);
    (124, 'Dheeraj', 'IT', 11000);
"""
    )
)

# ------------- Query 4 -------------
# Create doctors table
doctors_table = conn.execute(
    text(
        """
        drop table if exists doctors;

        create table doctors (
        id int primary key,
        name varchar(50) not null,
        speciality varchar(100),
        hospital varchar(50),
        city varchar(50),
        consultation_fee int
        );

        insert into doctors values
        (1, 'Dr. Shashank', 'Ayurveda', 'Apollo Hospital', 'Bangalore', 2500),
        (2, 'Dr. Abdul', 'Homeopathy', 'Fortis Hospital', 'Bangalore', 2000),
        (3, 'Dr. Shwetha', 'Homeopathy', 'KMC Hospital', 'Manipal', 1000),
        (4, 'Dr. Murphy', 'Dermatology', 'KMC Hospital', 'Manipal', 1500),
        (5, 'Dr. Farhana', 'Physician', 'Gleneagles Hospital', 'Bangalore', 1700),
        (6, 'Dr. Maryam', 'Physician', 'Gleneagles Hospital', 'Bangalore', 1500);
        """
    )
)

# ------------- Query 5 -------------
# Create login_details table
login_details_table = conn.execute(
    text(
        """
        drop table if exists login_details;

        create table login_details (
        login_id int primary key,
        user_name varchar(50) not null,
        login_date date);

        insert into login_details values
        (101, 'Michael', current_date),
        (102, 'James', current_date),
        (103, 'Stewart', current_date+1),
        (104, 'Stewart', current_date+1),
        (105, 'Stewart', current_date+1),
        (106, 'Michael', current_date+2),
        (107, 'Michael', current_date+2),
        (108, 'Stewart', current_date+3),
        (109, 'Stewart', current_date+3),
        (110, 'James', current_date+4),
        (111, 'James', current_date+4),
        (112, 'James', current_date+5),
        (113, 'James', current_date+6);
        """
    )
)


# ------------- Query 6 -------------
# Create students table
students_table = conn.execute(
    text(
        """
        drop table if exists students;

        create table students (
        id int primary key,
        student_name varchar(50) not null);

        insert into students values
        (1, 'James'),
        (2, 'Michael'),
        (3, 'George'),
        (4, 'Stewart'),
        (5, 'Robin');
        """
    )
)

# ------------- Query 7 -------------
# Create weather table
weather_table = conn.execute(
    text(
        """
        drop table if exists weather;

        create table weather (
        id int,
        city varchar(50),
        temperature int,
        day date);

        insert into weather values
        (1, 'London', -1, '2021-01-01'),
        (2, 'London', -2, '2021-01-02'),
        (3, 'London', 4, '2021-01-03'),
        (4, 'London', 1, '2021-01-04'),
        (5, 'London', -2, '2021-01-05'),
        (6, 'London', -5, '2021-01-06'),
        (7, 'London', -7, '2021-01-07'),
        (8, 'London', 5, '2021-01-08');
        """
    )
)

# ------------- Query 8 -------------
# Create event_category table
event_category_table = conn.execute(
    text(
        """
        drop table if exists event_category;

        create table event_category (
        event_name varchar(50),
        category varchar(100));

        insert into event_category values
        ('Chemotherapy', 'Procedure'),
        ('Radiation', 'Procedure'),
        ('Immunosuppressants', 'Prescription'),
        ('BTKI', 'Prescription'),
        ('Biopsy', 'Test');
        """
    )
)

# Create physician_speciality table
physician_speciality_table = conn.execute(
    text(
        """
        drop table if exists physician_speciality;

        create table physician_speciality (
        physician_id int,
        speciality varchar(50));

        insert into physician_speciality values
        1000, 'Radiologist'),
        (2000, 'Oncologist'),
        (3000, 'Hermatologist'),
        (4000, 'Oncologist'),
        (5000, 'Pathologist'),
        (6000, 'Oncologist');
        """
    )
)

# Create patient_treatment table
patient_treatment_table = conn.execute(
    text(
        """
        drop table if exists patient_treatment;

        create table patient_treatment (
        patient_id int,
        event_name varchar(50),
        physician_id int);

        insert into patient_treatment values
        (1,'Radiation', 1000),
        (2,'Chemotherapy', 2000),
        (1,'Biopsy', 1000),
        (3,'Immunosuppressants', 2000),
        (4,'BTKI', 3000),
        (5,'Radiation', 4000),
        (4,'Chemotherapy', 2000),
        (1,'Biopsy', 5000),
        (6,'Chemotherapy', 6000);
        """
    )
)

# ------------- Query 9 -------------
# Create patient_logs table
patient_logs_table = conn.execute(
    text(
        """
        drop table if exists patient_logs;

        create table patient_logs (
        account_id int,
        date date,
        patient_id int);

        insert into patient_logs values
        (1, str_to_date('02-01-2020', '%d-%m-%Y'), 100),
        (1, str_to_date('27-01-2020', '%d-%m-%Y'), 200),
        (2, str_to_date('01-01-2020', '%d-%m-%Y'), 300),
        (2, str_to_date('21-01-2020', '%d-%m-%Y'), 400),
        (2, str_to_date('21-01-2020', '%d-%m-%Y'), 300),
        (2, str_to_date('01-01-2020', '%d-%m-%Y'), 500),
        (3, str_to_date('20-01-2020', '%d-%m-%Y'), 400),
        (1, str_to_date('04-03-2020', '%d-%m-%Y'), 500),
        (3, str_to_date('20-01-2020', '%d-%m-%Y'), 450);

        # I'm adding a 'month_name' column to get the months as strings
        alter table patient_logs
        add column month_name varchar(10);

        update patient_logs
        set month_name = monthname(date);
        """
    )
)
