"""This file contains my solutions about some SQL queries"""

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
# Write a SQL Query to fetch all the duplicate records in users table

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

# Each line is numbered and partitioned by email to keep only duplicates
query1 = conn.execute(
    text(
        """
        select * from (
        select *, row_number() over (partition by email order by user_id) as rn
        from users) as x
        where rn >= 2;
        """
    )
)


# ------------- Query 2 -------------
# Write a SQL query to fetch the second last record from employee table

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

# The solution I propose works even if emp_id was not a primary key
query2 = conn.execute(
    text(
        """
        with employee_temp1 as
            (select *, row_number() over() as rn from employee),

            employee_temp2 as
            (select *, row_number() over(order by rn desc) as rn2 from employee_temp1)

        select emp_id, emp_name, dept_name, salary from employee_temp2
        where rn2 = 2;
        """
    )
)
