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

# ------------- Query 3 -------------
# Write a SQL query to display only the details of employees who either
# earn the highest salary or the lowest salary in each department from the employee table

# I use windows functions to get the lowest and highest salary by department
query3 = conn.execute(
    text(
        """
        with
            high_low_salary as (select *, min(salary) over (partition by dept_name) as min_salary,
            max(salary) over(partition by dept_name) as max_salary
            from employee),

            high_low_salary_top as (
            select *, case when salary in (min_salary, max_salary) then 1
                    else NULL end as high_or_low_top
                    from high_low_salary
                    order by dept_name, salary)

        select emp_id, emp_name, dept_name, salary, max_salary, min_salary
        from high_low_salary_top
        where high_or_low_top = 1;
        """
    )
)


# ------------- Query 4 -------------
# From the doctors table, fetch the details of doctors who
# work in the same hospital but in different specialty

# I joined the table with itself to obtain the desired result
query4 = conn.execute(
    text(
        """
        select d1.name, d1.speciality, d1.hospital
        from doctors as d1
        join doctors as d2
        on (d1.hospital = d2.hospital
        and d1.speciality != d2.speciality);
        """
    )
)


# ------------- Query 5 -------------
# From the login_details table, fetch the users who
# logged in consecutively 3 or more times

# I use the lead and lag functions to compare the user name
# with the line before and after, and see if they coincide
query5 = conn.execute(
    text(
        """
        with login_details_temp as (
            select *,
                    case when lead(user_name) over() = user_name
                    and lag(user_name) over() = user_name
                    then 1 else null end as top_name
            from login_details),

            login_details_temp2 as (select *,
                    case when lead(top_name) over() = 1
                    or lag(top_name) over() = 1
                    or top_name = 1
                    then 1 else null end as top_name2
            from login_details_temp)

        select user_name,login_date from login_details_temp2
        where top_name2 = 1;
        """
    )
)


# ------------- Query 6 -------------
# From the students table, write a SQL query to
# interchange the adjacent student names

# I use the lead and lag functions to interchange student names
# based on the parity of the id column
query6 = conn.execute(
    text(
        """
        with
        student_adjacent as (select *,
                                case when id % 2 = 0 then lag(student_name) over()
                                else lead(student_name) over() end as adjacent_name
                            from students)

        select id, student_name,
            case when adjacent_name is not null then adjacent_name
                    else student_name end as new_student_name
        from student_adjacent;
        """
    )
)


# ------------- Query 7 -------------
# From the weather table, fetch all the records when London
# had extremely cold temperature for 3 consecutive days or more

# I use the lead and lag functions to spot consecutive negative temperatures
query7 = conn.execute(
    text(
        """
        with weather_temp1 as (
                select *,
                case when temperature < 0
                    and lead(temperature) over(order by day) < 0
                    and lag(temperature) over(order by day) < 0
                then temperature
                else null end as neg_temp_comp
                from weather
        ),
            weather_temp2 as (
                select *,
                case when
                    lead(neg_temp_comp) over(order by day) is not null
                    or lag(neg_temp_comp) over(order by day) is not null
                    or neg_temp_comp is not null
                then 1 else 0 end as is_neg_temp_3_consecutive
                from weather_temp1

            )

        select id, city, temperature, day from weather_temp2
        where is_neg_temp_3_consecutive = 1;
        """
    )
)

# ------------- Query 8 -------------
# From the following 3 tables (event_category, physician_speciality, patient_treatment),
# write a SQL query to get the histogram of specialties of the unique physicians
# who have done the procedures but never did prescribe anything

# I do consecutive joins to have a single table while filtering
# the data according to the desired query
query8 = conn.execute(
    text(
        """
        with tb1 as (
                select pt.patient_id, pt.event_name, pt.physician_id, ec.category
                from patient_treatment as pt
                join
                event_category as ec
                on (pt.event_name = ec.event_name)
            ),

            tb2 as (
                select distinct event_name, physician_id from tb1
                where category = 'Procedure'
                and physician_id not in (select distinct physician_id from tb1 where category = 'Prescription')
            ),

            tb3 as (
                select tb2.physician_id, ps.speciality from tb2
                join
                physician_speciality as ps
                on (tb2.physician_id = ps.physician_id)
            )

        select speciality, count(speciality) as speciality_count from tb3
        group by speciality;
        """
    )
)

# ------------- Query 9 -------------
# Find the top 2 accounts with the maximum number of unique patients on a monthly basis

# I calculate the number of (unique) patients per month and account_id,
# then use the row_number() window function to keep the top 2 per month
query9 = conn.execute(
    text(
        """
        with
            patient_logs_temp1 as (
                select month_name, account_id, count(distinct patient_id) as patient_count
                from patient_logs
                group by month_name, account_id
                order by month_name asc, patient_count desc, account_id asc),

            patient_logs_temp2 as (
                select *, row_number() over(partition by month_name) as rn from patient_logs_temp1
                )

        select month_name, account_id, patient_count from patient_logs_temp2
        where rn <= 2;
        """
    )
)


# ------------- Query 10 -------------
# Write a SQL Query to fetch “N” consecutive records from
# a table based on a certain condition

# The solution to this question lies not with me, but with the author of the questions.
# The idea of solution is to create a column whose values are constant on
# consecutive days (when the temperature is negative).
# I've decided to put the solution in a stored procedure,
# so that I can call the query as a function with a parameter
query10 = conn.execute(
    text(
        """
        create procedure weather_neg_temp_consecutive(in cnt int)
        with
            weather_temp1 as
                (select *, id - row_number() over (order by id) as diff
                from weather w
                where w.temperature < 0),

            weather_temp2 as
                (select *,
                count(*) over (partition by diff order by diff) as cnt
                from weather_temp1)

        select id, city, temperature, day
        from weather_temp2
        where weather_temp2.cnt = cnt;

        # Obtain 5 consecutive records where the temperature was negative
        call weather_neg_temp_consecutive(5);
        """
    )
)
