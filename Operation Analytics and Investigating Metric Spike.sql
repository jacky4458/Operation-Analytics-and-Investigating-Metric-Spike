# Operation Analytics and Investigating Metric Spike

# case study 1. job_data analysis

# task is to create table job_data

create database project3;
use project3;

#task 01: jobs reviewed over time

CREATE TABLE job_data (
    ds DATE NOT NULL,
    job_id INT NOT NULL,
    actor_id INT NOT NULL,
    event VARCHAR(50) NOT NULL,
    language VARCHAR(50) NOT NULL,
    time_spent INT NOT NULL,
    org CHAR(2) NOT NULL,
    PRIMARY KEY (job_id, actor_id)
);


#import the data into table

insert into job_data (ds, job_id, actor_id, event, language, time_spent, org)
Values('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
('2020-11-30', 22, 1006, 'transfer','Aeabic', 25, 'B'),
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
('2020-11-28', 23, 1005, 'transfer', 'persian', 22, 'D'),
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
('2020-11-26', 23, 1004, 'Skip', 'Persian', 56, 'A'),
('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');

#case study 01

#task 01: jobs reviewed over time

select ds as date,
count(job_id) as joint_job_id,
round((sum(time_spent) / 3600), 2) as total_time_per_hour, 
round((count(job_id) / (sum(time_spent)/3600)),2) as job_review
from job_data
where
ds between '2020-11-01' and '2020-11-30' 
group by ds
order by ds;

#task 02 : Throughput Analysis

select round(count(event) / sum(time_spent), 2) As weekly_avg_throughput
from job_data;

select ds as dates,
round(count(event) / sum(time_spent),2) as daily_avg_throughput
from job_data
group by ds
order by ds;

#task 04. Language Share Analysis:

select language, round(100* count(*) / total, 2) as percentage,
jd.total from job_data
cross join (select count(*)  as total 
from job_data) as jd
group by language, jd.total;

#task 04. Duplicate Rows Detection:

select actor_id, count(*) as duplicate
from job_data
group by actor_id
having count(*) > 1;

#case study 02 Investigating Metric Spike

show databases;
use project3;

#create users table

create table users(
user_id int not null,
created_at varchar(100),
company_id int,
language varchar(100),
activated_at varchar(100),
state varchar(100)); 

#upload users data

SHOW VARIABLES LIKE 'SECURE_FILE_PRIV';

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from users;

#add temprory column temp_created_at 

alter table users add column temp_created_at datetime;

#add data to the temp column

set sql_safe_updates = 0;

Update users set temp_created_at = str_to_date(created_at, '%d-%m-%Y %H:%i');

set sql_safe_updates = 1;

#change column name  from temp_created_at to created_at

alter table users drop column created_at;

alter table users change column temp_created_at created_at datetime;

#create events table

create table events(
user_id int not null,
occurred_at varchar(100),
event_type varchar(100),
event_name varchar(100),
location varchar(50),
device varchar(50),
user_type int);

#load data into events table

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from events;

desc events;

#add temprory column temp_occurred_at

alter table events add column temp_occurred_at datetime;

#temporory disable safe update

set sql_safe_updates = 0;

#add data into temp column

update events set temp_occurred_at = str_to_date(occurred_at, '%d-%m-%Y %H:%i');

#re-enable safe mode 

set sql_safe_updates = 1;

#drop column occurred_at 

alter table events drop column occurred_at;

#change column temp_occurred_at

alter table events change column temp_occurred_at occurred_at datetime;

drop table events;

#creating email_events table

create table email_events(
user_id int,
occurred_at varchar(100),
action varchar(100),
user_type int
);

#load data into email_events

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table email_events
fields terminated by','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

#task 01 Weekly User Engagement:

select extract(week from occurred_at) as week_num,
count(distinct user_id) as active_users
from events
where event_type = 'engagement'
group by week_num
order by week_num;

#Task 02. User Growth Analysis:

select count(*) from users where state = 'active';
select distinct state from users;

with weekly_active_users as(
select extract(year from created_at) as year,
extract(week from created_at) as week_number,
count(distinct user_id) num_of_users
from users
group by year, week_number
)

select year, week_number, num_of_users,
sum(num_of_users) over (order by year, week_number) as cumulative_users
from weekly_active_users
order by year, week_number;

#task 03. Weekly Retention Analysis:

SELECT 
    first AS "week_numbers",
    SUM(CASE WHEN week_number = 0 THEN 1 ELSE 0 END) AS "week_0",
    SUM(CASE WHEN week_number = 1 THEN 1 ELSE 0 END) AS "week_1",
    SUM(CASE WHEN week_number = 2 THEN 1 ELSE 0 END) AS "week_2",
    SUM(CASE WHEN week_number = 3 THEN 1 ELSE 0 END) AS "week_3",
    SUM(CASE WHEN week_number = 4 THEN 1 ELSE 0 END) AS "week_4",
    SUM(CASE WHEN week_number = 5 THEN 1 ELSE 0 END) AS "week_5",
    SUM(CASE WHEN week_number = 6 THEN 1 ELSE 0 END) AS "week_6",
    SUM(CASE WHEN week_number = 7 THEN 1 ELSE 0 END) AS "week_7",
    SUM(CASE WHEN week_number = 8 THEN 1 ELSE 0 END) AS "week_8",
    SUM(CASE WHEN week_number = 9 THEN 1 ELSE 0 END) AS "week_9",
    SUM(CASE WHEN week_number = 10 THEN 1 ELSE 0 END) AS "week_10",
    SUM(CASE WHEN week_number = 11 THEN 1 ELSE 0 END) AS "week_11",
    SUM(CASE WHEN week_number = 12 THEN 1 ELSE 0 END) AS "week_12",
    SUM(CASE WHEN week_number = 13 THEN 1 ELSE 0 END) AS "week_13",
    SUM(CASE WHEN week_number = 14 THEN 1 ELSE 0 END) AS "week_14",
    SUM(CASE WHEN week_number = 15 THEN 1 ELSE 0 END) AS "week_15",
    SUM(CASE WHEN week_number = 16 THEN 1 ELSE 0 END) AS "week_16",
    SUM(CASE WHEN week_number = 17 THEN 1 ELSE 0 END) AS "week_17",
    SUM(CASE WHEN week_number = 18 THEN 1 ELSE 0 END) AS "week_18"
FROM (
    SELECT 
        m.user_id, 
        m.login_week, 
        n.first, 
        m.login_week - n.first AS week_number
    FROM (
        SELECT 
            user_id, 
            EXTRACT(WEEK FROM occurred_at) AS login_week
        FROM 
            events
        GROUP BY 
            user_id, 
            login_week
    ) m
    JOIN (
        SELECT 
            user_id,
            MIN(EXTRACT(WEEK FROM occurred_at)) AS first
        FROM 
            events 
        GROUP BY 
            user_id
    ) n
    ON m.user_id = n.user_id
) sub
GROUP BY 
    first
ORDER BY 
    first;
    
#task 04 Weekly Engagement Per Device:

SELECT
    EXTRACT(WEEK FROM occurred_at) AS week_number,
    COUNT(DISTINCT CASE WHEN device = 'dell inspiron notebook' THEN user_id ELSE NULL END) AS dell_inspiron_notebook,
    COUNT(DISTINCT CASE WHEN device = 'iphone 5' THEN user_id ELSE NULL END) AS iphone_5,
    COUNT(DISTINCT CASE WHEN device = 'iphone 4s' THEN user_id ELSE NULL END) AS iphone_4s,
    COUNT(DISTINCT CASE WHEN device = 'iphone 5s' THEN user_id ELSE NULL END) AS iphone_5s,
    COUNT(DISTINCT CASE WHEN device = 'ipad air' THEN user_id ELSE NULL END) AS ipad_air,
    COUNT(DISTINCT CASE WHEN device = 'windows surface' THEN user_id ELSE NULL END) AS windows_surface,
    COUNT(DISTINCT CASE WHEN device = 'macbook air' THEN user_id ELSE NULL END) AS macbook_air,
    COUNT(DISTINCT CASE WHEN device = 'macbook pro' THEN user_id ELSE NULL END) AS macbook_pro,
    COUNT(DISTINCT CASE WHEN device = 'ipad mini' THEN user_id ELSE NULL END) AS ipad_mini,
    COUNT(DISTINCT CASE WHEN device = 'kindle fire' THEN user_id ELSE NULL END) AS kindle_fire,
    COUNT(DISTINCT CASE WHEN device = 'amazon fire phone' THEN user_id ELSE NULL END) AS amazon_fire_phone,
    COUNT(DISTINCT CASE WHEN device = 'nexus 5' THEN user_id ELSE NULL END) AS nexus_5,
    COUNT(DISTINCT CASE WHEN device = 'nexus 7' THEN user_id ELSE NULL END) AS nexus_7,
    COUNT(DISTINCT CASE WHEN device = 'nexus 10' THEN user_id ELSE NULL END) AS nexus_10,
    COUNT(DISTINCT CASE WHEN device = 'samsung_galaxy_s4' THEN user_id ELSE NULL END) AS samsung_galaxy_s4,
    COUNT(DISTINCT CASE WHEN device = 'samsung_galaxy_tablet' THEN user_id ELSE NULL END) AS samsung_galaxy_tablet,
    COUNT(DISTINCT CASE WHEN device = 'samsung_galaxy_note' THEN user_id ELSE NULL END) AS samsung_galaxy_note,
    COUNT(DISTINCT CASE WHEN device = 'lenovo thinkpad' THEN user_id ELSE NULL END) AS lenovo_thinkpad,
    COUNT(DISTINCT CASE WHEN device = 'acer aspire notebook' THEN user_id ELSE NULL END) AS acer_aspire_notebook,
    COUNT(DISTINCT CASE WHEN device = 'asus chromebook' THEN user_id ELSE NULL END) AS asus_chromebook,
    COUNT(DISTINCT CASE WHEN device = 'htc one' THEN user_id ELSE NULL END) AS htc_one,
    COUNT(DISTINCT CASE WHEN device = 'nokia lumia 635' THEN user_id ELSE NULL END) AS nokia_lumia_635,
    COUNT(DISTINCT CASE WHEN device = 'mac mini' THEN user_id ELSE NULL END) AS mac_mini,
    COUNT(DISTINCT CASE WHEN device = 'hp pavilion desktop' THEN user_id ELSE NULL END) AS hp_pavilion_desktop,
    COUNT(DISTINCT CASE WHEN device = 'dell inspiron desktop' THEN user_id ELSE NULL END) AS dell_inspiron_desktop
FROM
    events
WHERE
    event_type = 'engagement'
GROUP BY
    week_number
ORDER BY
    week_number;
    
    
# task 05 Email Engagement Analysis:

SELECT
    100.0 * SUM(CASE WHEN email_action = 'email-open' THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN email_action = 'email-sent' THEN 1 ELSE 0 END), 0) AS email_open_rate,
    
    100.0 * SUM(CASE WHEN email_action = 'email-clicked' THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN email_action = 'email-sent' THEN 1 ELSE 0 END), 0) AS email_clicked_rate
FROM (
    SELECT 
        *,
        CASE 
            WHEN action IN ('sent_weekly_digest', 'sent_reengagement_email') THEN 'email-sent'
            WHEN action = 'email_open' THEN 'email-open'
            WHEN action = 'email_clickthrough' THEN 'email-clicked'
            ELSE NULL
        END AS email_action
    FROM
        project3.email_events
) a;






