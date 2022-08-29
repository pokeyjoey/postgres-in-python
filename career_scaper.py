import pandas as pd
import psycopg2
import warnings

DB_NAME="careers"
DB_HOST="career-scraper.crd5vw1vref2.us-east-1.rds.amazonaws.com"
DB_USER="student"
DB_PASSWORD="jigsaw_student"

print('getting database connection')
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/careers"
conn = psycopg2.connect(db_url)

cursor = conn.cursor()

print('')
print('')
print('')
print('select information about all of the tables')
cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
tables = []
for table in cursor.fetchall():
    tables.append(table[0])


relevant_tables = ['states', 'cities', 'companies', 'position_locations',
                   'position_skills', 'skills', 'positions', 'job_titles']
print('')
print('')
print('')
print(f'relevant tables {relevant_tables}')


print('')
print('')
print('')
print(f'fields from relevant tables')
for relevant_table in relevant_tables:
    cursor.execute(f"Select * FROM {relevant_table} LIMIT 0")
    print(relevant_table, [desc[0] for desc in cursor.description])

print('')
print('')
print('Read the positions table')
sql = "select * from positions limit 2"
print(pd.read_sql(sql, conn).T)

print('')
print('')
print('Read the position_locations table')
sql = "select * from position_locations limit 2"
print(pd.read_sql(sql, conn).T)

print('')
print('')
print('Read the cities table')
sql = "select * from cities limit 2"
print(pd.read_sql(sql, conn).T)

print('')
print('')
print('')
print('most frequent job titles')
sql = """SELECT jt.name, count(*) as most_frequent_job_titles
            FROM positions p
            JOIN job_titles jt
            ON p.job_title_id = jt.id
            GROUP BY jt.name;"""
cursor.execute(sql)
results = cursor.fetchall()
print(results)

print('')
print('')
print('')
print('Top skills required of data engineers')
sql = """SELECT s.name, count(*) as top_skills
            FROM position_skills ps
            JOIN positions p
            ON ps.position_id = p.id
            JOIN skills s 
            ON ps.skill_id = s.id
            GROUP BY s.name
            ORDER BY count(*) DESC
            LIMIT 10""" 
cursor.execute(sql)
results = cursor.fetchall()
print(results)


print('')
print('')
print('')
print('Average salary of data engineers sorted by minimum_experience DESC')
sql = """SELECT p.minimum_experience, ROUND((AVG(p.minimum_salary) + AVG(p.minimum_salary))/2) as average_salary
            FROM positions p
            GROUP BY p.minimum_experience
            ORDER BY average_salary DESC""" 
print(pd.read_sql(sql, conn))

print('')
print('')
print('')
print('Average salary of data engineers in New York City')
sql = """SELECT c.name, ROUND((AVG(p.minimum_salary) + AVG(p.minimum_salary))/2) as average_salary
            FROM position_locations pl
            JOIN positions p
            ON pl.position_id = p.id
            JOIN cities c
            ON pl.city_id = c.id
            WHERE c.name LIKE '%New York%'
            GROUP BY c.name""" 
print(pd.read_sql(sql, conn))


