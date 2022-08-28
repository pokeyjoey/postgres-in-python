import psycopg2

DB_NAME="careers"
DB_HOST="career-scraper.crd5vw1vref2.us-east-1.rds.amazonaws.com"
DB_USER="student"
DB_PASSWORD="jigsaw_student"

print('getting database connection')
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/careers"
conn = psycopg2.connect(db_url)

cursor = conn.cursor()

print('select information about all of the tables')
cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
tables = []
for table in cursor.fetchall():
    tables.append(table[0])


relevant_tables = ['states', 'cities', 'companies', 'position_locations',
                   'position_skills', 'skills', 'positions', 'job_titles']
print(f'relevant tables {relevant_tables}')
for relevant_table in relevant_tables:
    cursor.execute(f"Select * FROM {relevant_table} LIMIT 0")
    print(relevant_table, [desc[0] for desc in cursor.description])

print('most frequent job titles')
sql = """SELECT jt.name, count(*) as most_frequent_job_titles
            FROM positions p
            JOIN job_titles jt
            ON p.job_title_id = jt.id
            GROUP BY jt.name;"""
cursor.execute(sql)
results = cursor.fetchall()
print(results)

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

