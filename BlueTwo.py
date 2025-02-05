import mysql.connector

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="MudangXh20",
    password="Olaosilo4u*",  # Enter your correct password
    database="Xh20"  # Change this if your database name is different
)

cursor = conn.cursor()

# Run a simple query to check if connection works
cursor.execute("SHOW TABLES;")  # Lists all tables in the database

print("✅ Connected to MySQL! Tables in database:")
for table in cursor.fetchall():
    print(table)

# Close connection
cursor.close()
conn.close()
