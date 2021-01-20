from sqlserverconn import dbConfig
import pypyodbc as pyconn


conn = pyconn.connect(**dbConfig)
print(conn)

Sqlcommand = "select employee_id,First_Name,Last_Name,Phone_Number from employees"
cursor = conn.cursor()
cursor.execute(Sqlcommand)
allrows = cursor.fetchall()  # This return a tuple list in list
print("Firstname", "", "lastname", "", "", "phone number", sep="\t")
for list in allrows:
    # where the blackslash is a seperator
    print(list[1], list[2], list[3], sep="\t")
