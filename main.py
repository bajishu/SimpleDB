from db import *

# Create the database and table
db = Database()
db.create_table("users", ["id", "name", "age"])
db.insert_into("users", [1, "Alice", 30])
db.insert_into("users", [2, "Bob", 25])
db.insert_into("users", [3, "Charlie", 30])
db.insert_into("users", [4, "David", 25])

# Show the table
print(db.show_table("users"))

# SELECT with ORDER BY and LIMIT
print(db.select("users", ["*"], order_by="age", limit=2))

# GROUP BY and HAVING
print(db.select_group_by("users", ["age"], "age", having=("age", 30)))

# Update operation
db.update("users", "age", 31, ("name", "Alice"))
print(db.show_table("users"))

# DELETE operation
db.delete("users", ("name", "David"))
print(db.show_table("users"))
