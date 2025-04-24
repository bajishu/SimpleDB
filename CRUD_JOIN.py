# CRUD + JOIN
from db import Database
from parser import SQLParser

db = Database()
parser = SQLParser()

# --- Step 1: Setup Commands ---
commands = [
    'CREATE TABLE users (id, name);',
    'INSERT INTO users VALUES (1, "Alice");',
    'INSERT INTO users VALUES (2, "Bob");',
    'CREATE TABLE orders (id, user_id, product);',
    'INSERT INTO orders VALUES (1, 1, "Book");',
    'INSERT INTO orders VALUES (2, 2, "Pen");',
    'INSERT INTO orders VALUES (3, 1, "Notebook");',
]

selects = [
    'SELECT id, name FROM users;',
    'SELECT * FROM users WHERE name = "Alice";',
    'SELECT id FROM users WHERE name = "Bob";',
]

updates_and_deletes = [
    'UPDATE users SET name = "Charlie" WHERE id = 2;',
    'DELETE FROM users WHERE name = "Alice";',
]

join_query = 'SELECT users.id, orders.product FROM users JOIN orders ON users.id = orders.user_id;'

# --- Step 2: Run CREATE + INSERT ---
for cmd in commands:
    result = parser.parse(cmd)
    action = result[0]

    if action == "create_table":
        _, table_name, columns = result
        db.create_table(table_name, columns)
    elif action == "insert_into":
        _, table_name, values = result
        db.insert_into(table_name, values)

# --- Step 3: Display Table ---
print("\nInitial table: users")
print(db.show_table("users"))

# --- Step 4: Handle SELECTs ---
for query in selects:
    result = parser.parse(query)
    action = result[0]

    if action == "select":
        if len(result) == 4:  # normal SELECT query
            _, table, cols, cond = result
            rows = db.select(table, cols, cond)
            print(f"\nQuery: {query}")
            for row in rows:
                print(row)
        elif len(result) == 6:  # SELECT with WHERE condition
            _, table, cols, cond, extra = result
            rows = db.select(table, cols, cond)
            print(f"\nQuery: {query}")
            for row in rows:
                print(row)
    elif action == "select_join":
        _, t1, cols, cond, t2, jleft, jright = result
        rows = db.select_join(t1, t2, jleft, jright, cols, cond)
        print(f"\nJoin Query: {query}")
        for row in rows:
            print(row)

# --- Step 5: Handle UPDATE & DELETE ---
for query in updates_and_deletes:
    result = parser.parse(query)
    if result[0] == "update":
        _, table, col, val, cond = result
        count = db.update(table, col, val, cond)
        print(f"\nUpdated {count} rows.")
    elif result[0] == "delete":
        _, table, cond = result
        count = db.delete(table, cond)
        print(f"\nDeleted {count} rows.")

# --- Step 6: Show final state ---
print("\nFinal table: users")
print(db.show_table("users"))

# --- Step 7: Handle JOIN ---
result = parser.parse(join_query)
if result[0] == "select_join":
    _, t1, cols, cond, t2, jleft, jright = result
    rows = db.select_join(t1, t2, jleft, jright, cols, cond)
    print(f"\nJoin Query: {join_query}")
    for row in rows:
        print(row)
