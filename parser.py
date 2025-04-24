import re

class SQLParser:
    def parse(self, query):
        # Match CREATE TABLE
        create_table_regex = r"CREATE TABLE (\w+) \(([\w, ]+)\);"
        insert_into_regex = r"INSERT INTO (\w+) VALUES \(([\w, \"\']+)\);"
        select_regex = r"SELECT (.+) FROM (\w+)( WHERE (.+))?( GROUP BY (.+))?( ORDER BY (.+))?( LIMIT (\d+))?( HAVING (.+))?;"
        update_regex = r"UPDATE (\w+) SET (\w+) = (.+) WHERE (.+);"
        delete_regex = r"DELETE FROM (\w+) WHERE (.+);"
        join_regex = r"SELECT (.+) FROM (\w+) JOIN (\w+) ON (\w+.\w+) = (\w+.\w+);"

        # Check for SELECT queries with GROUP BY, ORDER BY, LIMIT, HAVING
        if match := re.match(select_regex, query):
            cols = match.group(1).strip()
            table = match.group(2)
            where = match.group(4) if match.group(4) else None
            group_by = match.group(6) if match.group(6) else None
            order_by = match.group(8) if match.group(8) else None
            limit = int(match.group(10)) if match.group(10) else None
            having = match.group(12) if match.group(12) else None
            return ("select", table, cols, where, group_by, order_by, limit, having)

        # Handle other commands like CREATE, INSERT, UPDATE, DELETE, JOIN, etc.
        if match := re.match(create_table_regex, query):
            table_name = match.group(1)
            columns = match.group(2).split(", ")
            return ("create_table", table_name, columns)
        if match := re.match(insert_into_regex, query):
            table_name = match.group(1)
            values = match.group(2).split(", ")
            return ("insert_into", table_name, values)
        if match := re.match(update_regex, query):
            table_name = match.group(1)
            col = match.group(2)
            val = match.group(3)
            cond = match.group(4)
            return ("update", table_name, col, val, cond)
        if match := re.match(delete_regex, query):
            table_name = match.group(1)
            cond = match.group(2)
            return ("delete", table_name, cond)
        if match := re.match(join_regex, query):
            cols = match.group(1)
            t1 = match.group(2)
            t2 = match.group(3)
            jleft = match.group(4)
            jright = match.group(5)
            return ("select_join", t1, cols, None, t2, jleft, jright)

        raise ValueError(f"Unrecognized query: {query}")

    def _parse_create(self, sql):
        match = re.match(r'CREATE TABLE (\w+)\s*\((.*?)\)', sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid CREATE TABLE syntax")
        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(",")]
        return ("create_table", table_name, columns)

    def _parse_insert(self, sql):
        match = re.match(r'INSERT INTO (\w+)\s*VALUES\s*\((.*?)\)', sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid INSERT INTO syntax")
        table_name = match.group(1)
        raw_values = match.group(2)
        values = self._parse_values(raw_values)
        return ("insert_into", table_name, values)

    def _parse_values(self, raw):
        # Handles numbers and quoted strings
        pattern = re.compile(r'(".*?"|\'.*?\'|\d+)')
        matches = pattern.findall(raw)
        def cast(value):
            value = value.strip()
            if value.startswith(("'", '"')):
                return value[1:-1]
            return int(value) if value.isdigit() else value
        return [cast(m) for m in matches]

    def parse(self, sql):
        sql = sql.strip().rstrip(";")
        if sql.upper().startswith("CREATE TABLE"):
            return self._parse_create(sql)
        elif sql.upper().startswith("INSERT INTO"):
            return self._parse_insert(sql)
        elif sql.upper().startswith("SELECT"):
            return self._parse_select(sql)
        elif sql.upper().startswith("UPDATE"):
            return self._parse_update(sql)
        elif sql.upper().startswith("DELETE FROM"):
            return self._parse_delete(sql)
        else:
            raise ValueError("Unsupported SQL command")

    def _parse_select(self, sql):
        pattern = r'SELECT (.+) FROM (\w+)(?: JOIN (\w+) ON (\w+\.\w+)\s*=\s*(\w+\.\w+))?(?: WHERE (.+))?'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid SELECT syntax")

        columns = [col.strip() for col in match.group(1).split(",")]
        main_table = match.group(2)
        join_table = match.group(3)
        join_left = match.group(4)
        join_right = match.group(5)
        where_clause = match.group(6)

        condition = None
        if where_clause:
            m = re.match(r'(\w+)\s*=\s*(\'[^\']*\'|"[^"]*"|\d+)', where_clause)
            if not m:
                raise ValueError("Invalid WHERE clause")
            key, val = m.group(1), m.group(2)
            val = val[1:-1] if val.startswith(("'", '"')) else int(val)
            condition = (key, val)

        return ("select_join" if join_table else "select", main_table, columns, condition, join_table, join_left, join_right)


    def _parse_update(self, sql):
        pattern = r'UPDATE (\w+)\s+SET\s+(\w+)\s*=\s*(\'[^\']*\'|"[^"]*"|\d+)\s+WHERE\s+(\w+)\s*=\s*(\'[^\']*\'|"[^"]*"|\d+)'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid UPDATE syntax")

        table = match.group(1)
        set_col = match.group(2)
        set_val = match.group(3)
        where_col = match.group(4)
        where_val = match.group(5)

        def cast(v):
            return v[1:-1] if v.startswith(("'", '"')) else int(v)

        return ("update", table, set_col, cast(set_val), (where_col, cast(where_val)))

    def _parse_delete(self, sql):
        pattern = r'DELETE FROM (\w+)\s+WHERE\s+(\w+)\s*=\s*(\'[^\']*\'|"[^"]*"|\d+)'
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid DELETE syntax")

        table = match.group(1)
        where_col = match.group(2)
        where_val = match.group(3)
        val = where_val[1:-1] if where_val.startswith(("'", '"')) else int(where_val)

        return ("delete", table, (where_col, val))

