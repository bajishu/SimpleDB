class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns  # List of column names
        self.rows = []          # List of dicts

    def insert(self, values):
        if len(values) != len(self.columns):
            raise ValueError("Column count does not match value count.")
        row = dict(zip(self.columns, values))
        self.rows.append(row)

    def __str__(self):
        lines = [" | ".join(self.columns)]
        for row in self.rows:
            lines.append(" | ".join(str(row[col]) for col in self.columns))
        return "\n".join(lines)


class Database:
    def __init__(self):
        self.tables = {}

    def create_table(self, name, columns):
        if name in self.tables:
            raise ValueError(f"Table {name} already exists.")
        self.tables[name] = Table(name, columns)

    def insert_into(self, table_name, values):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        self.tables[table_name].insert(values)

    def show_table(self, name):
        if name not in self.tables:
            raise ValueError(f"Table {name} does not exist.")
        return str(self.tables[name])

    def select(self, table_name, columns, where=None, order_by=None, limit=None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        table = self.tables[table_name]
        result = []

        for row in table.rows:
            if where:
                key, val = where
                if row.get(key) != val:
                    continue
            if columns == ["*"]:
                result.append(row)
            else:
                result.append({col: row[col] for col in columns})

        # ORDER BY
        if order_by:
            result = sorted(result, key=lambda x: x[order_by])

        # LIMIT
        if limit is not None:
            result = result[:limit]

        return result

    def select_group_by(self, table_name, columns, group_by_column, having=None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        table = self.tables[table_name]

        # Group rows by group_by_column
        group_index = table.columns.index(group_by_column)
        grouped = {}
        for row in table.rows:
            group_value = row[group_by_column]
            if group_value not in grouped:
                grouped[group_value] = []
            grouped[group_value].append(row)

        # Apply HAVING condition (after grouping)
        result = []
        for group_value, group_rows in grouped.items():
            group_row = {group_by_column: group_value}
            for col in columns:
                if col != group_by_column:
                    # Aggregate function (like COUNT)
                    group_row[col] = len(group_rows)  # Using COUNT as example
            if having:
                key, val = having
                if group_row.get(key) != val:
                    continue
            result.append(group_row)

        return result

    def select_join(self, table1, table2, join_left, join_right, columns, where=None):
        if table1 not in self.tables or table2 not in self.tables:
            raise ValueError("One of the tables does not exist.")

        rows1 = self.tables[table1].rows
        rows2 = self.tables[table2].rows

        left_table, left_col = join_left.split(".")
        right_table, right_col = join_right.split(".")

        result = []
        for r1 in rows1:
            for r2 in rows2:
                if r1[left_col] == r2[right_col]:
                    combined = {f"{table1}.{k}": v for k, v in r1.items()}
                    combined.update({f"{table2}.{k}": v for k, v in r2.items()})
                    if where:
                        key, val = where
                        if combined.get(key) != val:
                            continue
                    if columns == ["*"]:
                        result.append(combined)
                    else:
                        result.append({col: combined[col] for col in columns})
        return result

    def update(self, table_name, set_col, set_val, where):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        table = self.tables[table_name]
        count = 0
        for row in table.rows:
            if row.get(where[0]) == where[1]:
                row[set_col] = set_val
                count += 1
        return count

    def delete(self, table_name, where):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        table = self.tables[table_name]
        before = len(table.rows)
        table.rows = [row for row in table.rows if row.get(where[0]) != where[1]]
        return before - len(table.rows)
