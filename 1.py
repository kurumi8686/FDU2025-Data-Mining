import sqlite3

conn = sqlite3.connect('dataset_cache.sqlite')
cursor = conn.cursor()

# 列出所有表
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables:", tables)

# 查看某个表内容
for table_name in tables:
    print(f"\nTable: {table_name[0]}")
    cursor.execute(f"SELECT * FROM {table_name[0]};")  # 只查看前5行
    for row in cursor.fetchall():
        print(row)

conn.close()
