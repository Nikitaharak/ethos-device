#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect('users.db')
cursor = conn.cursor()

print("=" * 70)
print("USERS TABLE SCHEMA")
print("=" * 70)

# Get table schema
cursor.execute("PRAGMA table_info(users)")
columns = cursor.fetchall()

print("\nColumns:")
for col in columns:
    print(f"  {col[1]:20s} {col[2]:15s} {'NOT NULL' if col[3] else 'NULL':10s} {'PK' if col[5] else ''}")

print("\n" + "=" * 70)
print("LOGIN_LOGS TABLE SCHEMA")
print("=" * 70)

# Get login_logs schema
cursor.execute("PRAGMA table_info(login_logs)")
log_columns = cursor.fetchall()

print("\nColumns:")
for col in log_columns:
    print(f"  {col[1]:20s} {col[2]:15s} {'NOT NULL' if col[3] else 'NULL':10s} {'PK' if col[5] else ''}")

# Get sample user
print("\n" + "=" * 70)
print("SAMPLE USER DATA")
print("=" * 70)

cursor.execute("SELECT * FROM users LIMIT 1")
sample = cursor.fetchone()
if sample:
    print(f"\nSample user columns: {[desc[0] for desc in cursor.description]}")
else:
    print("\nNo users found in database")

conn.close()
