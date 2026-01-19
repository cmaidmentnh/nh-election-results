#!/usr/bin/env python3
"""Create an admin user for the election results entry system."""

import sys
import getpass
from auth import create_user, get_db

def main():
    print("Create Admin User")
    print("-" * 30)

    username = input("Username: ").strip()
    if not username:
        print("Error: Username is required")
        sys.exit(1)

    # Check if user exists
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
    if cursor.fetchone():
        print(f"Error: User '{username}' already exists")
        conn.close()
        sys.exit(1)
    conn.close()

    password = getpass.getpass("Password: ")
    if not password:
        print("Error: Password is required")
        sys.exit(1)

    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Error: Passwords do not match")
        sys.exit(1)

    user_id = create_user(username, password, role='admin')
    if user_id:
        print(f"\nAdmin user '{username}' created successfully (ID: {user_id})")
        print(f"Login at: /login")
    else:
        print("Error creating user")
        sys.exit(1)

if __name__ == '__main__':
    main()
