import psycopg2
from faker import Faker
import random
import sys
import os
from datetime import timedelta
from config import DB_CONFIG

conn = psycopg2.connect(
    **DB_CONFIG
)

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.append(parent_dir)

# Options to INSERT INTO bg3.tables
ticket_status = ["Open", "In Progress", "Resolved", "Closed"]
ticket_categories = ["Bug", "Payment Methods", "Gameplay", "Login Issue"]
devices = ["PC", "Xbox", "PlayStation", "Steam Deck"]
referrals = ["Google Ads", "YouTube", "Twitch", "Boca a boca"]
countries = [
        ("USA", "America"), ("Brazil", "South America"), ("Canada", "America"),
        ("UK", "Europe"), ("Germany", "Europe"), ("Japan", "Asia")
    ]
session_status = ["Active", "Inactive", "Expired"]
    
# Faker initialization
fake = Faker()

# Functions to insert data into tables
def create_devices():
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO bg3.devices (name) VALUES (%s) ON CONFLICT DO NOTHING", [(d,) for d in devices])
    conn.commit()
    cursor.close()

def create_tickets_categories():
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO bg3.tickets_categories (name) VALUES (%s) ON CONFLICT DO NOTHING", [(d,) for d in ticket_categories])
    conn.commit()
    cursor.close()

def create_referrals():
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO bg3.referrals (name) VALUES (%s) ON CONFLICT DO NOTHING", [(r,) for r in referrals])
    conn.commit()
    cursor.close()

def create_countries():
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO bg3.countries (name, continent) VALUES (%s, %s) ON CONFLICT DO NOTHING", countries)
    conn.commit()
    cursor.close()

def create_sessions_status():
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO bg3.session_status (description) VALUES (%s) ON CONFLICT DO NOTHING", [(s,) for s in session_status])
    conn.commit()
    cursor.close()

def create_tickets_status():
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO bg3.ticket_status (description) VALUES (%s) ON CONFLICT DO NOTHING", [(t,) for t in ticket_status])
    conn.commit()
    cursor.close()

def create_users():
    user_ids = []
    cursor = conn.cursor()
    for _ in range(2000): 
        email = fake.email()
        password = fake.password()
        name = fake.name()
        created_at = fake.date_time_this_year()
        country_id = random.randint(1, len(countries)) 
        referral_id = random.randint(1, len(referrals))

        cursor.execute(
            "INSERT INTO bg3.Users (email, password, name, created_at, country_id, referral_id) VALUES (%s, %s, %s, %s, %s, %s) on conflict do nothing RETURNING id ",
            (email, password, name, created_at, country_id, referral_id)
        )
        row = cursor.fetchone()
        if row is not None:
            user_ids.append(row[0])
        else:
            # Optionally, query the existing user's id if needed
            cursor.execute("SELECT id FROM bg3.Users WHERE email = %s", (email,))
            existing_row = cursor.fetchone()
            if existing_row is not None:
                user_ids.append(existing_row[0])
    conn.commit()
    cursor.close()
    return user_ids

# Creating sessions for users
def create_sessions(user_ids):
    cursor = conn.cursor()
    for _ in range(5000):  
        user_id = random.choice(user_ids)
        start_date = fake.date_time_this_year()
        end_date = start_date + timedelta(hours=random.randint(1, 5))
        device_id = random.randint(1, len(devices))
        country_access_id = random.randint(1, len(countries))
        session_status_id = random.randint(1, len(session_status))

        cursor.execute(
            "INSERT INTO bg3.Session (start_date, end_date, device_id, user_id, country_access_id, session_status_id) VALUES (%s, %s, %s, %s, %s, %s)",
            (start_date, end_date, device_id, user_id, country_access_id, session_status_id)
        )
    conn.commit()
    cursor.close()

# Creating tickets for users
def create_tickets(user_ids):
    cursor = conn.cursor()
    for _ in range(2000):  
        user_id = random.choice(user_ids)
        ticket_category_id = random.randint(1,len(ticket_categories))
        created_at = fake.date_time_this_year()
        first_response_at = created_at + timedelta(hours=random.randint(1, 5)) if random.random() > 0.3 else None
        resolved_at = created_at + timedelta(hours=random.randint(2, 10)) if random.random() > 0.3 else None
        title = fake.sentence()
        description = fake.text()
        ticket_status_id = random.randint(1, len(ticket_status))

        cursor.execute(
            "INSERT INTO bg3.Tickets (created_at, first_response_at, resolved_at, ticket_category_id, title, description, user_id, ticket_status_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (created_at, first_response_at, resolved_at, ticket_category_id, title, description, user_id, ticket_status_id)
        )
    conn.commit()
    cursor.close()

if __name__=="__main__":
    create_devices()
    create_tickets_categories()
    create_referrals()
    create_countries()
    create_sessions_status()
    create_tickets_status()
    users = create_users()
    create_sessions(users)
    create_tickets(users)
    conn.close()
    print("Execution completed successfully!")