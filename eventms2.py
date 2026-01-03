from eventms1 import get_connection

venue_table = """
CREATE TABLE IF NOT EXISTS venue(
id SERIAL PRIMARY KEY,
name VARCHAR(40),
address VARCHAR(50),
max_capacity INTEGER
);"""

event_table = """
CREATE TABLE IF NOT EXISTS events (
id SERIAL PRIMARY KEY,
title VARCHAR(40),
date DATE,
time TIME,
capacity INTEGER,
venue_id INTEGER NOT NULL REFERENCES venue(id) ON DELETE CASCADE
);"""

bookings_table = """CREATE TABLE IF NOT EXISTS bookings (
id SERIAL PRIMARY KEY,
event_id INTEGER REFERENCES events(id) ON DELETE CASCADE,
booking_date DATE,
customer_name VARCHAR(50),
email VARCHAR(50)
);"""

def initialize():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(venue_table)
        cur.execute(event_table)
        cur.execute(bookings_table)
        conn.commit()
        print(f"Tables created successfully")
        if cur:
            cur.close()
        if conn:
            conn.close()
    except Exception as e:
        print(f"Oops, sorry unable to create tables", e)
        conn.rollback()
def add_venue(name, address, max_capacity):
    conn = get_connection()
    cur = conn.cursor()
    sql = """INSERT INTO venue (name, address, max_capacity) VALUES (%s, %s, %s) RETURNING id;"""
    try:
        cur.execute(sql, (name, address, max_capacity))
        new_id = cur.fetchone()[0]
        conn.commit()
        print(f"Venue has been added with id = {new_id}")
    except Exception as e:
        print(f"Oops, sorry couldn't add venue", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
    
def list_venues():
        conn = get_connection()
        cur = conn.cursor()
        sql = """SELECT id, name, address, max_capacity FROM venue ORDER BY id;"""
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows:
                print(f"No venue found")
            print(f"id | name | address | max_capacity")
            for r in rows:
                print(f"{r[0]} | {r[1]} | {r[2]} | {r[3]}")
        except Exception as e:
            print(f"Oops, error listing books", e)
        finally:
            cur.close()
            conn.close()
def search_venue(keyword):
    conn = get_connection()
    cur = conn.cursor()
    sql = """SELECT id, name, address, max_capacity FROM venue WHERE name ILIKE %s OR address ILIKE %s ORDER BY id;"""
    try:
        cur.execute(sql, (f"%{keyword}%", f"%{keyword}%"))
        rows = cur.fetchall()
        if not rows:
            print(f"Oops, no venue found")
            return
        for w in rows:
            print(w)
    except Exception as e:
        print(f"Sorry, error searching for venue", e)
    finally:
        cur.close()
        conn.close()
def update_venue(id, name=None, address=None, max_capacity=None):
    conn = get_connection()
    cur = conn.cursor()
    updates = []
    value = []
    try:
        if name is not None:
            updates.append("name = %s")
            value.append(name)
        if address is not None:
            updates.append("address = %s")
            value.append(address)
        if max_capacity is not None:
            updates.append("max_capacity = %s")
            value.append(max_capacity)
        if not updates:
            print(f"Oops, nothing to update")
            return
        value.append(id)
        sql = "UPDATE venue SET name = %s, address = %s, max_capacity = %s WHERE id = %s;"
        cur.execute(sql, (name, address, max_capacity))
        conn.commit()
        print(f"Venue has been updated successfully")
    except Exception as e:
        print(f"Error updating venue", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
def delete_venue(id):
    conn = get_connection()
    cur = conn.cursor()
    sql = """DELETE FROM venue WHERE id = %s"""
    try:
        cur.execute(sql, (id,))
        conn.commit()
        print(f"Venue has been successfully deleted")
    except Exception as e:
        print(f"Error deleting venue", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
def add_event(name, time, capacity, venue_id):
    conn = get_connection()
    cur = conn.cursor()
    sql = """INSERT INTO events (name, time, capacity, venue_id) VALUES (%s, %s, %s, %s) RETURNING id;"""
    try:
        cur.execute(sql, (name, time, capacity, venue_id))
        new_id = cur.fetchone()[0]
        print(f"A new venue has been added with id = {new_id}")
        conn.commit()
    except Exception as e:
        print(f"Error adding venue to system", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
def list_events():
    conn = get_connection()
    cur = conn.cursor()
    sql = """SELECT id, name, date, time, capacity, venue_id FROM events ORDER BY id;"""
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        print(f"id | name | date | time | capacity | venue_id")
        if not rows:
            print(f"Oops, no venue found")
            return
        for r in rows:
            print(f"{r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]}")
    except Exception as e:
        print(f"Error listing all events", e)
    finally:
        cur.close()
        conn.close()
def search_event(keyword):
    conn = get_connection()
    cur = conn.cursor()
    sql = """SELECT id, name, date, time, capacity, venue_id FROM events WHERE name ILIKE %s ORDER BY id;"""
    try:
        cur.execute(sql, (f"%{keyword}%"))
        rows = cur.fetchall()
        if not rows:
            print(f"Oops, no event match found")
            return
        for r in rows:
            print(r)
    except Exception as e:
        print(f"Error searching for event", e)
    finally:
        cur.close()
        conn.close()
def update_event(id, name=None, date=None, time=None, capacity=None, venue_id=None):
    conn = get_connection()
    cur = conn.cursor()
    try:
        updates = []
        value = []
        if name is not None:
            updates.append("name = %s")
            value.append(name)
        if date is not None:
            updates.append("date = %s")
            value.append(date)
        if time is not None:
            updates.append("time = %s")
            value.append(time)
        if capacity is not None:
            updates.append("capacity = %s")
            value.append(capacity)
        if venue_id is not None:
            updates.append("venue_id = %s")
            value.append(venue_id)
        if not updates:
            print(f"Oops, nothing to update")
            return
        value.append(id)
        sql = "UPDATE events SET name = %s, date = %s, time = %s, capacity = %s, venue_id = %s WHERE id = %s"
        cur.execute(sql, (id,))
        conn.commit()
        print(f"Event has been successfully updated")
    except Exception as e:
        print(f"Error updating event", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
def delete_event(id):
    conn = get_connection()
    cur = conn.cursor()
    sql = """DELETE FROM events WHERE id = %s;"""
    try:
        cur.execute(sql, (id,))
        conn.commit()
        print(f"Event has been successfully deleted")
    except Exception as e:
        print(f"Error deleting event", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
def add_booking(event_id, booking_date, customer_name, email=None):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT v.max_capacity FROM events e
            JOIN venue v ON e.venue_id = v.id
            WHERE e.id = %s;""", (event_id,)
            )
        row = cur.fetchone()
        if row is None:
            print(f"Oops, event doesn't exist")
            return
        capacity = row[0]
        #now lets count the existing bookings 
        cur.execute(
            """SELECT COUNT(*) FROM bookings WHERE event_id = %s""", (event_id,)
            )
        booked = cur.fetchone()[0]
        if booked >= capacity:
            print(f"Sorry, event is fully booked")
            return
        #inserting into bookings
        cur.execute(
            """INSERT INTO bookings (event_id, booking_date, customer_name, email) VALUES (%s, %s, %s, %s)""",
            (event_id, booking_date, customer_name, email)
            )
        conn.commit()
        print(f"Booking successful")
    except Exception as e:
        print(f"Error adding booking", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
def search_booking(id):
    conn = get_connection()
    cur = conn.cursor()
    sql = """SELECT id, event_id, booking_date, customer_name, email FROM bookings WHERE id = %s;"""
    try:
        cur.execute(sql, (id,))
        row = cur.fetchall()
        if row is None:
            print(f"Oops, no booking found")
            return
        print(f"id | event_id | booking_date | customer_name | email")
        for r in row:
            print(f"{r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]}")
    except Exception as e:
        print(f"Error searching booking", e)
    finally:
        cur.close()
        conn.close()
def update_booking(id, event_id=None, booking_date=None, customer_name=None, email=None):
    conn = get_connection()
    cur = conn.cursor()
    updates = []
    value = []
    try:
        if event_id is not None:
            updates.append("event_id = %s")
            value.append(event_id)
        if booking_date is not None:
            updates.append("booking_date = %s")
            value.append(booking_date)
        if customer_name is not None:
            updates.append("customer_name = %s")
            value.append(customer_name)
        if email is not None:
            updates.append("email = %s")
            value.append(email)
        if not updates:
            print(f"Nothing to update")
            return
        value.append(id)
        sql = "UPDATE bookings SET {','.join(updates)} WHERE id = %s"
        cur.execute(sql, (event_id, booking_date, customer_name, email))
        conn.commit()
        if cur.rowcount == 0:
            print(f"There's no matching id")
        else:
            print(f"booking updated successfully")
    except Exception as e:
        print(f"Error updating booking", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
def delete_booking(id):
    conn = get_connection()
    cur = conn.cursor()
    sql = """DELETE FROM bookings WHERE id = %s"""
    try:
        cur.execute(sql, (id,))
        if cur.rowcount == 0:
            print(f"No matching id found")
        else:
            print(f"Booking has been deleted successfully")
        conn.commit()
    except Exception as e:
        print(f"Error deleting booking", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()




        
        
        













