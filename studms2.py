from studms import get_connection
import sys 

table_setup_sql = """
CREATE TABLE IF NOT EXISTS students (
id SERIAL PRIMARY KEY,
name VARCHAR(60),
age INT,
gender CHAR(1)
);
"""

def initialize():
    conn=None
    cur=None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(table_setup_sql)
        conn.commit()
        if cur:
            cur.close()
        if conn:
            conn.close()
    except Exception as e:
        print(f"Failed to initialize database:", e)
        if conn:
            conn.rollback()
        sys.exit(1)
    

def add_student(name, age, gender):
    sql = """INSERT INTO students (name, age, gender) VALUES (%s, %s, %s) RETURNING id"""
    conn=None
    cur=None 
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql, (name, age, gender))
        new_id = cur.fetchone()[0]
        conn.commit()
        print(f"Student added with id = {new_id}")
        if cur:
            cur.close()
        if conn:
            conn.close()
    except Exception as e:
        print(f"Error adding student:", e)
        if conn:
            conn.rollback()
   

def list_students():
    sql = """SELECT id, name, age, gender FROM students ORDER BY id"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        if not rows:
            print(f"No student found")
            return
        print(f"id | name | age | gender")
        for r in rows:
            print(f"{r[0]} | {r[1]} | {r[2]} | {r[3]}")
        conn.commit()
        if cur:
            cur.close()
        if conn:
            conn.close()
    except Exception as e:
        print(f"Error listing students:", e)
        if conn:
            conn.rollback()
    

def search_students_by_name(name_substr):
    sql = """SELECT id, name, age, gender FROM students WHERE name LIKE %s ORDER BY id;"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, (f"%{name_substr}%",))
        rows = cur.fetchall()
        if not rows:
            print(f"No match found")
            return 
        for r in rows:
            print(r)
        if cur:
            cur.close()
        if conn:
            conn.close()
    except Exception as e:
        print(f"Error searching students", e)

def update_student(id, name=None, age=None, gender=None):
    conn = get_connection()
    cur = conn.cursor()
    try:
        updates = []
        values = []
        if name is not None:
            updates.append("name = %s")
            values.append(name)
        if age is not None:
            updates.append("age = %s")
            values.append(age)
        if gender is not None:
            updates.append("gender = %s")
            values.append(gender)
        
        if not updates:
            print(f"Nothing to update")
            return
        
        values.append(id)
        sql = """UPDATE students SET {','.join(updates)} WHERE id = %s"""
        cur.execute(sql, tuple(values))
        conn.commit()
        print(f"Student updated")
        if cur:
            cur.close()
        if conn:
            conn.close()
    except Exception as e:
        print(f"Error", e)

def delete_student(id):
    conn = get_connection()
    cur = conn.cursor()
    try:
        sql = """DELETE FROM students WHERE id = %s"""
        cur.execute(sql, (id,))
        if cur.rowcount == 0:
            print(f"No student with that id")
        else:
            conn.commit()
            print(f"Student deleted")
            if cur:
                cur.close()
            if conn:
                conn.close()
    except Exception as e:
        print(f"Error deleting student", e)

if __name__ == "__main__":
    

    
    delete_student(5)