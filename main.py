import psycopg2 as pg


def create_db():  # создает таблицы
    with pg.connect(dbname='courses', user='postgres', password='glad') as conn:
        with conn.cursor() as cur:
            cur.execute("""
            create table if not exists student
                (id serial primary key not null, 
                name character varying(100) not null,
                gpa numeric(10, 2) null, 
                birth timestamp with time zone null);
                """)
            cur.execute("""
            create table if not exists course
                (id serial primary key not null,
                name character varying(100) not null);
                """)
            cur.execute("""
            create table if not exists student_course
                (id serial primary key not null,
                student_id integer references student(id),
                course_id integer references course(id));
            """)
    print('Database created')


def add_students(course_id, **kwargs):
    data = list(kwargs.values())
    create_db()
    with pg.connect(dbname='courses', user='postgres', password='glad') as conn:
        with conn.cursor() as cur:
            cur.execute("""
            insert into student(name, gpa, birth) values (%s, %s, %s);
            """, (data[0], data[1], data[2]))
            print('Student added into students table')
            cur.execute("select * from student;")
            rows = cur.fetchall()
            n = len(rows)
            current_student = rows[n-1]
            student_id = current_student[0]
            # добавляю ряды в таблицу с курсами с названиями сходными их id, так как без этого не заполняется
            # таблица со связями
            cur.execute("""
            insert into course(name) values (%s);
            """, (str(course_id)))
            cur.execute("""
            insert into student_course(student_id, course_id) values (%s, %s);
            """, (student_id, course_id))
            print('Student enrolled in a course')


def get_students(course_id):  # возвращает студентов определенного курса
    with pg.connect(dbname='courses', user='postgres', password='glad') as conn:
        with conn.cursor() as cur:
            cur.execute("""
            select s.id, s.name, c.name from student_course sc
            join student s on s.id = sc.student_id
            join course c on c.id = sc.course_id;
            """)
            rows = cur.fetchall()
            for row in rows:
                if row[2] == course_id:
                    print(f'id: {row[0]}, name: {row[1]}')


def add_student(**kwargs): # просто создает студента
    data = list(kwargs.values())
    with pg.connect(dbname='courses', user='postgres', password='glad') as conn:
        with conn.cursor() as cur:
            cur.execute("""
            insert into student(name, gpa, birth) values (%s, %s, %s);
            """, (data[0], data[1], data[2]))
            print('Student added into students table')


def get_student(student_id):
    with pg.connect(dbname='courses', user='postgres', password='glad') as conn:
        with conn.cursor() as cur:
            cur.execute("""
            select s.id, s.name, s.gpa, s.birth, c.name from student_course sc
            join student s on s.id = sc.student_id
            join course c on c.id = sc.course_id;
            """)
            rows = cur.fetchall()
            for row in rows:
                if row[0] == student_id:
                    print(f"name: {row[1]}, gpa: {row[2]}, birth: {row[3]}, course: {row[4]}")