from datetime import date, timedelta

from sqlalchemy import NVARCHAR, Date, DateTime, create_engine, extract, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

# Создание подключения к БД: 'Тип_БД+драйвер://пользователь:пароль@хост/имя_БД?driver=ODBC+driver+17+for+SQL+Server'
database_url = "mssql+pyodbc://uzver:12345678@15-2-441-1-PREP/vefu57_alchemy_lab?driver=ODBC+driver+17+for+SQL+Server"
engine = create_engine(database_url, echo=True)


class Base(DeclarativeBase):
    pass


class STUD(Base):
    __tablename__ = "stud"
    id: Mapped[int] = mapped_column(primary_key=True)
    last_name: Mapped[str] = mapped_column(NVARCHAR(25), nullable=False)
    f_name: Mapped[str] = mapped_column(NVARCHAR(25), nullable=False)
    s_name: Mapped[str] = mapped_column(NVARCHAR(25), nullable=True)
    form: Mapped[str] = mapped_column(NVARCHAR(10), nullable=False, default="очная")
    faculty: Mapped[str] = mapped_column(NVARCHAR(10), nullable=False, default="ФПМ")
    year: Mapped[int] = mapped_column(nullable=False, default=1)
    all_h: Mapped[int] = mapped_column(nullable=True, default=None)
    inclass_h: Mapped[int] = mapped_column(nullable=True, default=None)
    br_date: Mapped[DateTime] = mapped_column(Date, nullable=False)
    in_date: Mapped[DateTime] = mapped_column(Date, nullable=False)
    exm: Mapped[float] = mapped_column(nullable=True, default=None)


class TEACH(Base):
    __tablename__ = "teach"
    id: Mapped[int] = mapped_column(primary_key=True)
    last_name: Mapped[str] = mapped_column(NVARCHAR(25), nullable=False)
    f_name: Mapped[str] = mapped_column(NVARCHAR(25), nullable=False)
    s_name: Mapped[str] = mapped_column(NVARCHAR(25), nullable=True)
    subj: Mapped[str] = mapped_column(NVARCHAR(150), nullable=False)
    form: Mapped[str] = mapped_column(NVARCHAR(10), nullable=False, default="очная")
    faculty: Mapped[str] = mapped_column(NVARCHAR(10), nullable=False, default="ФПМ")
    year: Mapped[int] = mapped_column(nullable=False, default=1)
    hours: Mapped[int] = mapped_column(nullable=True, default=None)
    br_date: Mapped[date] = mapped_column(Date, nullable=False)
    start_work_date: Mapped[DateTime] = mapped_column(Date, nullable=False)


Base.metadata.create_all(engine)

stud_data_list = [
    ("Стрынгель", "К", None, "заочная", "ФПК", 1, 300, 100, "19831212", "20160901", 8),
    ("Козлова", "Д", "Е", "заочная", "ФПК", 2, 300, 100, "19831012", "20150901", 8.4),
    ("Федоров", "Н", "Н", "заочная", "ФПК", 3, 300, 100, "19811207", "20140901", 7),
    ("Рингель", "П", "О", "заочная", "ФПК", 3, 300, 100, "19730215", "20160901", 8),
    ("Бежик", "Н", "Н", "вечерняя", "ФПК", 1, 500, 400, "19931211", "2016-09-01", 4.5),
    ("Осипчик", "Н", "Н", "вечерняя", "ФПК", 1, 500, 400, "19831216", "20150901", 7.7),
    ("Белый", "С", "С", "вечерняя", "ФПК", 2, 450, 370, "19870627", "20150901", 6.7),
    (
        "Ботяновский",
        "А",
        "С",
        "вечерняя",
        "ФПК",
        2,
        450,
        370,
        "19870723",
        "20150901",
        7.6,
    ),
    (
        "Слободницкий",
        "С",
        "А",
        "вечерняя",
        "ФПК",
        2,
        450,
        370,
        "19870803",
        "20150901",
        6.7,
    ),
    ("Рогатка", "П", "Р", "очная", "ФПМ", 1, 500, 450, "19861027", "20160901", 7.4),
    ("Федоренко", "П", "Р", "очная", "ФПМ", 1, 500, 450, "19950426", "20160901", 5.6),
    ("Зингель", "П", "В", "очная", "ФПМ", 2, 500, 450, "19900425", "20150901", 3.4),
    ("Михеенок", "Л", "Н", "очная", "ФПМ", 2, 500, 450, "19890313", "20150901", 5.3),
    ("Савицкая", "Л", "Н", "очная", "ФПМ", 3, 450, 400, "19950705", "20140901", 7.7),
    ("Ковальчук", "О", "Е", "заочная", "ФПМ", 1, 350, 100, "19640523", "20160901", 7.6),
    (
        "Заболотная",
        "Л",
        "И",
        "заочная",
        "ФПМ",
        1,
        350,
        100,
        "19860914",
        "20160901",
        4.7,
    ),
    ("Ковриго", "И", None, "заочная", "ФПМ", 2, 360, 120, "19920301", "20150901", 7.7),
    ("Шарапо", "М", None, "заочная", "ФПМ", 2, 360, 120, "19970325", "20150901", 8.7),
    (
        "Сафроненко",
        "Н",
        "Л",
        "заочная",
        "ФПМ",
        3,
        370,
        130,
        "19920525",
        "20140901",
        7.7,
    ),
    ("Зайцева", "Т", "Я", "заочная", "ФПМ", 3, 370, 130, "19940725", "20140901", 5.6),
]

teach_data_list = [
    (
        "Скворцов",
        "К",
        None,
        "Дифференциальные исчисления",
        "очная",
        "ФПМ",
        1,
        150,
        "19831212",
        "20160901",
    ),
    (
        "Скворцов",
        "К",
        None,
        "Геометрия и алгебра",
        "очная",
        "ФПМ",
        1,
        200,
        "19831212",
        "20160901",
    ),
    (
        "Сидоренко",
        "Л",
        "К",
        "Теория вероятности",
        "очная",
        "ФПМ",
        1,
        100,
        "19831212",
        "20160901",
    ),
    (
        "Скворцов",
        "К",
        None,
        "Дифференциальные исчисления",
        "заочная",
        "ФПМ",
        1,
        34,
        "19831212",
        "20160901",
    ),
    (
        "Сидоренко",
        "Л",
        "К",
        "Геометрия и алгебра",
        "заочная",
        "ФПМ",
        1,
        50,
        "19831212",
        "20160901",
    ),
    (
        "Сидоренко",
        "Л",
        "К",
        "Теория вероятности",
        "заочная",
        "ФПМ",
        1,
        16,
        "19831212",
        "20160901",
    ),
    (
        "Круглов",
        "К",
        "Д",
        "Теория множеств",
        "очная",
        "ФПМ",
        2,
        150,
        "19860825",
        "20140901",
    ),
    (
        "Круглов",
        "К",
        "Д",
        "Методы численного анализа",
        "очная",
        "ФПМ",
        2,
        150,
        "19860825",
        "20140901",
    ),
    (
        "Загорова",
        "К",
        "Д",
        "Прикладная статистика",
        "очная",
        "ФПМ",
        2,
        150,
        "19791005",
        "20100901",
    ),
    (
        "Круглов",
        "К",
        "Д",
        "Теория множеств",
        "заочная",
        "ФПМ",
        2,
        40,
        "19860825",
        "20140901",
    ),
    (
        "Круглов",
        "К",
        "Д",
        "Методы численного анализа",
        "заочная",
        "ФПМ",
        2,
        40,
        "19860825",
        "20140901",
    ),
    (
        "Загорова",
        "К",
        "Д",
        "Прикладная статистика",
        "заочная",
        "ФПМ",
        2,
        40,
        "19791005",
        "20100901",
    ),
    ("Зуров", "П", None, "Философия", "очная", "ФПМ", 3, 50, "19780712", "20160901"),
    ("Зуров", "П", None, "Социология", "очная", "ФПМ", 3, 50, "19780712", "20160901"),
    (
        "Сидоренко",
        "Л",
        "К",
        "Методы машинного обучения",
        "очная",
        "ФПМ",
        3,
        150,
        "19831212",
        "20160901",
    ),
    (
        "Журков",
        "К",
        None,
        "Методы выпуклой оптимизации",
        "очная",
        "ФПМ",
        3,
        150,
        "19861116",
        "20150901",
    ),
    ("Курт", "П", "Р", "Философия", "заочная", "ФПМ", 3, 20, "19780712", "20160901"),
    ("Курт", "П", "Р", "Социология", "заочная", "ФПМ", 3, 20, "19780712", "20160901"),
    (
        "Сидоренко",
        "Л",
        "К",
        "Методы машинного обучения",
        "заочная",
        "ФПМ",
        3,
        50,
        "19831212",
        "20160901",
    ),
    (
        "Журков",
        "К",
        None,
        "Методы выпуклой оптимизации",
        "заочная",
        "ФПМ",
        3,
        40,
        "19861116",
        "20150901",
    ),
    (
        "Скворцов",
        "К",
        None,
        "Основы алгоритмизации",
        "заочная",
        "ФПК",
        1,
        30,
        "19780212",
        "20160901",
    ),
    (
        "Скворцов",
        "К",
        None,
        "Основы операционных систем",
        "заочная",
        "ФПК",
        1,
        20,
        "19780212",
        "20160901",
    ),
    (
        "Сидоренко",
        "Л",
        "К",
        "Объектно-ориенторованное программирование",
        "заочная",
        "ФПК",
        1,
        50,
        "19831212",
        "20160901",
    ),
    (
        "Скворцов",
        "К",
        None,
        "Основы алгоритмизации",
        "вечерняя",
        "ФПК",
        1,
        100,
        "19780212",
        "20160901",
    ),
    (
        "Скворцов",
        "К",
        None,
        "Основы операционных систем",
        "вечерняя",
        "ФПК",
        1,
        100,
        "19780212",
        "20160901",
    ),
    (
        "Сидоренко",
        "Л",
        "К",
        "Объектно-ориенторованное программирование",
        "вечерняя",
        "ФПК",
        1,
        200,
        "19831212",
        "20160901",
    ),
    (
        "Кипеня",
        "Д",
        "А",
        "Компонентное программирование",
        "заочная",
        "ФПК",
        2,
        30,
        "19840109",
        "20130901",
    ),
    (
        "Зорков",
        "К",
        "А",
        "Средства визуального программирования",
        "заочная",
        "ФПК",
        2,
        40,
        "19891212",
        "20160901",
    ),
    (
        "Иридова",
        "Т",
        "К",
        "Объектно-ориенторованное программирование",
        "заочная",
        "ФПК",
        1,
        50,
        "19830412",
        "20160901",
    ),
    (
        "Кипеня",
        "Д",
        "А",
        "Компонентное программирование",
        "вечерня",
        "ФПК",
        2,
        130,
        "19840109",
        "20130901",
    ),
    (
        "Зорков",
        "К",
        "А",
        "Средства визуального программирования",
        "вечерняя",
        "ФПК",
        2,
        140,
        "19891212",
        "20160901",
    ),
    (
        "Иридова",
        "Т",
        "К",
        "Объектно-ориенторованное программирование",
        "вечерняя",
        "ФПК",
        2,
        110,
        "19830412",
        "20160901",
    ),
    ("Курт", "П", "Р", "Философия", "заочная", "ФПК", 3, 20, "19780712", "20160901"),
    ("Курт", "П", "Р", "Социология", "заочная", "ФПК", 3, 20, "19780712", "20160901"),
    (
        "Иридова",
        "Т",
        "К",
        "Современные языки программирования",
        "заочная",
        "ФПК",
        3,
        30,
        "19830412",
        "20160901",
    ),
    (
        "Иридова",
        "Т",
        "К",
        "Тестирование программного обеспечения",
        "заочная",
        "ФПК",
        3,
        30,
        "19830412",
        "20160901",
    ),
    ("Курт", "П", "Р", "Философия", "вечерня", "ФПК", 3, 40, "19780712", "20160901"),
    ("Курт", "П", "Р", "Социология", "вечерня", "ФПК", 3, 40, "19780712", "20160901"),
    (
        "Иридова",
        "Т",
        "К",
        "Современные языки программирования",
        "вечерня",
        "ФПК",
        3,
        150,
        "19830412",
        "20160901",
    ),
    (
        "Иридова",
        "Т",
        "К",
        "Тестирование программного обеспечения",
        "вечерня",
        "ФПК",
        3,
        160,
        "19830412",
        "20160901",
    ),
]


Session = sessionmaker(bind=engine)
session = Session()


def insert_stud():
    for stud in stud_data_list:
        student = STUD(
            last_name=stud[0],
            f_name=stud[1],
            s_name=stud[2],
            form=stud[3],
            faculty=stud[4],
            year=stud[5],
            all_h=stud[6],
            inclass_h=stud[7],
            br_date=stud[8],
            in_date=stud[9],
            exm=stud[10],
        )
        session.add(student)
    session.commit()


def insert_teach():
    for teach in teach_data_list:
        teacher = TEACH(
            last_name=teach[0],
            f_name=teach[1],
            s_name=teach[2],
            subj=teach[3],
            form=teach[4],
            faculty=teach[5],
            year=teach[6],
            hours=teach[7],
            br_date=teach[8],
            start_work_date=teach[9],
        )
        session.add(teacher)
    session.commit()


insert_stud()
insert_teach()


def task1():
    sel = select(STUD).where(STUD.last_name.like("%о%") | STUD.last_name.like("%б%"))
    for student in session.scalars(sel):
        print(student.last_name)


def task2():
    sel = select(STUD).where(STUD.last_name.like("К%") & STUD.s_name.is_(None))
    for student in session.scalars(sel):
        print(
            student.last_name,
            student.f_name,
            student.form,
            student.faculty,
            student.all_h,
            student.inclass_h,
            student.br_date,
            student.in_date,
            student.exm,
            sep=", ",
        )


def task3():
    sel = select(STUD).where(func.length(STUD.last_name) >= 8)
    for student in session.scalars(sel):
        print(
            student.last_name,
            student.f_name,
            student.s_name,
            student.form,
            student.faculty,
            student.all_h,
            student.inclass_h,
            student.br_date,
            student.in_date,
            student.exm,
            sep=", ",
        )


def task4():
    sel = select(STUD).where(
        (STUD.last_name.like("%а%")) & (func.length(STUD.last_name) != 7)
    )
    for student in session.scalars(sel):
        print(
            student.last_name,
            student.f_name,
            student.s_name,
            student.form,
            student.faculty,
            student.all_h,
            student.inclass_h,
            student.br_date,
            student.in_date,
            student.exm,
            sep=", ",
        )


def task5():
    sel = select(STUD).where(
        (STUD.faculty == "ФПМ") & (STUD.form == "очная") & (STUD.year.in_([1, 2]))
    )
    for student in session.scalars(sel.order_by(STUD.s_name)):
        print(student.last_name)


def task6():
    sel = select(STUD).where(
        (STUD.faculty == "ФПК") & (STUD.form == "заочная") & (STUD.exm > 6)
    )
    for student in session.scalars(sel.order_by(STUD.exm.desc())):
        print(student.last_name)


def task7():
    sel = select(TEACH).where(TEACH.faculty == "ФПК")
    for teacher in session.scalars(sel.order_by(TEACH.form, TEACH.last_name)):
        print(teacher.last_name)


def task8():
    sel = select(TEACH).where(
        (TEACH.faculty == "ФПМ") & (TEACH.year == 1) & (TEACH.hours > 100)
    )
    for teacher in session.scalars(sel):
        print(teacher.last_name)


def task9():
    today = date.today()
    min_year = today - timedelta(days=3 * 365)
    sel = select(TEACH).where(
        (TEACH.s_name.is_(None)) & (TEACH.start_work_date <= min_year)
    )
    for teacher in session.scalars(sel):
        print(teacher.last_name)


def task10():
    sel = select(TEACH).where(
        (TEACH.faculty == "ФПМ") & (TEACH.year == 1) & (TEACH.hours > 100)
    )
    for teacher in session.scalars(sel):
        print(
            teacher.subj,
            teacher.year,
            teacher.form,
            teacher.hours,
        )


def task11():
    sel = select(TEACH).where((TEACH.faculty == "ФПК") & (TEACH.hours > 100))
    for teacher in session.scalars(sel):
        print(
            teacher.year,
            teacher.form,
            teacher.last_name,
            teacher.f_name,
            teacher.s_name,
            sep=", ",
        )


def task12():
    sel = select(TEACH).where(TEACH.s_name.is_(None))
    for teacher in session.scalars(sel):
        print(
            teacher.faculty,
            teacher.year,
            teacher.form,
            teacher.last_name,
            teacher.f_name,
            teacher.s_name,
            sep=", ",
        )


def task13():
    year_start = date(date.today().year, 1, 1)
    min_year = year_start - timedelta(days=30 * 365)
    sel = select(TEACH).where(TEACH.br_date <= min_year)
    for teacher in session.scalars(sel):
        print(teacher.last_name)


def task14():
    today = date.today()
    min_year = today - timedelta(days=35 * 365)
    max_year = today - timedelta(days=40 * 365)
    sel = select(TEACH).where((TEACH.br_date <= min_year) & (TEACH.br_date >= max_year))
    for teacher in session.scalars(sel.order_by(TEACH.last_name)):
        print(teacher.last_name)
        print(teacher.br_date)


def task15():
    sel = select(TEACH).where(extract("MONTH", TEACH.br_date) == 10)
    for teacher in session.scalars(sel.order_by(TEACH.br_date)):
        print(teacher.last_name)


task1()
task2()
task3()
task4()
task5()
task6()
task7()
task8()
task9()
task10()
task11()
task12()
task13()
task14()
task15()


session.close()
engine.dispose()
