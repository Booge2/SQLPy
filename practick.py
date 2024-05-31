from sqlalchemy import create_engine, Column, Integer, String, Sequence, Date, ForeignKey, Numeric, extract, inspect
from sqlalchemy import or_, and_, func
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import text
from datetime import datetime

db_user = "postgres"
db_password = "shtormbreuker"

db_url = f'postgresql+psycopg2://{db_user}:{db_password}@localhost:5432/hospital'
engine = create_engine(db_url)

Base = declarative_base()


class Department(Base):
    __tablename__ = 'departments'
    id = Column(Integer, primary_key=True)
    building = Column(String, nullable=False)
    financing = Column(String, nullable=False)
    name = Column(String, nullable=False)


class Disease(Base):
    __tablename__ = 'diseases'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    severity = Column(String, nullable=False)


class Doctor(Base):
    __tablename__ = 'doctors'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    phone = Column(String, nullable=False)
    salary = Column(Numeric, nullable=False)
    surname = Column(String, nullable=False)
    premium = Column(Numeric, nullable=False)


class DoctorSpecialization(Base):
    __tablename__ = 'doctorsspecializations'
    id = Column(Integer, primary_key=True)
    doctorid = Column(Integer, ForeignKey('doctors.id'), nullable=False)
    specializationid = Column(Integer, ForeignKey('specializations.id'), nullable=False)


class Donation(Base):
    __tablename__ = 'donations'
    id = Column(Integer, primary_key=True)
    amount = Column(Numeric, nullable=False)
    date = Column(Date, nullable=False)
    departmentid = Column(Integer, ForeignKey('departments.id'), nullable=False)
    sponsorid = Column(Integer, ForeignKey('sponsors.id'), nullable=False)


class Examination(Base):
    __tablename__ = 'examinations'
    id = Column(Integer, primary_key=True)
    dayofweek = Column(String, nullable=False)
    andtime = Column(String, nullable=False)
    name = Column(String, nullable=False)
    starttime = Column(String, nullable=False)
    date = Column(Date, nullable=False)


class Specialization(Base):
    __tablename__ = 'specializations'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)


class Sponsor(Base):
    __tablename__ = 'sponsors'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)


class Vacation(Base):
    __tablename__ = 'vacations'
    id = Column(Integer, primary_key=True)
    startdate = Column(Date, nullable=False)
    enddate = Column(Date, nullable=False)
    doctorid = Column(Integer, ForeignKey('doctors.id'), nullable=False)


class Ward(Base):
    __tablename__ = 'wards'
    id = Column(Integer, primary_key=True)
    building = Column(String, nullable=False)
    floor = Column(String, nullable=False)
    name = Column(String, nullable=False)
    departmentid = Column(Integer, ForeignKey('departments.id'), nullable=False)


Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


#
#
# def insert_row(table_class, **kwargs):
#     new_row = table_class(**kwargs)
#     session.add(new_row)
#     session.commit()
#     print(f"Row added to {table_class.__tablename__} successfully.")
#
#
# def update_row(table_class, row_id, **kwargs):
#     row = session.query(table_class).get(row_id)
#     if row:
#         for key, value in kwargs.items():
#             setattr(row, key, value)
#         session.commit()
#         print(f"Row ID {row_id} in {table_class.__tablename__} updated successfully.")
#     else:
#         print(f"Row ID {row_id} not found in {table_class.__tablename__}.")
#
#
# def delete_row(table_class, row_id):
#     row = session.query(table_class).get(row_id)
#     if row:
#         session.delete(row)
#         session.commit()
#         print(f"Row ID {row_id} in {table_class.__tablename__} deleted successfully.")
#     else:
#         print(f"Row ID {row_id} not found in {table_class.__tablename__}.")
#
#
# def update_all_rows(table_class, **kwargs):
#     confirmation = input(f"Are you sure you want to update all rows in {table_class.__tablename__}? (yes/no): ")
#     if confirmation.lower() == 'yes':
#         rows = session.query(table_class).all()
#         for row in rows:
#             for key, value in kwargs.items():
#                 setattr(row, key, value)
#         session.commit()
#         print(f"All rows in {table_class.__tablename__} updated successfully.")
#     else:
#         print("Operation canceled.")
#
#
# def delete_all_rows(table_class):
#     confirmation = input(f"Are you sure you want to delete all rows in {table_class.__tablename__}? (yes/no): ")
#     if confirmation.lower() == 'yes':
#         session.query(table_class).delete()
#         session.commit()
#         print(f"All rows in {table_class.__tablename__} deleted successfully.")
#     else:
#         print("Operation canceled.")
#
#
# def view_rows(table_class):
#     rows = session.query(table_class).all()
#     if rows:
#         for row in rows:
#             print(row.__dict__)
#     else:
#         print(f"No rows found in {table_class.__tablename__}.")
#
#
#
# def main():
#     tables = {
#         "departments": Department,
#         "diseases": Disease,
#         "doctors": Doctor,
#         "doctorsspecializations": DoctorSpecialization,
#         "donations": Donation,
#         "examinations": Examination,
#         "specializations": Specialization,
#         "sponsors": Sponsor,
#         "vacations": Vacation,
#         "wards": Ward,
#     }


#
#     while True:
#         print("\nМеню:")
#         print("1. Додати рядок")
#         print("2. Оновити рядок")
#         print("3. Видалити рядок")
#         print("4. Оновити всі рядки")
#         print("5. Видалити всі рядки")
#         print("6. Переглянути всі рядки")
#         print("0. Вихід")
#         choice = input("Виберіть опцію: ")
#
#         if choice == '1':
#             table_name = input("Введіть назву таблиці: ").lower()
#             table_class = tables.get(table_name)
#             if table_class:
#                 kwargs = {}
#                 for column in table_class.__table__.columns.keys():
#                     if column != 'id':
#                         value = input(f"Введіть значення для {column}: ")
#                         kwargs[column] = value
#                 insert_row(table_class, **kwargs)
#             else:
#                 print("Неправильна назва таблиці.")
#
#         elif choice == '2':
#             table_name = input("Введіть назву таблиці: ").lower()
#             table_class = tables.get(table_name)
#             if table_class:
#                 row_id = int(input("Введіть ID рядка для оновлення: "))
#                 kwargs = {}
#                 for column in table_class.__table__.columns.keys():
#                     if column != 'id':
#                         value = input(f"Введіть нове значення для {column} (або залиште порожнім): ")
#                         if value:
#                             kwargs[column] = value
#                 update_row(table_class, row_id, **kwargs)
#             else:
#                 print("Неправильна назва таблиці.")
#
#         elif choice == '3':
#             table_name = input("Введіть назву таблиці: ").lower()
#             table_class = tables.get(table_name)
#             if table_class:
#                 row_id = int(input("Введіть ID рядка для видалення: "))
#                 delete_row(table_class, row_id)
#             else:
#                 print("Неправильна назва таблиці.")
#
#         elif choice == '4':
#             table_name = input("Введіть назву таблиці: ").lower()
#             table_class = tables.get(table_name)
#             if table_class:
#                 kwargs = {}
#                 for column in table_class.__table__.columns.keys():
#                     if column != 'id':
#                         value = input(f"Введіть нове значення для {column} (або залиште порожнім): ")
#                         if value:
#                             kwargs[column] = value
#                 update_all_rows(table_class, **kwargs)
#             else:
#                 print("Неправильна назва таблиці.")
#
#         elif choice == '5':
#             table_name = input("Введіть назву таблиці: ").lower()
#             table_class = tables.get(table_name)
#             if table_class:
#                 delete_all_rows(table_class)
#             else:
#                 print("Неправильна назва таблиці.")
#
#         elif choice == '6':
#             table_name = input("Введіть назву таблиці: ").lower()
#             table_class = tables.get(table_name)
#             if table_class:
#                 view_rows(table_class)
#             else:
#                 print("Неправильна назва таблиці.")
#
#         elif choice == '0':
#             break
#         else:
#             print("Невірний вибір. Спробуйте ще раз.")
#
#
# if __name__ == "__main__":
#     main()


# Завдання 2

# def doctors_and_specializations(session):
#     query = session.query(Doctor.surname, Specialization.name)\
#                    .join(DoctorSpecialization, Doctor.id == DoctorSpecialization.doctorid)\
#                    .join(Specialization, Specialization.id == DoctorSpecialization.specializationid)\
#                    .all()
#     for doctor, specialization in query:
#         print(f"{doctor}: {specialization}")
#
# def doctors_salaries_not_on_vacation(session):
#     query = session.query(Doctor.surname, (Doctor.salary + Doctor.premium).label('total_salary'))\
#                    .filter(~Doctor.id.in_(session.query(Vacation.doctorid)))\
#                    .all()
#     for doctor, salary in query:
#         print(f"{doctor}: {salary}")
#
# def wards_in_department(session, department_name):
#     query = session.query(Ward.name)\
#                    .join(Department, Ward.departmentid == Department.id)\
#                    .filter(Department.name == department_name)\
#                    .all()
#     for ward in query:
#         print(ward.name)
#
# def donations_for_month(session, month):
#     query = session.query(Department.name, Sponsor.name, Donation.amount, Donation.date)\
#                    .join(Sponsor, Donation.sponsorid == Sponsor.id)\
#                    .join(Department, Donation.departmentid == Department.id)\
#                    .filter(extract('month', Donation.date) == month)\
#                    .all()
#     for department, sponsor, amount, date in query:
#         print(f"Відділення: {department}, Спонсор: {sponsor}, Сума: {amount}, Дата: {date}")
#
# def sponsored_departments(session, company_name):
#     query = session.query(Department.name)\
#                    .join(Donation, Department.id == Donation.departmentid)\
#                    .join(Sponsor, Donation.sponsorid == Sponsor.id)\
#                    .filter(Sponsor.name == company_name)\
#                    .distinct()\
#                    .all()
#     for department in query:
#         print(department.name)
#
# doctors_and_specializations(session)
# doctors_salaries_not_on_vacation(session)
# wards_in_department(session, 'Department D')
# donations_for_month(session, 5)
# sponsored_departments(session, 'Global Health Initiative')
#
# session.close()
# Завдання 3

# Функція відображення назв усіх таблиць
def display_all_tables():
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print("Назви усіх таблиць:")
    for table in tables:
        print(table)


# Функція відображення назв стовпців для певної таблиці
def display_table_columns(table_name):
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    print(f"Назви стовпців для таблиці '{table_name}':")
    for column in columns:
        print(column['name'])


# Функція відображення назв стовпців та їх типів для певної таблиці
def display_table_columns_and_types(table_name):
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    print(f"Назви стовпців та їх типи для таблиці '{table_name}':")
    for column in columns:
        print(column['name'], column['type'])


# Функція відображення зв’язків між таблицями
def display_relationships():
    inspector = inspect(engine)
    relationships = inspector.get_foreign_keys('doctors')
    print("Зв'язки між таблицями:")
    for relationship in relationships:
        print(relationship)


# Функція створення нової таблиці
def create_table(table_name, columns):
    table = Table(table_name, Base.metadata)
    for column_name, column_type in columns.items():
        table.append_column(Column(column_name, column_type))
        table.create(engine, checkfirst=True)


# Функція видалення таблиці

def drop_table(table_name):
    table = Table(table_name, Base.metadata)
    table.drop(engine, checkfirst=True)


# Функція додавання стовпця до таблиці
def add_column(table_name, column_name, column_type):
    table = Table(table_name, Base.metadata, autoload=True, autoload_with=engine)
    column = Column(column_name, column_type)
    column.create(table, populate_default=True)


# Функція оновлення типу стовпця
def update_column_type(table_name, column_name, new_column_type):
    table = Table(table_name, Base.metadata, autoload=True, autoload_with=engine)
    column = table.columns[column_name]
    column.type = new_column_type
    table.c[column_name].alter(type=new_column_type)


# Функція видалення стовпця з таблиці
def drop_column(table_name, column_name):
    table = Table(table_name, Base.metadata, autoload=True, autoload_with=engine)
    column = table.columns[column_name]
    column.drop()


def main():
    print("1. Відображення назв усіх таблиць")
    print("2. Відображення назв стовпців певної таблиці")
    print("3. Відображення назв стовпців та їх типів для певної таблиці")
    print("4. Відображення зв’язків між таблицями")
    print("5. Створення таблиці")
    print("6. Видалення таблиці")
    print("7. Додавання стовпця до таблиці")
    print("8. Оновлення типу стовпця")
    print("9. Видалення стовпця з таблиці")

    choice = input("Виберіть опцію: ")

    if choice == '1':
        display_all_tables()
    elif choice == '2':
        table_name = input("Введіть назву таблиці: ")
        display_table_columns(table_name)
    elif choice == '3':
        table_name = input("Введіть назву таблиці: ")
        display_table_columns_and_types(table_name)
    elif choice == '4':
        display_relationships()
    elif choice == '5':
        table_name = input("Введіть назву таблиці: ")
        columns = {"id": Integer, "name": String}
        create_table(table_name, columns)
    elif choice == '6':
        table_name = input("Введіть назву таблиці для видалення: ")
        drop_table(table_name)
    elif choice == '7':
        table_name = input("Введіть назву таблиці: ")
        column_name = input("Введіть назву стовпця: ")
        column_type = input("Введіть тип стовпця: ")
        # convert column_type to actual type (e.g., Integer, String)
        add_column(table_name, column_name, column_type)
    elif choice == '8':
        table_name = input("Введіть назву таблиці: ")
        column_name = input("Введіть назву стовпця: ")
        new_column_type = input("Введіть новий тип стовпця: ")
        # convert new_column_type to actual type (e.g., Integer, String)
        update_column_type(table_name, column_name, new_column_type)
    elif choice == '9':
        table_name = input("Введіть назву таблиці: ")
        column_name = input("Введіть назву стовпця для видалення: ")
        drop_column(table_name, column_name)
    else:
        print("Невірний вибір. Спробуйте ще раз.")


if __name__ == "__main__":
    main()
