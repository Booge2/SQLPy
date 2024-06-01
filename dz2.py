from sqlalchemy import create_engine, Column, Integer, String, Sequence, Date, ForeignKey, REAL, extract, inspect, \
    Boolean, REAL, Numeric
from sqlalchemy import or_, and_, func
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import text

# from datetime import datetime

db_user = "postgres"
db_password = "shtormbreuker"

db_url = f'postgresql+psycopg2://{db_user}:{db_password}@localhost:5432/academy'
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


# Визначення моделей таблиць
class Curator(Base):
    __tablename__ = 'curators'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    surname = Column(String, nullable=False)
    groups = relationship('GroupsCurators', back_populates='curator')


class Department(Base):
    __tablename__ = 'departments'
    id = Column(Integer, primary_key=True)
    financing = Column(Numeric, nullable=False)
    name = Column(String, nullable=False)
    facultyid = Column(Integer, ForeignKey('faculties.id'), nullable=False)
    faculty = relationship('Faculty', back_populates='departments')
    groups = relationship('Group', back_populates='department')


class Faculty(Base):
    __tablename__ = 'faculties'
    id = Column(Integer, primary_key=True)
    dean = Column(String, nullable=False)
    name = Column(String, nullable=False)
    financing = Column(Numeric, nullable=False)
    departments = relationship('Department', back_populates='faculty')


class Group(Base):
    __tablename__ = 'groups'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    rating = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    departmentid = Column(Integer, ForeignKey('departments.id'), nullable=False)
    department = relationship('Department', back_populates='groups')
    curators = relationship('GroupsCurators', back_populates='group')
    lectures = relationship('GroupsLectures', back_populates='group')


class GroupsCurators(Base):
    __tablename__ = 'groupscurators'
    id = Column(Integer, primary_key=True)
    curatorid = Column(Integer, ForeignKey('curators.id'), nullable=False)
    groupid = Column(Integer, ForeignKey('groups.id'), nullable=False)
    curator = relationship('Curator', back_populates='groups')
    group = relationship('Group', back_populates='curators')


class GroupsLectures(Base):
    __tablename__ = 'groupslectures'
    id = Column(Integer, primary_key=True)
    groupid = Column(Integer, ForeignKey('groups.id'), nullable=False)
    lectureid = Column(Integer, ForeignKey('lectures.id'), nullable=False)
    group = relationship('Group', back_populates='lectures')
    lecture = relationship('Lecture', back_populates='groups')


class Lecture(Base):
    __tablename__ = 'lectures'
    id = Column(Integer, primary_key=True)
    lectureroom = Column(String, nullable=False)
    subjectid = Column(Integer, ForeignKey('subjects.id'), nullable=False)
    teacherid = Column(Integer, ForeignKey('teachers.id'), nullable=False)
    subject = relationship('Subject', back_populates='lectures')
    teacher = relationship('Teacher', back_populates='lectures')
    groups = relationship('GroupsLectures', back_populates='lecture')


class Subject(Base):
    __tablename__ = 'subjects'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    lectures = relationship('Lecture', back_populates='subject')


class Teacher(Base):
    __tablename__ = 'teachers'
    id = Column(Integer, primary_key=True)
    employmentdate = Column(Date, nullable=False)
    isassistant = Column(Boolean, nullable=False)
    isprofessor = Column(Boolean, nullable=False)
    name = Column(String, nullable=False)
    position = Column(String, nullable=False)
    premium = Column(Numeric, nullable=False)
    salary = Column(Numeric, nullable=False)
    surname = Column(String, nullable=False)
    lectures = relationship('Lecture', back_populates='teacher')


# Функції для вставки, оновлення, видалення рядків
def insert_row(instance):
    session.add(instance)
    session.commit()


def update_row(model, id, updates):
    session.query(model).filter_by(id=id).update(updates)
    session.commit()


def delete_row(model, id):
    row = session.query(model).get(id)
    if row:
        session.delete(row)
        session.commit()
        print(f"Row ID {id} in {model.__tablename__} deleted successfully.\n")
    else:
        print(f"Row ID {id} not found in {model.__tablename__}.\n")



# Функції для створення звітів
def report_all_groups():
    groups = session.query(Group).all()
    for group in groups:
        print(f"Group ID: {group.id}, Name: {group.name}, Rating: {group.rating}, Year: {group.year}, Department ID: {group.departmentid}")
    print()

def report_all_teachers():
    teachers = session.query(Teacher).all()
    for teacher in teachers:
        print(f"Teacher ID: {teacher.id}, Name: {teacher.name}, Surname: {teacher.surname}, Position: {teacher.position}")
    print()


def report_all_departments():
    departments = session.query(Department).all()
    for department in departments:
        print(f"Department ID: {department.id}, Name: {department.name}, Financing: {department.financing}")
    print()


def report_teachers_in_group(group_id):
    teachers = session.query(Teacher).join(Lecture, Teacher.id == Lecture.teacherid).join(GroupsLectures, Lecture.id == GroupsLectures.lectureid).filter(
        GroupsLectures.groupid == group_id).all()
    for teacher in teachers:
        print(f"Teacher ID: {teacher.id}, Name: {teacher.name}, Surname: {teacher.surname}, Position: {teacher.position}")
    print()


def report_departments_and_groups():
    department_groups = session.query(Department.name, Group.name).join(Group, Department.id == Group.departmentid).all()
    for department, group in department_groups:
        print(f"Department: {department}, Group: {group}")
    print()

def report_department_with_max_groups():
    department = session.query(Department.name).join(Group, Department.id == Group.departmentid).group_by(
        Department.name).order_by(func.count(Group.id).desc()).limit(1).first()
    print(f"Department with most groups: {department[0]}\n")



def report_department_with_min_groups():
    department = session.query(Department.name).join(Group, Department.id == Group.departmentid).group_by(
        Department.name).order_by(func.count(Group.id).asc()).limit(1).first()
    print(f"Department with least groups: {department[0]}\n")



def report_subjects_by_teacher(teacher_id):
    subjects = session.query(Subject.name).join(Lecture, Subject.id == Lecture.subjectid).filter(
        Lecture.teacherid == teacher_id).all()
    for subject in subjects:
        print(f"Subject: {subject.name}")
    print()


def report_departments_by_subject(subject_id):
    departments = session.query(Department.name).join(Group, Department.id == Group.departmentid).join(GroupsLectures, Group.id == GroupsLectures.groupid).join(
        Lecture, GroupsLectures.lectureid == Lecture.id).filter(Lecture.subjectid == subject_id).all()
    for department in departments:
        print(f"Department: {department.name}")
    print()

def report_groups_by_faculty(faculty_id):
    groups = session.query(Group.name).join(Department, Group.departmentid == Department.id).filter(
        Department.facultyid == faculty_id).all()
    for group in groups:
        print(f"Group: {group.name}")
    print()


def report_subjects_and_teachers():
    subjects_teachers = session.query(Subject.name, Teacher.name, Teacher.surname).join(Lecture, Subject.id == Lecture.subjectid).join(
        Teacher, Lecture.teacherid == Teacher.id).group_by(Subject.name, Teacher.name, Teacher.surname).order_by(
        func.count(Lecture.id).desc()).all()
    for subject, teacher_name, teacher_surname in subjects_teachers:
        print(f"Subject: {subject}, Teacher: {teacher_name} {teacher_surname}")
    print()


def report_subject_with_least_lectures():
    subject = session.query(Subject.name).join(Lecture, Subject.id == Lecture.subjectid).group_by(Subject.name).order_by(
        func.count(Lecture.id).asc()).limit(1).first()
    print(f"Subject with least lectures: {subject[0]}\n")



def report_subject_with_most_lectures():
    subject = session.query(Subject.name).join(Lecture, Subject.id == Lecture.subjectid).group_by(Subject.name).order_by(
        func.count(Lecture.id).desc()).limit(1).first()
    print(f"Subject with most lectures: {subject[0]}\n")



if __name__ == "__main__":
    new_curator = Curator(name='John', surname='Doe')
    insert_row(new_curator)

    update_row(Curator, 2, {'surname': 'Smith'})

    delete_row(Curator, 15)

    print(report_all_groups())
    print(report_all_teachers())
    print(report_all_departments())
    print(report_teachers_in_group(1))
    print(report_departments_and_groups())
    print(report_department_with_max_groups())
    print(report_department_with_min_groups())
    print(report_subjects_by_teacher(1))
    print(report_departments_by_subject(1))
    print(report_groups_by_faculty(1))
    print(report_subjects_and_teachers())
    print(report_subject_with_least_lectures())
    print(report_subject_with_most_lectures())

    session.close()