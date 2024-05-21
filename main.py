from sqlalchemy import create_engine, Column, Integer, String, Sequence, Date
from sqlalchemy import or_, and_, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import text

db_user = "postgres"
db_password = "shtormbreuker"

db_url = f'postgresql+psycopg2://{db_user}:{db_password}@localhost:5432/people'
engine = create_engine(db_url)

Base = declarative_base()


class Person(Base):
    __tablename__ = 'person'
    id = Column(Integer, Sequence('person_id_seq'), primary_key=True)
    first_name = Column(String(20))
    last_name = Column(String(20))
    city = Column(String(50))
    country = Column(String(20))
    birth_date = Column(Date)


Base.metadata.create_all(bind=engine)

Session = sessionmaker(bind=engine)
session = Session()

# person1 = Person(first_name = 'John', last_name = 'Smith', city = 'Lutsk', country = 'Ukraine', birth_date = '01-01-2000')
#
# person2 = Person(first_name = 'Mary', last_name = 'Smith', city = 'Lutsk', country = 'Ukraine', birth_date = '01-01-2000')

# session.add_all([person1, person2])
# session.commit()

while True:
    print("введіть команду")
    print("виконати запит 1")
    print("вивести всіх людей 2")
    print("вивести людей з певного міста 3")
    print("вивести всіх людейз певного міста або країни 4")
    print("вивести імена по літері 5")
    print("добавити нову людину 6")
    print("поміняти місто проживання людини 7")
    print("видалити запис 8")

    command = input('номер команди')

    if command == 'exit':
        break

    if command == '1':
        user_query = input('введіть запит')

        result = session.execute(text(user_query))
        print(result)

        for person in result:
            print(person)
    elif command == '2':
        result = session.query(Person).all()

        for person in result:
            print(person.first_name, person.last_name)

    elif command == '3':
        city = input('введіть назву міста')

        # result = session.query(Person).filter_by(city=city).all()
        result = session.query(Person).filter(Person.city == city).all()

        for person in result:
            print(person.first_name, person.last_name)

    elif command == '4':
        city = input('введіть назву міста')
        country = input('введіть назву країни')

        result = session.query(Person).filter(or_(Person.city == city, Person.country == country)).all()

        for person in result:
            print(person.first_name, person.last_name)

    elif command == '5':
        letter = input('введіть літеру')

        result = session.query(Person).filter(
            Person.first_name.like(f'{letter}%')
        ).all()

        for person in result:
            print(person.first_name, person.last_name)

    elif command == '6':
        person = Person(first_name=input("введіть ім'я"),
                        last_name=input("введіть прізвище"),
                        city=input("введіть місто"),
                        country=input("введіть країну"),
                        birth_date=input("введіть дату народження"))

        session.add(person)
        session.commit()

    elif command == '7':
        first_name = input("Введіть ім`я")

        person = session.query(Person).filter(
            Person.first_name == first_name
        ).first()

        person.city = input("введіть нове місто")

        session.commit()

    elif command == '8':
        first_name = input("Введіть ім`я")

        person = session.query(Person).filter(
            Person.first_name == first_name
        ).first()

        session.delete(person)
        session.commit()


session.close()