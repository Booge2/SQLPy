from sqlalchemy import create_engine, Column, Integer, String, Sequence, Date, ForeignKey, Numeric
from sqlalchemy import or_, and_, func
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import text
from datetime import datetime

db_user = "postgres"
db_password = "shtormbreuker"

db_url = f'postgresql+psycopg2://{db_user}:{db_password}@localhost:5432/Sales'
engine = create_engine(db_url)

Base = declarative_base()


class Salesman(Base):
    __tablename__ = 'salesmen'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)


class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)


class Sale(Base):
    __tablename__ = 'sales'
    id = Column(Integer, primary_key=True)
    amount = Column(Numeric(10, 2), nullable=False)
    sale_date = Column(Date, nullable=False)
    salesman_id = Column(Integer, ForeignKey('salesmen.id'), nullable=False)
    customer_id = Column(Integer, ForeignKey('customers.id'), nullable=False)

    salesman = relationship("Salesman", back_populates="sales")
    customer = relationship("Customer", back_populates="sales")


Salesman.sales = relationship("Sale", order_by=Sale.id, back_populates="salesman")
Customer.sales = relationship("Sale", order_by=Sale.id, back_populates="customer")

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def add_sample_data():
    salesman1 = Salesman(name='John Doe')
    salesman2 = Salesman(name='Jane Smith')
    customer1 = Customer(name='Customer A')
    customer2 = Customer(name='Customer B')

    sale1 = Sale(amount=500.0, sale_date=datetime.strptime('2024-05-28', '%Y-%m-%d'), salesman=salesman1,
                 customer=customer1)
    sale2 = Sale(amount=1000.0, sale_date=datetime.strptime('2024-05-28', '%Y-%m-%d'), salesman=salesman2,
                 customer=customer2)
    sale3 = Sale(amount=200.0, sale_date=datetime.strptime('2024-05-28', '%Y-%m-%d'), salesman=salesman1,
                 customer=customer2)

    session.add_all([salesman1, salesman2, customer1, customer2, sale1, sale2, sale3])
    session.commit()


# add_sample_data()

def add_salesman(name):
    new_salesman = Salesman(name=name)
    session.add(new_salesman)
    session.commit()


def add_customer(name):
    new_customer = Customer(name=name)
    session.add(new_customer)
    session.commit()


def add_sale(amount, sale_date, salesman_id, customer_id):
    new_sale = Sale(amount=amount, sale_date=sale_date, salesman_id=salesman_id, customer_id=customer_id)
    session.add(new_sale)
    session.commit()


def update_salesman(salesman_id, new_name):
    salesman = session.query(Salesman).filter(Salesman.id == salesman_id).first()
    if salesman:
        salesman.name = new_name
        session.commit()


def update_customer(customer_id, new_name):
    customer = session.query(Customer).filter(Customer.id == customer_id).first()
    if customer:
        customer.name = new_name
        session.commit()


def update_sale(sale_id, new_amount=None, new_date=None, new_salesman_id=None, new_customer_id=None):
    sale = session.query(Sale).filter(Sale.id == sale_id).first()
    if sale:
        if new_amount:
            sale.amount = new_amount
        if new_date:
            sale.sale_date = new_date
        if new_salesman_id:
            sale.salesman_id = new_salesman_id
        if new_customer_id:
            sale.customer_id = new_customer_id
        session.commit()


def delete_salesman(salesman_id):
    salesman = session.query(Salesman).filter(Salesman.id == salesman_id).first()
    if salesman:
        session.delete(salesman)
        session.commit()


def delete_customer(customer_id):
    customer = session.query(Customer).filter(Customer.id == customer_id).first()
    if customer:
        session.delete(customer)
        session.commit()


def delete_sale(sale_id):
    sale = session.query(Sale).filter(Sale.id == sale_id).first()
    if sale:
        session.delete(sale)
        session.commit()


def get_all_sales():
    return session.query(Sale).all()


def get_sales_by_salesman(salesman_id):
    return session.query(Sale).filter(Sale.salesman_id == salesman_id).all()


def get_max_sale():
    return session.query(Sale).order_by(Sale.amount.desc()).first()


def get_min_sale():
    return session.query(Sale).order_by(Sale.amount.asc()).first()


def get_max_sale_by_salesman(salesman_id):
    return session.query(Sale).filter(Sale.salesman_id == salesman_id).order_by(Sale.amount.desc()).first()


def get_min_sale_by_salesman(salesman_id):
    return session.query(Sale).filter(Sale.salesman_id == salesman_id).order_by(Sale.amount.asc()).first()


def get_max_sale_by_customer(customer_id):
    return session.query(Sale).filter(Sale.customer_id == customer_id).order_by(Sale.amount.desc()).first()


def get_min_sale_by_customer(customer_id):
    return session.query(Sale).filter(Sale.customer_id == customer_id).order_by(Sale.amount.asc()).first()


def get_salesman_with_max_sales():
    return session.query(Salesman).join(Sale).group_by(Salesman.id).order_by(func.sum(Sale.amount).desc()).first()


def get_salesman_with_min_sales():
    return session.query(Salesman).join(Sale).group_by(Salesman.id).order_by(func.sum(Sale.amount).asc()).first()


def get_customer_with_max_purchases():
    return session.query(Customer).join(Sale).group_by(Customer.id).order_by(func.sum(Sale.amount).desc()).first()


def get_avg_purchase_by_customer(customer_id):
    return session.query(func.avg(Sale.amount)).filter(Sale.customer_id == customer_id).scalar()


def get_avg_sale_by_salesman(salesman_id):
    return session.query(func.avg(Sale.amount)).filter(Sale.salesman_id == salesman_id).scalar()


def print_menu():
    print("\nМеню:")
    print("1. Відображення усіх угод")
    print("2. Відображення угод конкретного продавця")
    print("3. Відображення максимальної за сумою угоди")
    print("4. Відображення мінімальної за сумою угоди")
    print("5. Відображення максимальної суми угоди для конкретного продавця")
    print("6. Відображення мінімальної за сумою угоди для конкретного продавця")
    print("7. Відображення максимальної за сумою угоди для конкретного покупця")
    print("8. Відображення мінімальної за сумою угоди для конкретного покупця")
    print("9. Відображення продавця з максимальною сумою продажів за всіма угодами")
    print("10. Відображення продавця з мінімальною сумою продажів за всіма угодами")
    print("11. Відображення покупця з максимальною сумою покупок за всіма угодами")
    print("12. Відображення середньої суми покупки для конкретного покупця")
    print("13. Відображення середньої суми покупки для конкретного продавця")
    print("14. Додати нового продавця")
    print("15. Додати нового покупця")
    print("16. Додати нову угоду")
    print("17. Оновити дані продавця")
    print("18. Оновити дані покупця")
    print("19. Оновити дані угоди")
    print("20. Видалити продавця")
    print("21. Видалити покупця")
    print("22. Видалити угоду")
    print("0. Вихід")


def main():
    while True:
        print_menu()
        choice = input("Виберіть опцію: ")

        if choice == '1':
            sales = get_all_sales()
            for sale in sales:
                print(sale.id, sale.amount, sale.sale_date, sale.salesman.name, sale.customer.name)

        elif choice == '2':
            salesman_id = int(input("Введіть ідентифікатор продавця: "))
            sales = get_sales_by_salesman(salesman_id)
            for sale in sales:
                print(sale.id, sale.amount, sale.sale_date, sale.salesman.name, sale.customer.name)

        elif choice == '3':
            sale = get_max_sale()
            if sale:
                print(sale.id, sale.amount, sale.sale_date, sale.salesman.name, sale.customer.name)

        elif choice == '4':
            sale = get_min_sale()
            if sale:
                print(sale.id, sale.amount, sale.sale_date, sale.salesman.name, sale.customer.name)

        elif choice == '5':
            salesman_id = int(input("Введіть ідентифікатор продавця: "))
            sale = get_max_sale_by_salesman(salesman_id)
            if sale:
                print(sale.id, sale.amount, sale.sale_date, sale.salesman.name, sale.customer.name)

        elif choice == '6':
            salesman_id = int(input("Введіть ідентифікатор продавця: "))
            sale = get_min_sale_by_salesman(salesman_id)
            if sale:
                print(sale.id, sale.amount, sale.sale_date, sale.salesman.name, sale.customer.name)

        elif choice == '7':
            customer_id = int(input("Введіть ідентифікатор покупця: "))
            sale = get_max_sale_by_customer(customer_id)
            if sale:
                print(sale.id, sale.amount, sale.sale_date, sale.salesman.name, sale.customer.name)

        elif choice == '8':
            customer_id = int(input("Введіть ідентифікатор покупця: "))
            sale = get_min_sale_by_customer(customer_id)
            if sale:
                print(sale.id, sale.amount, sale.sale_date, sale.salesman.name, sale.customer.name)

        elif choice == '9':
            salesman = get_salesman_with_max_sales()
            if salesman:
                total_sales = sum(sale.amount for sale in salesman.sales)
                print(salesman.id, salesman.name, total_sales)

        elif choice == '10':
            salesman = get_salesman_with_min_sales()
            if salesman:
                total_sales = sum(sale.amount for sale in salesman.sales)
                print(salesman.id, salesman.name, total_sales)

        elif choice == '11':
            customer = get_customer_with_max_purchases()
            if customer:
                total_purchases = sum(sale.amount for sale in customer.sales)
                print(customer.id, customer.name, total_purchases)

        elif choice == '12':
            customer_id = int(input("Введіть ідентифікатор покупця: "))
            avg_purchase = get_avg_purchase_by_customer(customer_id)
            print(f"Середня сума покупки: {avg_purchase}")

        elif choice == '13':
            salesman_id = int(input("Введіть ідентифікатор продавця: "))
            avg_sale = get_avg_sale_by_salesman(salesman_id)
            print(f"Середня сума продажу: {avg_sale}")

        elif choice == '14':
            name = input("Введіть ім'я нового продавця: ")
            add_salesman(name)
            print("Новий продавець доданий.")

        elif choice == '15':
            name = input("Введіть ім'я нового покупця: ")
            add_customer(name)
            print("Новий покупець доданий.")

        elif choice == '16':
            amount = float(input("Введіть суму угоди: "))
            sale_date = datetime.strptime(input("Введіть дату угоди (YYYY-MM-DD): "), '%Y-%m-%d')
            salesman_id = int(input("Введіть ідентифікатор продавця: "))
            customer_id = int(input("Введіть ідентифікатор покупця: "))
            add_sale(amount, sale_date, salesman_id, customer_id)
            print("Нова угода додана.")

        elif choice == '17':
            salesman_id = int(input("Введіть ідентифікатор продавця: "))
            new_name = input("Введіть нове ім'я продавця: ")
            update_salesman(salesman_id, new_name)
            print("Ім'я продавця оновлено.")

        elif choice == '18':
            customer_id = int(input("Введіть ідентифікатор покупця: "))
            new_name = input("Введіть нове ім'я покупця: ")
            update_customer(customer_id, new_name)
            print("Ім'я покупця оновлено.")

        elif choice == '19':
            sale_id = int(input("Введіть ідентифікатор угоди: "))
            new_amount = float(input("Введіть нову суму угоди (або залиште порожнім): ") or 0)
            new_date = input("Введіть нову дату угоди (YYYY-MM-DD) (або залиште порожнім): ")
            new_date = datetime.strptime(new_date, '%Y-%m-%d') if new_date else None
            new_salesman_id = int(input("Введіть новий ідентифікатор продавця (або залиште порожнім): ") or 0)
            new_customer_id = int(input("Введіть новий ідентифікатор покупця (або залиште порожнім): ") or 0)
            update_sale(sale_id, new_amount or None, new_date, new_salesman_id or None, new_customer_id or None)
            print("Дані угоди оновлено.")

        elif choice == '20':
            salesman_id = int(input("Введіть ідентифікатор продавця для видалення: "))
            delete_salesman(salesman_id)
            print("Продавця видалено.")

        elif choice == '21':
            customer_id = int(input("Введіть ідентифікатор покупця для видалення: "))
            delete_customer(customer_id)
            print("Покупця видалено.")

        elif choice == '22':
            sale_id = int(input("Введіть ідентифікатор угоди для видалення: "))
            delete_sale(sale_id)
            print("Угоду видалено.")

        elif choice == '0':
            break

        else:
            print("Невірний вибір. Спробуйте ще раз.")


if __name__ == "__main__":
    main()
