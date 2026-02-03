from sqlmodel import Session, select
from api.db import engine, create_db_and_tables
from api.models import User
from api.utils import get_password_hash

def init_db():
    create_db_and_tables()

    with Session(engine) as session:
        # Check if admin exists
        user = session.exec(select(User).where(User.email == "admin@example.com")).first()
        if not user:
            print("Creating Admin user...")
            admin_user = User(
                email="admin@example.com",
                hashed_password=get_password_hash("password"), # standard default
                role="admin",
                is_active=True
            )
            session.add(admin_user)
            session.commit()
            print("Admin user created: admin@example.com / password")
        else:
            print("Admin user already exists.")

if __name__ == "__main__":
    init_db()
