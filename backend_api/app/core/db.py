
from sqlmodel import Session, create_engine, SQLModel
from app.core.config import settings

# Create engine
# pool_pre_ping=True verifies the connection before usage
engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)

def get_session():
    with Session(engine) as session:
        yield session

def init_db():
    # Tables should be created via SQL scripts, but this is kept for reference
    # or optional dev-mode auto-creation if needed.
    pass
