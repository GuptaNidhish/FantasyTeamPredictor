from db.initialization import engine, Base
from db import models

Base.metadata.create_all(bind=engine)

print("✅ Tables created successfully!")