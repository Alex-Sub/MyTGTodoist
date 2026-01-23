from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import Project


def seed_projects(session: Session) -> None:
    desired = [
        {"name": "Inbox", "is_system": True, "sort_order": 0},
        {"name": "Личные", "sort_order": 10},
        {"name": "Инфомед", "sort_order": 20},
        {"name": "Проекты", "sort_order": 30},
        {"name": "Подумать", "sort_order": 40},
        {"name": "Обо всем", "sort_order": 50},
    ]

    existing = set(session.scalars(select(Project.name)).all())
    created = False
    for project in desired:
        if project["name"] in existing:
            continue
        session.add(Project(**project))
        created = True

    if created:
        session.commit()
