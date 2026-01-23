from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import Project


def get_or_create_by_name(session: Session, name: str, **defaults) -> Project:
    project = session.scalar(select(Project).where(Project.name == name))
    if project:
        return project

    project = Project(name=name, **defaults)
    session.add(project)
    session.commit()
    session.refresh(project)
    return project


def list_projects(session: Session) -> list[Project]:
    return list(session.scalars(select(Project).order_by(Project.sort_order, Project.name)).all())
