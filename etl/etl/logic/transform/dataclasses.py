from pydantic import BaseModel, validator
from uuid import UUID
from datetime import datetime
from enum import Enum
import json


class Roles(Enum):
    ACTOR = "actor"
    DIRECTOR = "director"
    WRITTER = "writer"


class ESPerson(BaseModel):
    id: str
    name: str


class ESMoviesDoc(BaseModel):
    id: str
    imdb_rating: float
    genre: str
    title: str
    description: str | None
    director: str | None
    actors_names: list[str]
    writers_names: list[str]
    actors: list[ESPerson]
    writers: list[ESPerson]


class ESIndexTemplate(BaseModel):
    _index: str
    _id: str


class ESBulk(BaseModel):
    index: str = "movies"

    bulk: list[ESMoviesDoc]

    def to_bulk_request(self) -> str:
        def get_index_description(doc: ESMoviesDoc) -> str:
            index_template = {"index": {"_index": self.index, "_id": str(doc.id)}}
            return json.dumps(index_template)

        def get_doc_properties(doc: ESMoviesDoc) -> str:
            return json.dumps(doc.dict(exclude={"id"}))

        bulk: list[str] = []
        for doc in self.bulk:
            bulk.append(get_index_description(doc))
            bulk.append(get_doc_properties(doc))
        bulk.append("\n")

        return "\n".join(bulk)


class Person(BaseModel):
    person_id: UUID
    person_name: str
    person_role: Roles


class FilmWork(BaseModel):
    id: UUID
    title: str
    description: str | None
    rating: float
    type: str
    created: datetime
    modified: datetime
    genres: list[str]
    persons: list[Person]

    def transform_to_es_doc(self) -> ESMoviesDoc:
        def get_persons_by(role: Roles) -> list[ESPerson]:
            return [
                ESPerson(id=str(i.person_id), name=i.person_name)
                for i in self.persons
                if i.person_role == role
            ]

        actors = get_persons_by(Roles.ACTOR)
        writers = get_persons_by(Roles.WRITTER)
        director = get_persons_by(Roles.DIRECTOR)

        actors_names = [i.name for i in actors]
        writers_names = [i.name for i in writers]

        es_index = ESMoviesDoc(
            id=str(self.id),
            imdb_rating=self.rating,
            genre=", ".join(self.genres),
            title=self.title,
            description=self.description,
            director=director[0].name if director else None,
            actors_names=actors_names,
            writers_names=writers_names,
            actors=actors,
            writers=writers,
        )

        return es_index


class FilmWorkContainer(BaseModel):
    batch: list[FilmWork]

    def transform_to_es_bulk(self) -> ESBulk:
        es_indexes = [i.transform_to_es_doc() for i in self.batch]
        return ESBulk(bulk=es_indexes)
