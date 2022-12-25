import dataclasses
import datetime
import hashlib

import psycopg2
from psycopg2.extras import DictCursor

conn = psycopg2.connect(
    dbname="task_13",
    user="task_13",
    password="task_13",
    host="localhost",
    port="5432",
    cursor_factory=DictCursor,
)


@dataclasses.dataclass
class User:
    id: int
    username: str
    password_sha: str
    enabled: bool
    created: datetime.datetime


@dataclasses.dataclass
class Ad:
    id: int
    user_id: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    deleted: bool
    body: str
    user: User


class Connector:
    tbl_users = "users"
    tbl_ads = "ads"
    solt = "secure"

    def __init__(self, connection):
        self.conn = connection

    def table_exists(self, table_name) -> bool:
        with self.conn.cursor() as cursor:
            cursor: DictCursor
            query = f"""SELECT EXISTS (
                   SELECT FROM information_schema.tables 
                   WHERE  table_schema = 'public'
                   AND    table_name   = (%s)
                   );"""

            cursor.execute(query, [table_name])
            result = cursor.fetchone()[0]
        return result

    def table_users_exists(self):
        return self.table_exists(self.tbl_users)

    def create_table_users(self):
        query = f"""
        CREATE TABLE {self.tbl_users} (
              id              serial primary key,
              username        varchar(32) not null unique,
              password_sha    varchar(64) null,
              enabled         bool default true,
              created         timestamptz default now()
            );
        """

        with self.conn.cursor() as cursor:
            cursor: DictCursor
            cursor.execute(query)

        conn.commit()

    def table_ads_exists(self):
        return self.table_exists(self.tbl_ads)

    def clear(self):
        query = f"truncate table {self.tbl_users} cascade;"
        with self.conn.cursor() as cursor:
            cursor: DictCursor
            cursor.execute(query)

        conn.commit()

    def create_table_ads(self):
        query = f"""
        CREATE TABLE {self.tbl_ads} (
              id              serial primary key,
              user_id         integer constraint ads_users_id_fk references users,
              created_at      timestamptz default now(),
              updated_at      timestamptz default now(),
              deleted         bool default false,
              body            text not null
            );
        """

        with self.conn.cursor() as cursor:
            cursor: DictCursor
            cursor.execute(query)

        conn.commit()

    def add_user(self, username: str, password: str) -> int:
        password_sha = hashlib.sha256(f"{self.solt}.{password}".encode()).hexdigest()
        query = "insert into users (username, password_sha) values ((%s), (%s)) returning id;"
        with self.conn.cursor() as cursor:
            cursor: DictCursor
            cursor.execute(query, [username, password_sha])
            user_id = cursor.fetchone()[0]
        conn.commit()
        return user_id

    def add_ad(self, user_id: int, body: str) -> int:
        query = "insert into ads (user_id, body) values (%s, %s) returning id"
        with self.conn.cursor() as cursor:
            cursor: DictCursor
            cursor.execute(query, [user_id, body])
            ad_id = cursor.fetchone()[0]
        conn.commit()
        return ad_id

    def soft_delete_ad(self, ad_id: int):
        query = "update ads set (deleted, updated_at) = (True, now()) where id=%s;"
        with self.conn.cursor() as cursor:
            cursor: DictCursor
            cursor.execute(query, [ad_id])
        conn.commit()

    def delete_ad(self, ad_id: int):
        query = "delete from ads where id=%s;"
        with self.conn.cursor() as cursor:
            cursor: DictCursor
            cursor.execute(query, [ad_id])
        conn.commit()

    def all_ads(self):
        response = []
        query = """
            select
            u.id as user__id,
            u.username as user__username,
            u.password_sha as user__password_sha,
            u.enabled as user__enabled,
            u.created as user__created,
            ads.id as ads__id,
            ads.user_id as ads__user_id,
            ads.created_at as ads__created_at,
            ads.updated_at as ads__updated_at,
            ads.deleted as ads__deleted,
            ads.body as ads__body
            from ads 
            join users u on u.id = ads.user_id 
            where not ads.deleted 
            order by ads.id desc;
            """
        with self.conn.cursor() as cursor:
            cursor: DictCursor
            cursor.execute(query)
            for data in cursor.fetchall():
                response.append(
                    Ad(
                        id=data["ads__id"],
                        user_id=data["ads__user_id"],
                        created_at=data["ads__created_at"],
                        updated_at=data["ads__updated_at"],
                        deleted=data["ads__deleted"],
                        body=data["ads__body"],
                        user=User(
                            id=data["user__id"],
                            username=data["user__username"],
                            password_sha=data["user__password_sha"],
                            enabled=data["user__enabled"],
                            created=data["user__created"],
                        ),
                    )
                )
        return response


connector = Connector(conn)
