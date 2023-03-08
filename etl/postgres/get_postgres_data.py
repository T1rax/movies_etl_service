from psycopg2.extensions import connection as _connection
import logging


def get_new_film_updates_cursor(connection: _connection, ts):
    """Код для проверки новых фильмов"""

    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            select
                fw.id
                , fw.rating as imdb_rating
                , array_agg(distinct g.name) as genres
                , fw.title
                , fw.description
                , COALESCE (
                json_agg(
                    distinct jsonb_build_object(
                        'person_role', pfw.role,
                        'person_id', p.id,
                        'person_name', p.full_name
                    )
                ) FILTER (WHERE p.id is not null),
                '[]'
                ) as persons
                , fw.updated_at
            from content.film_work as fw
            left join content.person_film_work as pfw 
                on pfw.film_work_id = fw.id
            left join content.person as p 
                on p.id = pfw.person_id
            left join content.genre_film_work as gfw 
                on gfw.film_work_id = fw.id
            left join content.genre as g 
                on g.id = gfw.genre_id
            where 1=1
                and fw.updated_at > '{ts}'::timestamp
            group by fw.id
            order by fw.updated_at, fw.id
        """)
        
        return cursor

    except Exception as e:
        logging.exception("Произошла ошибка при Чтении данных из PostgreSQL, текст ошибки ниже: \n %s", e)


def get_new_genre_updates_cursor(connection: _connection, ts):
    """Код для проверки новых жанров"""

    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            with genre_updates as (
                select
                id
                , name
                , updated_at
                from content.genre
                where 1=1
                    and updated_at > '{ts}'::timestamp
            )
            select
                fw.id
                , fw.rating as imdb_rating
                , array_agg(distinct g.name) as genres
                , fw.title
                , fw.description
                , COALESCE (
                json_agg(
                    distinct jsonb_build_object(
                        'person_role', pfw.role,
                        'person_id', p.id,
                        'person_name', p.full_name
                    )
                ) FILTER (WHERE p.id is not null),
                '[]'
                ) as persons
                , max(g.updated_at) as updated_at
            from genre_updates as g
            left join content.genre_film_work as gfw 
                on gfw.genre_id = g.id
            left join content.film_work as fw
                on fw.id = gfw.film_work_id   
            left join content.person_film_work as pfw 
                on pfw.film_work_id = fw.id
            left join content.person as p 
                on p.id = pfw.person_id 
            group by fw.id
            order by max(g.updated_at), fw.id
        """)
        
        return cursor

    except Exception as e:
        logging.exception("Произошла ошибка при Чтении данных из PostgreSQL, текст ошибки ниже: \n %s", e)


def get_new_person_updates_cursor(connection: _connection, ts):
    """Код для проверки обновления по людям"""

    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            with person_updates as (
                select
                id
                , full_name
                , updated_at
                from content.person
                where 1=1
                    and updated_at > '{ts}'::timestamp
            )
            select
                fw.id
                , fw.rating as imdb_rating
                , array_agg(distinct g.name) as genres
                , fw.title
                , fw.description
                , COALESCE (
                json_agg(
                    distinct jsonb_build_object(
                        'person_role', pfw.role,
                        'person_id', p.id,
                        'person_name', p.full_name
                    )
                ) FILTER (WHERE p.id is not null),
                '[]'
                ) as persons
                , max(p.updated_at) as updated_at
            from person_updates as p
            left join content.person_film_work as pfw 
                on pfw.person_id = p.id
            left join content.film_work as fw
                on fw.id = pfw.film_work_id  
            left join content.genre_film_work as gfw 
                on gfw.film_work_id = fw.id
            left join content.genre as g 
                on g.id = gfw.genre_id
            group by fw.id
            order by max(p.updated_at), fw.id
        """)
        
        return cursor

    except Exception as e:
        logging.exception("Произошла ошибка при Чтении данных из PostgreSQL, текст ошибки ниже: \n %s", e)