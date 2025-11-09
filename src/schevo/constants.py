from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

PATH_PRJ = Path(__file__).resolve().parents[2]
PATH_CFG = PATH_PRJ / 'config'
PATH_RES = PATH_PRJ / 'res'

EXCEL_FORMATS: dict = {
    type(None): 'General',
    str: '@',
    int: '#,##0',
    float: '#,##0.00',
    Decimal: '#,##0.00',
    date: 'dd/mm/yyyy',
    time: 'h:mm:ss;@',
    datetime: 'dd/mm/yyyy h:mm:ss;@'
}
EXCEL_MAX_ROWS = 1_000_000

SQL_FORMATS: dict = {
    'string': 'VARCHAR(%d)',
    'integer': 'INTEGER',
    'decimal': 'NUMERIC(%d, %d)',
    'date': 'DATE',
    'time': 'TIME',
    'datetime': 'TIMESTAMP'
}

TEMPLATE_QUERY_CREATE_STREAM = """\
    CREATE TABLE IF NOT EXISTS schevo.%(stream)s (
        id INTEGER GENERATED ALWAYS AS IDENTITY,
        sys_filename VARCHAR(255) NOT NULL,
        sys_row_number INTEGER NOT NULL,
        sys_ins_date TIMESTAMP NOT NULL DEFAULT NOW(),
        %(records)s
        CONSTRAINT pk_%(stream)s_id
            PRIMARY KEY (id),
        CONSTRAINT uq_%(stream)s_filename_row_number 
            UNIQUE (sys_filename, sys_row_number),
        CONSTRAINT chk_%(stream)s_row_number
            CHECK (sys_row_number > 0)
    )
    ;
"""
TEMPLATE_QUERY_CREATE_STREAM_INDEX = """\
    CREATE UNIQUE INDEX IF NOT EXISTS idx_%(stream)s_filename_row_number
        ON schevo.%(stream)s (sys_filename, sys_row_number)
    ;
"""
TEMPLATE_QUERY_ADD_STREAM_RECORD = """\
    ALTER TABLE schevo.%(stream)s
    ADD COLUMN %(record)s %(record_type)s
    ;
"""
TEMPLATE_QUERY_ALTER_STREAM_RECORD = """\
    ALTER TABLE schevo.%(stream)s
    ALTER COLUMN %(record)s TYPE %(record_type)s
    ;
"""
TEMPLATE_QUERY_INSERT_STREAM_RECORD = """\
    INSERT INTO schevo.%(stream)s (%(columns)s) 
    VALUES (%(values)s)
    ;
"""

QUERY_GET_TABLES = """\
    SELECT TRUE
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
        AND table_schema = 'schevo'
        AND table_name = ?
    ;
"""
QUERY_GET_COLUMNS = """\
    SELECT column_name, 
        CASE data_type
            WHEN 'character varying' THEN 'VARCHAR'
            WHEN 'integer' THEN 'INTEGER'
            WHEN 'numeric' THEN 'NUMERIC'
            WHEN 'date' THEN 'DATE'
            WHEN 'time without time zone' THEN 'TIME'
            WHEN 'timestamp without time zone' THEN 'TIMESTAMP'
        END AS data_type,
        character_maximum_length AS varchar_length
    FROM information_schema.columns
    WHERE table_schema = 'schevo'
        AND table_name = ?
    ORDER BY ordinal_position
    ;
"""

QUERY_CHK_DUPLICATE = """\
    SELECT TRUE
    FROM schevo.%(stream)s
    WHERE sys_filename = ?
        AND sys_row_number = ?
    ;
"""
