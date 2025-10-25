import re
from datetime import datetime

from constants import (PATH_CFG, PATH_RES, QUERY_GET_COLUMNS, QUERY_GET_TABLES, SQL_FORMATS,
                       TEMPLATE_QUERY_ADD_STREAM_RECORD, TEMPLATE_QUERY_ALTER_STREAM_RECORD,
                       TEMPLATE_QUERY_CREATE_STREAM, TEMPLATE_QUERY_CREATE_STREAM_INDEX,
                       TEMPLATE_QUERY_INSERT_STREAM_RECORD)
from core import Querier
from decoder import decode_config, decode_record
from schevo.constants import QUERY_CHK_DUPLICATE


def define_record_name(record_name: str) -> str:
    """
    Parse records name to make them usable in databases. By making it lowercase, removing spaces and initial numbers.

    :param record_name: The raw record name.
    :type record_name: str
    :return: The record name parsed.
    :rtype: str
    """
    record_name = re.sub(r'[^a-z0-9]+', '_', record_name.lower()).strip('_')
    if re.match(r'^\d', record_name): record_name = f'c_{record_name}'
    return record_name


def define_record_type(record: dict) -> str:
    """
    Define the database record type from the configuration record.

    :param record: The record configuration.
    :type record: dict
    :return: The parsed SQL type.
    :rtype: str
    """
    match (record_type := record.get('type', 'string')):
        case 'decimal':
            precision = record['end'] - record['begin'] + 1
            scale = int(record.get('format', 'e-2').split('-')[1])
            return SQL_FORMATS[record_type] % (precision, scale)
        case 'string':
            length = record['end'] - record['begin'] + 1
            return SQL_FORMATS[record_type] % length
        case _:
            return SQL_FORMATS[record_type]


def check_stream(stream: str,
                 record_code: str,
                 config: dict[str, dict]) -> str:
    """
    Check if the table is already created or need to be modified in the database.

    :param stream: The stream name.
    :type stream: str
    :param record_code: The record code for identify the correct table.
    :type record_code: str
    :param config: The configuration with column and their types.
    :type config: dict[str, dict]
    :return: The database table name.
    :rtype: str
    """
    querier: Querier = Querier(cfg_in=PATH_CFG, save_changes=True)
    stream_name = define_record_name(f'{stream.lower()}_{record_code.lower()}')

    if querier.run(QUERY_GET_TABLES, stream_name).fetch(Querier.FETCH_VAL):
        columns = {
            col: (type, len)
            for col, type, len in querier.run(QUERY_GET_COLUMNS, stream_name).fetch(Querier.FETCH_ALL)
        }

        for key, value in config.items():
            if key not in columns:
                querier.run(TEMPLATE_QUERY_ADD_STREAM_RECORD % {
                    'stream': stream_name,
                    'record': key,
                    'record_type': define_record_type(value)
                })
            elif columns[key][0] == 'VARCHAR' and columns[key][1] < (value['end'] - value['begin'] + 1):
                querier.run(TEMPLATE_QUERY_ALTER_STREAM_RECORD % {
                    'stream': stream_name,
                    'record': key,
                    'record_type': define_record_type(value)
                })
    else:
        records = ''
        for key, value in config.items():
            records += define_record_name(key)
            records += f' {define_record_type(value)}, '

        querier.run(TEMPLATE_QUERY_CREATE_STREAM % {'stream': stream_name, 'records': records})
        querier.run(TEMPLATE_QUERY_CREATE_STREAM_INDEX % {'stream': stream_name})

    del querier
    return stream_name


def charge_stream(streams: dict[str, dict],
                  job_begin: datetime = datetime.now()) -> None:
    """
    Charge a file with format fixed-length into database tables, one or more for each record code.

    :param streams: The result of decode_config() function with the input file and stream configuration.
    :type streams: dict[str, dict]
    :param job_begin: The timestamp of the job starting.
    :type job_begin: datetime
    """
    querier: Querier = Querier(cfg_in=PATH_CFG, save_changes=True)

    for stream, config in streams.items():
        for fin in config['streams']:
            with open(fin, encoding=config.get('encoding')) as source_in:

                is_checked = {}
                for row_num, row in enumerate(source_in, start=1):
                    record_code = row[config['record_code'][0] - 1 : config['record_code'][1]]
                    record = decode_record(row, config['config'].get(record_code))
                    if not record: continue

                    if not is_checked.get(record_code):
                        is_checked[record_code] = check_stream(stream,
                                                               record_code,
                                                               config['config'].get(record_code))
                    if querier.run(QUERY_CHK_DUPLICATE % {
                        'stream': is_checked[record_code]
                    }, fin.name, row_num).fetch(Querier.FETCH_VAL):
                        continue

                    record.update({
                        'sys_filename': fin.name,
                        'sys_row_number': row_num,
                        'sys_ins_date': job_begin
                    })
                    querier.run(
                        TEMPLATE_QUERY_INSERT_STREAM_RECORD % {
                            'stream': is_checked[record_code],
                            'columns': ', '.join([*record.keys()]),
                            'values': ', '.join('?' for _ in record)
                        }, [*record.values()]
                    )
    del querier


if __name__ == '__main__':
    job_begin = datetime.now()
    charge_stream(decode_config(PATH_RES), job_begin=job_begin)
    print(datetime.now() - job_begin)
