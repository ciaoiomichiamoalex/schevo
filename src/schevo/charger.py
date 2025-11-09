import re
from asyncio import Future
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from os import cpu_count
from pathlib import Path

from constants import (PATH_CFG, PATH_RES, QUERY_CHK_DUPLICATE, QUERY_GET_COLUMNS, QUERY_GET_TABLES,
                       SQL_FORMATS, TEMPLATE_QUERY_ADD_STREAM_RECORD, TEMPLATE_QUERY_ALTER_STREAM_RECORD,
                       TEMPLATE_QUERY_CREATE_STREAM, TEMPLATE_QUERY_CREATE_STREAM_INDEX,
                       TEMPLATE_QUERY_INSERT_STREAM_RECORD)
from core import Querier
from decoder import decode_config, decode_record


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


def check_stream(stream_name: str,
                 config: dict[str, dict]) -> None:
    """
    Check if the table is already created or need to be modified in the database.

    :param stream_name: The stream name.
    :type stream_name: str
    :param config: The configuration with column and their types.
    :type config: dict[str, dict]
    """
    querier: Querier = Querier(cfg_in=PATH_CFG, save_changes=True)
    for record_code, columns_config in config.items():
        table_name = define_record_name(f'{stream_name.lower()}_{record_code.lower()}')

        if querier.run(QUERY_GET_TABLES, table_name).fetch(Querier.FETCH_VAL):
            columns = {
                col: (type, len)
                for col, type, len
                in querier.run(QUERY_GET_COLUMNS, table_name).fetch(Querier.FETCH_ALL)
            }

            for key, value in columns_config.items():
                if key not in columns:
                    querier.run(TEMPLATE_QUERY_ADD_STREAM_RECORD % {
                        'stream': table_name,
                        'record': key,
                        'record_type': define_record_type(value)
                    })
                elif columns[key][0] == 'VARCHAR' and columns[key][1] < (value['end'] - value['begin'] + 1):
                    querier.run(TEMPLATE_QUERY_ALTER_STREAM_RECORD % {
                        'stream': table_name,
                        'record': key,
                        'record_type': define_record_type(value)
                    })
        else:
            records = ''
            for key, value in columns_config.items():
                records += define_record_name(key)
                records += f' {define_record_type(value)}, '

            querier.run(TEMPLATE_QUERY_CREATE_STREAM % {'stream': table_name, 'records': records})
            querier.run(TEMPLATE_QUERY_CREATE_STREAM_INDEX % {'stream': table_name})
    del querier


def break_stream(stream: Path,
                 encoding: str = None,
                 rows_break: int = 100_000) -> list[Path]:
    """
    Split a stream file into multiple sub-stream file, by splitting every rows_number rows.

    :param stream: The input stream file.
    :type stream: Path
    :param encoding: The encoding of the input file, defaults to None.
    :type encoding: str
    :param rows_break: The number of rows to split the file stream into.
    :type rows_break: int
    :return: La lista dei sub-stream file splittati.
    :rtype: list[Path]
    """
    with open(stream, encoding=encoding) as fin:
        suffix = 0
        fou = open(stream.parent / f'{stream.name}#{suffix}', 'w', encoding=encoding)

        for row_num, row in enumerate(fin, start=1):
            fou.write(row)

            if row_num % rows_break == 0:
                fou.close()
                suffix += 1
                fou = open(stream.parent / f'{stream.name}#{suffix}', 'w', encoding=encoding)
        fou.close()

    return [
        substream
        for substream in stream.parent.iterdir()
        if substream.is_file()
            and substream.name.startswith(stream.name)
            and substream != stream
    ]


def charge_stream(stream: Path,
                  stream_name: str,
                  config: dict,
                  job_begin: datetime = datetime.now(),
                  rows_break: int = 100_000) -> None:
    """
    Charge a file with format fixed-length into database tables, one or more for each record code.

    :param stream: The input file to working on.
    :type stream: Path
    :param stream_name: The name of the stream.
    :type stream_name: str
    :param config: The stream configuration
    :type config: dict
    :param job_begin: The timestamp of the job starting.
    :type job_begin: datetime
    :param rows_break: The break size used from break_stream function for splitting stream.
    :type rows_break: int
    """
    querier: Querier = Querier(cfg_in=PATH_CFG, save_changes=True)
    with open(stream, encoding=config.get('encoding')) as source_in:

        for row_num, row in enumerate(source_in, start=1):
            record_code = row[config['record_code'][0] - 1 : config['record_code'][1]]
            record = decode_record(row, config['config'].get(record_code))
            if not record: continue

            table_name = define_record_name(f'{stream_name.lower()}_{record_code.lower()}')
            substream = stream.name.rsplit('#', maxsplit=1)
            filename, row_number = substream[0], row_num + (int(substream[-1].replace('#', '')) * rows_break)
            if querier.run(QUERY_CHK_DUPLICATE % {
                'stream': table_name
            }, filename, row_number).fetch(Querier.FETCH_VAL):
                continue

            record.update({
                'sys_filename': filename,
                'sys_row_number': row_number,
                'sys_ins_date': job_begin
            })
            querier.run(
                TEMPLATE_QUERY_INSERT_STREAM_RECORD % {
                    'stream': table_name,
                    'columns': ', '.join([*record.keys()]),
                    'values': ', '.join('?' for _ in record)
                }, [*record.values()]
            )
    del querier


def runner(streams: dict[str, dict],
           job_begin: datetime = datetime.now()) -> None:
    """
    Split the input file into files of 100.000 rows and start a thread on each one.

    :param streams: The result of decode_config() function with the input file and stream configuration.
    :type streams: dict[str, dict]
    :param job_begin:
    :type job_begin:
    """
    def worker(stream: Path,
               stream_name: str,
               config: dict,
               job_begin: datetime = datetime.now()) -> None:
        """
        Call the charge_stream function and delete the substream file after working on.

        :param stream: The input file to working on.
        :type stream: Path
        :param stream_name: The name of the stream.
        :type stream_name: str
        :param config: The stream configuration
        :type config: dict
        :param job_begin: The timestamp of the job starting.
        :type job_begin: datetime
        """
        try: charge_stream(stream, stream_name, config, job_begin),
        except Exception: raise
        finally: stream.unlink(missing_ok=True)

    with ThreadPoolExecutor(max_workers=min(32, (cpu_count() or 1) * 2)) as executor:
        futures: list[Future] = []
        for stream_name, config in streams.items():
            check_stream(stream_name, config['config'])

            for stream in config['streams']:
                for substream in break_stream(stream, encoding=config.get('encoding')):
                    futures.append(executor.submit(worker, substream, stream_name, config, job_begin))
        for future in futures: future.result()


if __name__ == '__main__':
    job_begin = datetime.now()
    runner(decode_config(PATH_RES), job_begin=job_begin)
    print(datetime.now() - job_begin)
