#!/usr/bin/env python
# -*- coding:utf-8 -*-

import csv
import logging
from collections import OrderedDict
import os

LOG_FILE = os.environ.get("TIMESLICE_LOG", "/var/log/timeslice/timeslice.log")
LOG_LEVEL = logging.INFO

logging.basicConfig(filename = LOG_FILE, level = LOG_LEVEL)
logger = logging.getLogger(__name__)

class Window:
    u"""
    行を保持できる
    >>> window = Window(3, 1)
    >>> window.push({"col1": 1, "col2": 1})
    >>> window.push({"col1": 3, "col2": 4})
    >>> window.push({"col1": 5, "col2": 6})
    >>> window[0]
    {'col2': 4, 'col1': 3}

    関数を定義できる
    >>> def delta(window):
    ...     return window[1, "col2"] - window[0, "col2"]
    >>> window = Window(3, 1, {"delta": delta})
    >>> window.push({"col1": 1, "col2": 1})
    >>> window.push({"col1": 3, "col2": 4})
    >>> window.push({"col1": 5, "col2": 6})
    >>> window[0]
    {'delta': 2, 'col2': 4, 'col1': 3}

    関数を参照できる
    >>> def add(window):
    ...     return window[0, "col1"] + window[0, "col2"]
    >>> def delta(window):
    ...     return window[1, "add"] - window[0, "add"]
    >>> window = Window(3, 1, {"add": add, "delta": delta})
    >>> window.push({"col1": 1, "col2": 1})
    >>> window.push({"col1": 3, "col2": 4})
    >>> window.push({"col1": 5, "col2": 6})
    >>> window[0]
    {'add': 7, 'delta': 4, 'col2': 4, 'col1': 3}
    >>> window.push({"col1": 5, "col2": 6})
    >>> window[0]
    {'add': 11, 'delta': 0, 'col2': 6, 'col1': 5}
    """

    def __init__(self,
                 size,
                 offset,
                 additional_columns = {}
                 ):
        self._size = size
        self._offset = offset
        self._data = []
        self._additional_columns = additional_columns

    @property
    def size(self):
        return self._size

    @property
    def offset(self):
        return self._offset

    def columns(self, offset):
        return self._data[offset].keys() + self._additional_columns.keys()

    def push(self, row):
        self._data.append(row)
        if len(self._data) > self._size:
            self.pop()

    def pop(self):
        self._data.pop(0)

    def __getitem__(self, pos):
        if isinstance(pos, tuple):
            offset = self._offset + pos[0]
            column = pos[1]
            if column in self._data[offset]:
                return self._data[offset][column]
            elif column in self._additional_columns:
                self._offset += pos[0]
                value = self._additional_columns[column](self)
                self._offset -= pos[0]
                self._data[offset][column] = value
                return value
            else:
                raise ValueError("unknown column [%s]" % column)
        else:
            offset = self._offset + pos
            row = {}
            for column in self.columns(offset):
                row[column] = self[pos, column]
            return row

class TimeSliceData:
    u"""
    CSVファイルを処理できる
    >>> import StringIO
    >>> csv_str = 'col1,col2\\n10,12\\n11,15\\n12,16\\n13,20'
    >>> csv = StringIO.StringIO(csv_str)
    >>> data = TimeSliceData(csv, header = True)
    >>> def avg(window):
    ...     return float(window[1, "col2"] + window[0, "col2"])/2
    >>> data.add_column("avg", avg)
    >>> for row in data:
    ...     print row
    {'avg': 13.5, 'col2': 12.0, 'col1': 10.0}
    {'avg': 15.5, 'col2': 15.0, 'col1': 11.0}
    {'avg': 18.0, 'col2': 16.0, 'col1': 12.0}
    {'avg': 10.0, 'col2': 20.0, 'col1': 13.0}

    >>> # ignore_if でフィルタリングができる
    >>> def col1_eq_11(row):
    ...     return row["col1"] == 11.0
    >>> csv = StringIO.StringIO(csv_str)
    >>> data = TimeSliceData(csv, header = True)
    >>> def delta(window):
    ...     return window[1, "col1"] - window[0, "col1"]
    >>> data.add_column("delta", delta)
    >>> data.ignore_if(col1_eq_11)
    >>> for row in data:
    ...     print row
    {'delta': 2.0, 'col2': 12.0, 'col1': 10.0}
    {'delta': 1.0, 'col2': 16.0, 'col1': 12.0}
    {'delta': -13.0, 'col2': 20.0, 'col1': 13.0}

    """
    _SNIFF_SAMPLE_SIZE = 4096

    def __init__(self,
                 input_stream,
                 dialect = None,
                 before_data = {},
                 after_data = {},
                 sniff = True,
                 window_size = 1000,
                 window_offset = 500,
                 encoding = "utf-8",
                 delimiter = "\t",
                 ln = "\n",
                 has_header = None,
                 header = None,
                 cast = None,
                 ):
        self._input = input_stream
        self._dialect = dialect
        self._before_data = before_data
        self._after_data = after_data
        self._window_size = window_size
        self._window_offset = window_offset
        self._encoding = encoding
        self._delimiter = delimiter
        self._ln = ln
        self._has_header = has_header
        self._header = header
        self._additional_columns = OrderedDict()
        self._sniff = sniff
        self._cast = cast if cast != None else self._guess_and_cast
        self._types = {}
        self._ignore_filters = []
        self._row_count = 0

        if self._sniff:
            sniffer = csv.Sniffer()
            sample = self._input.read(self._SNIFF_SAMPLE_SIZE)
            if self._dialect == None:
                self._dialect = sniffer.sniff(sample)
            if self._has_header == None:
                self._has_header = sniffer.has_header(sample)
            self._input.seek(0)

        if self._dialect:
            self._delimiter = self._dialect.delimiter
            self._ln = self._dialect.lineterminator

        if self._window_offset >= self._window_size:
            raise ValueError("window offset must be smaller than window size")
        if not has_header and header == None:
            raise ValueError("header is not specified.")

        self._reader = self._get_reader()

    def basic_columns(self):
        return self._reader.fieldnames

    def columns(self):
        return self.basic_columns() + self._additional_columns.keys()

    def add_column(self, name, func):
        self._additional_columns[name] = func

    def ignore_if(self, func):
        self._ignore_filters.append(func)

    def _get_serializable_time_key(self, row):
        serializable = ""
        for key in self._unique_time_key:
            serializable += "\t%s" % row[key]
        return serializable

    def __iter__(self, headers = None):
        window = Window(
            self._window_size,
            self._window_offset,
            additional_columns = self._additional_columns)

        before_data = {col:self._before_data.get(col, 0) for col in self.columns()}
        after_data = {col:self._after_data.get(col, 0) for col in self.columns()}

        for i in xrange(window.offset):
            window.push(before_data)

        rows = 0
        row = None
        for i in xrange(self._window_size - self._window_offset):
            try:
                row = self._read_next()
                window.push(self._cast(row))
                rows += 1
            except StopIteration:
                for j in xrange(i, self._window_size - self._window_offset):
                    window.push(after_data)
                break

        while True:
            try:
                row = self._read_next()
                try:
                    yield window[0]
                except ZeroDivisionError as e:
                    logger.warn("Exception occured. [line = %s, error = %s]" % (self._row_count, e))
                    raise
                except Exception as e:
                    logger.warn("Exception occured. [line = %s, error = %s]" % (self._row_count, e))
                window.push(self._cast(row))
            except StopIteration:
                break

        for i in xrange(rows):
            try:
                yield window[0]
            except ZeroDivisionError as e:
                logger.warn("Exception occured. [line = %s, error = %s]" % (self._row_count, e))
            except Exception as e:
                logger.warn("Exception occured. [line = %s, error = %s]" % (self._row_count, e))
            window.push(after_data)

    def _read_next(self):
        while True:
            row = self._reader.next()
            self._row_count += 1
            if len(row) < len(self.basic_columns()):
                logger.warn("invalid row found(lack some columns). ignore.[line = %s]" % self._row_count)
                continue
            if None in row:
                logger.warn("invalid row found(too many columns). ignore.[line = %s]" % self._row_count)
                continue
            try:
                for filter_ in self._ignore_filters:
                    if filter_(self._cast(row)):
                        break
                else:
                    self._current_row = row
                    return row
            except TypeError:
                logger.warn("invalid row found(cast failed with TypeError). ignore.[line = %s]" % self._row_count)
                continue
            except ValueError:
                logger.warn("invalid row found(cast failed with ValueError). ignore.[line = %s]" % self._row_count)
                continue

    def _get_reader(self):
        if self._encoding == "utf-8":
            input = self._input
        else:
            input = (unicdoe(row, self._encoding).encode("utf-8")
                     for row in self._input)

        if not self._has_header:
            if self._dialect:
                reader = csv.DictReader(input,
                                        fieldnames = self._header,
                                        dialect = self._dialect)
            else:
                reader = csv.DictReader(input,
                                        fieldnames = self._header,
                                        delimiter = self._delimiter,
                                        )
        else:
            if self._dialect:
                reader = csv.DictReader(input,
                                        dialect = self._dialect)
            else:
                reader = csv.DictReader(input,
                                        delimiter = self._delimiter,
                                        )
        return reader

    def _guess_and_cast(self, row):
        casted = {}
        for key, value in row.items():
            if key not in self._types:
                type_ = self._guess_type(value)
                self._types[key] = type_
            try:
                casted[key] = self._types[key](value)
            except:
                logger.info("cast failed. [value = '%s', type = %s]" % (value, self._types[key]))
                raise
        return casted

    def _guess_type(self, obj):
        try:
            float(obj)
            return float
        except:
            return obj.__class__
