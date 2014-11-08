# -*- coding: utf-8 -*-
#
# Copyright (C) 2005-2009 Edgewall Software
# Copyright (C) 2005-2006 Christopher Lenz <cmlenz@gmx.de>
# Copyright (C) 2005 Jeff Weiss <trac@jeffweiss.org>
# Copyright (C) 2006 Andres Salomon <dilinger@athenacr.com>
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution. The terms
# are also available at http://trac.edgewall.org/wiki/TracLicense.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at http://trac.edgewall.org/log/.

import os
import re
import sys
import types

from genshi.core import Markup

from trac.core import *
from trac.config import Option
from trac.db.api import ConnectionBase, DatabaseManager, IDatabaseConnector, \
						_parse_db_str, get_column_names
from trac.db import Table, Column, Index
from trac.db.util import ConnectionWrapper, IterableCursor
from trac.env import IEnvironmentSetupParticipant, ISystemInfoProvider
from trac.util import as_int, get_pkginfo
from trac.util.compat import close_fds
from trac.util.text import exception_to_unicode, to_unicode
from trac.util.translation import _

try:
	import pymssql as pymssql
	has_mssql = True

except ImportError:
	has_mssql = False

# Mapping from "abstract" SQL types to DB-specific types
_type_map = {
	'int64': 'bigint',
	'text': 'nvarchar(512)',
}

# TODO: You cannot use MS Access because column name 'value' can seems not use via odbc.
_column_map = {
	'key': '"key"',
#    'value': '"value"'
}

re_limit = re.compile(" LIMIT (\d+)( OFFSET (\d+))?", re.IGNORECASE)
re_order_by = re.compile("ORDER BY ", re.IGNORECASE)
re_where = re.compile("WHERE ", re.IGNORECASE)
re_equal = re.compile("(\w+)\s*=\s*(['\w]+|\?)", re.IGNORECASE)
re_isnull = re.compile("(\w+) IS NULL", re.IGNORECASE)
re_select = re.compile('SELECT( DISTINCT)?( TOP)?', re.IGNORECASE)
re_coalesce_equal = re.compile("(COALESCE\([^)]+\))=([^,]+)", re.IGNORECASE)
re_cast = re.compile("CAST\(\s*([\.\w]+)\s*AS\s*([\.\w]+)\)", re.IGNORECASE + re.MULTILINE)

class MSSQLConnector(Component):
	"""Database connector for MSSQL version 4.1 and greater.

	Database URLs should be of the form:
	{{{
	mssql://user[:password]@host[:port]/database[?param1=value&param2=value]
	}}}
	The following parameters are supported:
	 * `compress`: Enable compression (0 or 1)
	 * `init_command`: Command to run once the connection is created
	 * `named_pipe`: Use a named pipe to connect on Windows (0 or 1)
	 * `read_default_file`: Read default client values from the given file
	 * `read_default_group`: Configuration group to use from the default file
	 * `unix_socket`: Use a Unix socket at the given path to connect
	"""
	implements(IDatabaseConnector, IEnvironmentSetupParticipant,
			   ISystemInfoProvider)

	required = False

	mssqldump_path = Option('trac', 'mssqldump_path', 'mssqldump',
		"""Location of mssqldump for MSSQL database backups""")

	def __init__(self):
		self._mssql_version = None
		self._mssql_version = has_mssql and \
								get_pkginfo(pymssql).get('version',
														 pymssql.__version__)
		if self._mssql_version:
			self.required = True
			self.error = None

	# ISystemInfoProvider methods

	def get_system_info(self):
		if self.required:
			yield 'pymssql', self._mssql_version

	# IDatabaseConnector methods

	def get_supported_schemes(self):
		if not has_mssql:
			self.error = _("Cannot load Python bindings for MSSQL")
		yield ('mssql', -1 if self.error else 1)

	def get_connection(self, path, log=None, user=None, password=None,
					   host=None, port=None, params={}):
		cnx = MSSQLConnection(path, log, user, password, host, port, params)
		return cnx

	def get_exceptions(self):
		return pymssql

	def init_db(self, path, schema=None, log=None, user=None, password=None,
				host=None, port=None, params={}):
		cnx = self.get_connection(path, log, user, password, host, port,
								  params)
		self.is_azure = host.find("database.windows.net") != -1
		#self._verify_variables(cnx)
		self.clustered = []
		cursor = cnx.cursor()
		if schema is None:
			from trac.db_default import schema
		for table in schema:
			for stmt in self.to_sql(table):
				#print "Executing '%s'" % stmt
				#self.log.debug(stmt)
				cursor.execute(stmt)
		cnx.commit()

	def to_sql(self, table):
		sql = ["CREATE TABLE %s (" % table.name]
		coldefs = []
		for column in table.columns:
			column.name = _column_map.get(column.name, column.name)
			ctype = column.type.lower()
			ctype = _type_map.get(ctype, ctype)
			#  for SQL Server, patch for "enum" table, value is not text, use int instead.
			if table.name == 'enum' and column.name == 'value':
				ctype = 'int'
			if (table.name, column.name) in [
					('wiki', 'text'),
					('report', 'query'),
					('report', 'description'),
					('milestone', 'description'),
					('version', 'description'),
				]:
				ctype = 'nvarchar(MAX)'
			if (table.name, column.name) in [
					('ticket', 'description'),
					('ticket_change', 'oldvalue'),
					('ticket_change', 'newvalue'),
					('ticket_custom', 'value'),
					('session_attribute', 'value')
				]:
				ctype = 'nvarchar(4000)'

	# I'm using SQL Userver 2012 Express
			if column.auto_increment:
				ctype = 'INT IDENTITY NOT NULL'  # SQL Server Style
	#            ctype = 'INT UNSIGNED NOT NULL AUTO_INCREMENT'  # MySQL Style
	#            ctype = 'SERIAL'  # PGSQL Style
	#            ctype = "integer constraint P_%s PRIMARY KEY" % table.name  # SQLite Style
			else:
	#            if column.name in table.key or any([column.name in index.columns for index in table.indices]):
	#                ctype = {'ntext': 'nvarchar(255)'}.get(ctype, ctype)  # SQL Server cannot use text as PK
				if len(table.key) == 1 and column.name in table.key:
					ctype += " constraint P_%s PRIMARY KEY" % table.name
					self.clustered.append(table.name) # For the Azure case
			coldefs.append("    %s %s" % (column.name, ctype))
		if len(table.key) > 1:
			coldefs.append("    UNIQUE (%s)" % ','.join(table.key))
		sql.append(',\n'.join(coldefs) + '\n);')
		yield '\n'.join(sql)
		if self.is_azure and len(table.indices) == 0:
			table.indices.append(Index([table.columns[0].name]))
		for index in table.indices:
			type_ = ('INDEX', 'UNIQUE INDEX')[index.unique]
			if self.is_azure and not table.name in self.clustered:
				clustered_bit = "CLUSTERED "
				self.clustered.append(table.name)
			else:
				clustered_bit = ""
			yield "CREATE %s%s %s_%s_idx ON %s (%s);" % (clustered_bit, type_, table.name,
				  '_'.join(index.columns), table.name, ','.join(index.columns))


	def alter_column_types(self, table, columns):
		"""Yield SQL statements altering the type of one or more columns of
		a table.

		Type changes are specified as a `columns` dict mapping column names
		to `(from, to)` SQL type tuples.
		"""
		alterations = []
		for name, (from_, to) in sorted(columns.iteritems()):
			to = _type_map.get(to, to)
			if to != _type_map.get(from_, from_):
				alterations.append((name, to))
		if alterations:
			yield "ALTER TABLE %s %s" % (table,
				', '.join("ALTER COLUMN %s TYPE %s" % each
						  for each in alterations))

	def backup(self, dest_file):
		raise "Backup of MS SQL Server db not supported."

	# IEnvironmentSetupParticipant methods

	def environment_created(self):
		pass

	def environment_needs_upgrade(self):
		return False

	def upgrade_environment(self):
		pass

class MSSQLConnection(ConnectionBase, ConnectionWrapper):
	"""Connection wrapper for MSSQL."""

	poolable = True

	def __init__(self, path, log, user=None, password=None, host=None, port=None, params={}):
		if path.startswith('/'):
			path = path[1:]
		if 'host' in params:
			host = params['host']
		is_azure = host.find("database.windows.net") != -1
		if is_azure:
			servername = host.split(".")[0]
			user = user + "@" + servername
		cnx = pymssql.connect(database=path, user=user, password=password, host=host,
							  port=port)
		self.schema = path
		conn = ConnectionWrapper.__init__(self, cnx, log)
		self._is_closed = False

	def cursor(self):
		cursor = SQLServerCursor(self.cnx.cursor(), self.log)
		cursor.cnx = self
		return cursor

	def rollback(self):
		try:
			self.cnx.rollback()
		except pymssql.ProgrammingError:
			self._is_closed = True

	def close(self):
		if not self._is_closed:
			try:
				self.cnx.close()
			except pymssql.ProgrammingError:
				pass # this error would mean it's already closed.  So, ignore
			self._is_closed = True

	def cast(self, column, type):
		self.log.debug("CASTing %s to %s" % (column,type))
		if type == 'signed':
			type = 'int'
		elif type == 'text':
			type = 'varchar(max)'
		return 'CAST(%s AS %s)' % (column, type)

	def concat(self, *args):
		return 'concat(%s)' % ', '.join(args)

	def drop_table(self, table):
		cursor = pymssql.cursors.Cursor(self.cnx)
		cursor._defer_warnings = True  # ignore "Warning: Unknown table ..."
		cursor.execute("DROP TABLE IF EXISTS " + self.quote(table))

	def get_column_names(self, table):
		rows = self.execute("""
			SELECT column_name FROM information_schema.columns
			WHERE table_schema=%s AND table_name=%s
			""", (self.schema, table))
		return [row[0] for row in rows]

	def get_last_id(self, cursor, table, column='id'):
		return cursor.lastrowid

	def get_table_names(self):
		rows = self.execute("""
			SELECT table_name FROM information_schema.tables
			WHERE table_schema=%s""", (self.schema,))
		return [row[0] for row in rows]

	def like(self):
		return 'LIKE %s'
		# TODO quick hacked. check me.

	def like_escape(self, text):
		return text
		# TODO quick hacked. check me.

	def prefix_match(self):
		return "LIKE %s ESCAPE '/'"

	def prefix_match_value(self, prefix):
		return self.like_escape(prefix) + '%'

	def quote(self, identifier):
		return "`%s`" % identifier.replace('`', '``')

	def update_sequence(self, cursor, table, column='id'):
		# MSSQL handles sequence updates automagically
		pass

class SQLServerCursor(object):

	def __init__(self, cursor, log=None):
		self.cursor = cursor
		self.log = log

	def __getattr__(self, name):
		return getattr(self.cursor, name)

	def __iter__(self):
		while True:
			row = self.cursor.fetchone()
			if not row:
				return
			yield row

	def execute(self, sql, args=None):
		if args:
			sql = sql % (('%s',) * len(args))

		# replace __column__ IS NULL -> COALESCE(__column__, '') after ORDER BY
		match = re_order_by.search(sql)
		if match:
			end = match.end()
			for match in reversed([match for match in re_isnull.finditer(sql[end:])]):
				replacement = "COALESCE(%s,'')" % match.group(1)
				sql = sql[:end + match.start()] + replacement + sql[end + match.end():]

		# replace __column__ = %s -> CASE __column__ WHEN %s THEN '0' ELSE '1' END after ORDER BY
		match = re_order_by.search(sql)
		if match:
			end = match.end()
			for match in reversed([match for match in re_equal.finditer(sql[end:])]):
				replacement = "CASE %s WHEN %s THEN '0' ELSE '1' END" % (match.group(1), match.group(2))
				sql = sql[:end + match.start()] + replacement + sql[end + match.end():]
			for match in reversed([match for match in re_coalesce_equal.finditer(sql[end:])]):
				replacement = "CASE %s WHEN %s THEN '0' ELSE '1' END" % (match.group(1), match.group(2))
				sql = sql[:end + match.start()] + replacement + sql[end + match.end():]

		# trim duplicated columns after ORDER BY
		match = re_order_by.search(sql)
		if match:
			end = match.end()
			match = re.search("'([a-z]+)'", sql[end:])
			if match:
				column_name = match.group(1)
				re_columns = re.compile("([a-z]+.)?%s,?" % column_name)
				order_by = ' '.join([column for column in match.string.split(' ') if not re_columns.match(column)])
				self.log.debug(order_by)
				sql = sql[:end] + order_by

		# transform LIMIT clause
		match = re_limit.search(sql)
		if match:
			limit = match.group(1)
			offset = match.group(3)
			if not offset:
				# LIMIT n (without OFFSET) -> SELECT TOP n
				sql = match.string[:match.start()].replace("SELECT", "SELECT TOP %s" % limit)
			else:
				# LIMIT n OFFSET m -> OFFSET m ROWS FETCH NEXT n ROWS ONLY
				sql = match.string[:match.start()] + " OFFSET %s ROWS FETCH NEXT %s ROWS ONLY" % (offset, limit)
#                match = re_where.search(sql)
#                sql = match.string[:match.end()] + 'ROW_NUMBER() > %s, ' % limit + match.string[match.end():]
		# avoid error in "order by" in sub query
		# TODO: decide count of lines
		else:
			for match in reversed([match for match in re_select.finditer(sql) if match.group(2) == None]):
				sql = sql[:match.end()] + ' TOP 1000' + sql[match.end():]

		match = re_cast.search(sql)
		if match:
			self.log.debug("Found a cast: " + match.group(0))
			source = match.group(1)
			dest = match.group(2)
			if dest == "signed":
				self.log.debug("Cast was signed: " + dest)
				sql = sql.replace("AS signed", "AS int")
			else:
				self.log.debug("Cast wasn't signed: " + dest)
		#self.log.debug("Executing sql %s with %s" % (sql,args))
		try:
			if self.log:  # See [trac] debug_sql in trac.ini
				self.log.debug(sql)
				self.log.debug(args)
			if args:
				self.cursor.execute(sql, tuple(args))
			else:
				self.cursor.execute(sql, ())
		except:
			self.cnx.rollback()
			raise

	def executemany(self, sql, args):
		if not args:
			return
		sql = sql % (('%s',) * len(args[0]))
		try:
			if self.log:  # See [trac] debug_sql in trac.ini
				self.log.debug(sql)
				self.log.debug(args)
			self.cursor.executemany(sql, args)
		except:
			self.cnx.rollback()
			raise
