import datetime
import json
import os
import csv
import zipfile
import mysql.connector
import tempfile


class DBConn:
    """
    DB Connection class with methods to manipulate the database
    """

    def __init__(self, host: str, user: str, passwd: str, db: str, port: int, auth_plugin: str):
        """ Initialize the database connection using the provided data """
        self.connCreds = {
            'host': host,
            'user': user,
            'passwd': passwd,
            'db': db,
            'port': port,
            'auth_plugin': auth_plugin
        }
        self.conn = self.createConn(modeSet=False)
        self.database = self.connCreds['db']

    def createConn(self, modeSet=True):
        """
        Create a new database connection.  If modeSet is True, the instance connection variable is set. If false, the
        connection variable is returned
        """
        connTmp = mysql.connector.connect(
            host=self.connCreds['host'],
            user=self.connCreds['user'],
            password=self.connCreds['passwd'],
            database=self.connCreds['db'],
            port=self.connCreds['port'],
            auth_plugin=self.connCreds['auth_plugin']
        )
        connTmp.autocommit = True
        if modeSet:
            self.conn = connTmp
        return connTmp

    def getConn(self):
        """
        Fetch a connection from the pool.
        :return: tuple: (connection, cursor)
        """
        if not self.conn.is_connected():
            self.conn.close()
            self.createConn()
        return self.conn, self.conn.cursor()

    def dbSelect(self, query, data=()):
        """
        Select data from a database.

        :param query: The query string. Format: using %s as a placeholder for variables.  No quotes around %s.
        :param data: Iterable , even if there is only one value.  Data for placeholders in the query.
        """
        dbc = self.getConn()
        if len(data) == 0:
            dbc[1].execute(query)
        else:
            dbc[1].execute(query, data)
        selectedFields = [item[0] for item in dbc[1].description]
        selectedData = dbc[1].fetchall()
        return [{selectedFields[colId]:row[colId] for colId in range(len(selectedFields))} for row in selectedData]

    def dbQuery(self, query: str, data=()):
        """
        Perform a database query.

        :param query: The query string.  Format: "VALUES (%s, %s)" where %s is a placeholder.
        :param data: Tuple, even if there is only one value.  Data for placeholders in the query.
        """
        dbc = self.getConn()
        if len(data) == 0:
            dbc[1].execute(query)
        else:
            dbc[1].execute(query, data)

    def fetchTableList(self) -> list:
        """ Return a list of table name strings from this database. """
        dbc = self.getConn()
        dbc[1].execute("SHOW TABLES;")
        tList = dbc[1].fetchall()
        tNamesOut = []
        for row in tList:
            if isinstance(row[0], str):
                tNamesOut.append(row[0])
            else:
                tNamesOut.append(bytes(row[0]).decode("utf-8"))
        return tNamesOut

    def getTableColumns(self, tableName: str) -> dict:
        """
        Return a dict of the column names for the given table, where keys are column names and values are data types.
        Raises mysql.connector.errors.ProgrammingError if the table does not exist.
        :return: A dict in format: {colName, colType}
        """
        def decodeDefault(valIn):
            if isinstance(valIn, bytes):
                return valIn.decode("utf-8")
            elif isinstance(valIn, str):
                return valIn
            elif valIn is None:
                return None
            return str(valIn)

        getTable = self.dbSelect(f'SHOW COLUMNS FROM `{tableName}`;')
        return {
            str(col['Field']): [
                str(col['Type'].decode('utf-8')),
                str(col['Null']),
                decodeDefault(col['Default']),
                str(col['Extra'])
            ] for col in getTable
        }

    def getCurrentDBStructure(self):
        """
        Gather and return a dictionary representation of the current database structure.
        :return: {tableName: {colName: [type: str, notNull: 'YES'|'NO', default: str|None, extra: str], ...}, ...}
        """
        return {table: self.getTableColumns(table) for table in self.fetchTableList()}

    def createDataBackup(self, outputFolderPath: str):
        """
        Create a backup of the data from all tables in the database.
        :param outputFolderPath: The name of the folder where the backup zip archive should be written. Path should
        end with a trailing slash.
        :return: The path to the backup zip archive.
        """

        tableNames = self.fetchTableList()

        # create a backup of the database tables and structure
        tableFiles = {'tableStructure': tempfile.TemporaryFile('w+')}

        # write table structure to file
        tableFiles['tableStructure'].write(json.dumps(self.getCurrentDBStructure()))
        tableFiles['tableStructure'].seek(0)

        # backup the data for each table file to the temp files (save to tmp to limit in-memory size)
        for tName in tableNames:
            # create temp files
            tableFiles[tName] = tempfile.TemporaryFile('w+')

            # fetch the data from the database
            tableData = self.dbSelect(f'SELECT * FROM `{tName}`;')

            # skip writing for empty tables
            if len(tableData) == 0:
                continue

            # write the dictionary to the file
            writer = csv.DictWriter(tableFiles[tName], fieldnames=list(dict.keys(tableData[0])))
            writer.writeheader()
            writer.writerows(tableData)
            tableFiles[tName].seek(0)

        # compile this table data into a single zip archive
        cTime = datetime.datetime.now()
        backupFileName = (f'backup_{cTime.year}{cTime.month:02d}{cTime.day:02d}_' +
                          f'{cTime.hour:02d}:{cTime.minute:02d}:{cTime.second:02d}.zip')

        with zipfile.ZipFile(outputFolderPath + backupFileName, 'w') as f:
            for tName in tableFiles:
                f.writestr(tName, tableFiles[tName].read(), compress_type=zipfile.ZIP_DEFLATED)
                tableFiles[tName].close()

        return outputFolderPath + backupFileName

    def restoreDataBackup(self, backupFilePath: str):
        """
        Restore the database to the state described in the backup file.

        Scan for the tables and validate the columns, then delete all current entries and re-populate with the
        data from the backup file.
        :param backupFilePath: String path to the backup file from which to restore the database.
        """
        if not os.path.exists(backupFilePath):
            raise Exception('The provided backup file does not exist')

        with tempfile.TemporaryDirectory() as tempDir:
            # dump the zip archive to a temporary directory
            with zipfile.ZipFile(backupFilePath, 'r') as backupArchive:
                backupArchive.extractall(tempDir)

            tableStructurePath = tempDir + '/tableStructure'
            if not os.path.exists(tableStructurePath):
                raise Exception('The table structure file does not exist')

            with open(tableStructurePath, 'r') as f:
                targetTables = json.loads(f.read())

            # validate that current structure matches the target structure
            compareResult = self.compareDBToStructure(targetTables)
            if len(compareResult['add']) != 0 or len(compareResult['edit']) != 0:
                # perform changes to the database structure
                self.applyChangesToDBStructure(compareResult)

            # restore the data from the backup
            for table in targetTables:
                if not os.path.exists(tempDir + '/' + table):
                    raise Exception(f'Table `{table}` is missing from the backup files.')

                self.dbQuery(f'DELETE FROM `{table}`;')

                with open(tempDir + '/' + table, newline='') as csvfile:
                    reader = csv.DictReader(csvfile)
                    dataRows = [row for row in reader]
                if len(dataRows) == 0:  # guard against empty tables
                    print(f'Table `{table}` empty')
                    continue

                colsToInsert = ', '.join([f'`{x}`' for x in dataRows[0]])
                valuePlaceholders = ', '.join(['%s'] * len(dataRows[0]))
                insertDataQ = f'INSERT INTO `{table}` ({colsToInsert}) VALUES ({valuePlaceholders})'
                idCursor = self.conn.cursor()
                idCursor.executemany(
                    insertDataQ,
                    [dict.values(x) for x in dataRows]
                )
                print('Table restored:', table)
        print('Database restored from backup:', backupFilePath)

    def compareDBToStructure(self, targetStructure: dict):
        """
        Create a description of changes needed for the database to conform to the target structure.
        :param targetStructure: Dictionary describing the target structure. Format:
            {tableName: {col1: targetStructureCol, ...}, ...} where
            targetStructureCol: [type: str, allowNull: 'YES'|'NO', default: str|None, extra: str]
        :return: {add: dict, edit: dict} where add and remove dicts follow format:
            {tableName: '' | {colName: '' | targetStructureCol}, ...}  if edit[tableName] == '' then remove the table.
            if edit[tableName][colName] == '' remove the column.  if colName is targetStructureCol, update the column.
        """

        # load the current table list, including columns
        currentStructure = self.getCurrentDBStructure()

        dbDiff = {'add': {}, 'edit': {}}

        # find tables and columns to be removed
        for tableName in list(dict.keys(currentStructure)):
            # check if table need to be removed
            if tableName not in targetStructure:
                dbDiff['edit'][tableName] = ''
                continue

            dbDiff['edit'][tableName] = {}

            # check if columns need to be removed
            for colName in list(dict.keys(currentStructure[tableName])):
                # check if column needs to be removed
                if colName not in targetStructure[tableName]:
                    dbDiff['edit'][tableName][colName] = ''

                elif currentStructure[tableName][colName] != targetStructure[tableName][colName]:
                    # mark column as 'needs edit' to conform
                    dbDiff['edit'][tableName][colName] = targetStructure[tableName][colName]

            # remove table key from edit if no edits are needed
            if len(dbDiff['edit'][tableName]) == 0:
                del dbDiff['edit'][tableName]

        # find tables that need to be added
        for tableName in list(dict.keys(targetStructure)):
            # add complete missing tables
            if tableName not in currentStructure:
                dbDiff['add'][tableName] = targetStructure[tableName]
                continue

            # add only missing columns
            dbDiff['add'][tableName] = {}
            for colName in list(dict.keys(targetStructure[tableName])):
                if colName not in currentStructure[tableName]:
                    dbDiff['add'][tableName][colName] = targetStructure[tableName][colName]

            # remove table key from edit if no additions are needed
            if len(dbDiff['add'][tableName]) == 0:
                del dbDiff['add'][tableName]

        return dbDiff

    def applyChangesToDBStructure(self, dbDelta: dict):
        """
        Applies changes to the database structure specified by [dbDela].  dbDelta format comes from output of the
        function compareDBToStructure()
        :param dbDelta: Dictionary describing changes needed for database structure. Format from compareDBToStructure():
            {tableName: '' | {colName: '' | targetStructureCol}, ...}  if edit[tableName] == '' then remove the table.
            if edit[tableName][colName] == '' remove the column.  if colName is targetStructureCol, update the column.}
        """

        currentStructure = self.getCurrentDBStructure()

        def buildColModSQL(nameOfCol: str, cData: list):
            """ Build the SQL used to create a column. """
            b_nullText = '' if cData[1] == 'YES' else 'NOT'
            b_defaultText = '' if cData[2] is None else f'DEFAULT \'{cData[2]}\''
            return f'`{nameOfCol}` {cData[0]} {b_defaultText} {cData[3]} {b_nullText} NULL'

        # process edits and deletions
        for tableName in dbDelta['edit']:
            if dbDelta['edit'][tableName] == '':
                self.dbQuery(f'DROP TABLE `{tableName}`;')
                continue

            for colName in dbDelta['edit'][tableName]:
                if dbDelta['edit'][tableName][colName] == '':
                    self.dbQuery(f'ALTER TABLE `{tableName}` DROP COLUMN `{colName}`;')
                else:
                    editColQ = f'ALTER TABLE `{tableName}` MODIFY COLUMN ' + buildColModSQL(
                        colName,
                        dbDelta['edit'][tableName][colName]
                    )
                    self.dbQuery(editColQ)

        # process table and column additions
        for tableName in dbDelta['add']:
            if tableName not in currentStructure:
                # add entire table + columns
                createTableQ = f'CREATE TABLE `{tableName}` ('

                hadAutoIncrement = ''
                for colName in dbDelta['add'][tableName]:
                    colData = dbDelta['add'][tableName][colName]

                    if 'auto_increment' in colData[3]:
                        hadAutoIncrement = colName

                    createTableQ += buildColModSQL(colName, colData) + ', '

                # remove trailing space and comma
                createTableQ = createTableQ[:-2]

                # add primary key for the auto increment column.
                if hadAutoIncrement != '':
                    createTableQ += f', PRIMARY KEY (`{hadAutoIncrement}`)'

                createTableQ += ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;'
                self.dbQuery(createTableQ)
                continue

            # add only missing columns
            for colName in list(dict.keys(dbDelta['add'][tableName])):

                addColQ = f'ALTER TABLE `{tableName}` ADD ' + buildColModSQL(
                    colName,
                    dbDelta['add'][tableName][colName]
                )
                self.dbQuery(addColQ)

