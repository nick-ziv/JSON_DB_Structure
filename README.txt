JSON Database Structure

This tool is used for applying a target structure to a database given the structure as a JSON object.
The database supported are MySQL and MariaDB.

This tool can save time over manually creating the database structure while improving the accuracy
of a conversion or installation.

The tool requires credentials for the database which have permission to perform the required operations,
such as create table, delete table, update table, edit columns, etc.

The format for the database structure JSON document is as follows:

{
    "table1Name": {
        "col1Name": [type: string, allow_null: 'YES'|'NO', default_value: string|null, extra: string],
        "col2Name": [...]
    },
    "table2Name": {...}
}

Column data notes:
- The column data is an array, not an object.
- 'extra' can be set to auto_increment or an empty string.  (Maximum of one auto increment column per table)
- Examples for 'type': "varchar(100)",  "int", "text"

Usage 

1: Fill out the credentials for the database in the dbCreds.json file.

2: Place the target structure JSON file you created into the directory with main.py.  Make sure the file has
a .json extension.

3: Install the python dependencies from requirements.txt

4: Run main.py

5: Follow the program's instructions to apply the new database structure.
