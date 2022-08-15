"""
Nicholas Zivkovic, 2022

JSON Database Structure Tool
"""

import json
import dbFuncs
import os

def interactiveProcess():
    with open('dbCreds.json', 'r') as f:
        dbCreds = json.loads(f.read())

        print('Creating database connection')

        dbConn = dbFuncs.DBConn(
            dbCreds['host'],
            dbCreds['user'],
            dbCreds['user_password'],
            dbCreds['database'],
            dbCreds['port'],
            dbCreds['auth_plugin']
        )

        print('Database connection established')
        
        # find a list of JSON files that could be the target JSON structure
        possibleFiles = list(filter(lambda fName: fName[-5:] == '.json', os.listdir()))
        possibleFiles.remove('dbCreds.json')
        
        if len(possibleFiles) == 0:
            print('There are no files in this directory that can be used as the target structure file.')
            return

        print('Possible target structure JSON files found:')
        
        for ind, fileName in enumerate(possibleFiles):
            print('(', ind, ')', fileName)
        
        print('Enter the number next to the JSON file you want ot use for the target structure:')

        useFile = possibleFiles[int(input('> '))]

        with open(useFile, 'r') as f:
            newStructure = json.loads(f.read())

        print('Using the file "', useFile, '" as the target structure JSON file.')
        print('Please confirm you want to apply the target structure to the database.  This action cannot be undone.')
        if input('Type confirm to continue > ') != 'confirm':
            print('Aborting')
            return

        print('Applying target structure...')

        structureDiff = dbConn.compareDBToStructure(newStructure)
        dbConn.applyChangesToDBStructure(structureDiff)

        print('Done applying the target structure to the database.')


if __name__ == "__main__":
    interactiveProcess()
