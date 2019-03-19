import time
import datetime
import pandas as pd
from sqlsrvrHandler import SQLServer

from colorama import init; init()
from colorama import Fore, Back, Style
logWarn = '\n[LOG]' + Fore.YELLOW + Style.BRIGHT + '[WARNING] ' + Style.RESET_ALL

#-------------------------------------------------------------------------------------------
print(Fore.CYAN + Style.BRIGHT)
print('-----------------------')
print('POPULATION METRICS TOOL')
print('-----------------------')
print(Style.RESET_ALL)
print(datetime.datetime.now().strftime('%m/%d/%Y - %H:%M:%S'))

serverConnect = True
tableConnect = True

while True:

        while serverConnect:

                SERVER = input('\nSERVER : ')
                DATABASE = input('DATABASE : ')

                spellCheck = True
                while True:
                        startTime = time.time()
                        dT = datetime.datetime.now()
                        print(dT.strftime('\n%H:%M:%S\n'))
                        
                        try:
                                CNXN = SQLServer(SERVER, DATABASE)
                                print('\n[LOG] ' + Fore.GREEN + Style.BRIGHT + 'Connected' + Style.RESET_ALL)
                                serverConnect = False
                                break
                        except:
                                if spellCheck:
                                        spellCheck = input('\nIs SERVER and DATABASE spelled correctly? ( Y / N ) : ')
                                        if spellCheck.upper() == 'Y':
                                                spellCheck = False
                                                continue
                                        elif spellCheck.upper() == 'N':
                                                break            
                                print(logWarn + 'SQL Server Time Out - Trying Again in 1 Minute')
                                time.sleep(60)
                                continue

        if tableConnect:
                TABLE = input('\nTABLE : ')
                tableConnect = False

        COLUMN = input('\nCOLUMN : ')

        try:
                for tableIdx, table in enumerate([TABLE]):

                        print('\n[LOG] Profiling Table: {}'.format(table))

                        queryStart = time.time()

                        query = 'select {} \
                                from {}.{}'.format(COLUMN, DATABASE, table)

                        chunk = 10000
                        data = CNXN.queryToDF(query, chunksize = chunk)

                        queryEnd = time.time()
                        print('\nQuery Execution: {}'.format(queryEnd - queryStart))

                        for idx, df in enumerate(data):
                                print('Chunk: {}'.format(idx + 1), end = '\r', flush = True)

                                nullCount = df.isna().sum()
                                zeroCountNum = (df == 0).sum()
                                zeroCountChar = (df == '0').sum()
                                zeroCount = zeroCountNum + zeroCountChar

                                populated = -(nullCount + zeroCount) + len(df.index)

                                countedData = pd.concat([populated, nullCount, zeroCount], axis = 1, join = 'outer')
                                countedData.insert(0, range(len(df.index)), len(df.index))
                                countedData.columns = ['total', 'populated', 'nullCount', 'zeroCount']

                                if idx == 0:
                                        outData = countedData
                                else:
                                        outData = outData.add(countedData, fill_value = 0)

                        analysisEnd = time.time()

                        print()
                        print(Back.GREEN)
                        print()
                        print(outData)
                        print(Style.RESET_ALL)

                        # csvCheck = input('Save Data to .csv? ( Y / N ) : ')
                        # if csvCheck == 'Y':
                        #     outData.to_csv('{}_{}.csv'.format(table, COLUMN), header = True)

                        print('\nAnalysis Execution: {}\n'.format(analysisEnd - queryEnd))
        except:
                print(logWarn + 'Invalid Table / Column Name')

        endTime = time.time()

        print('[LOG] Analysis Complete')
        print('Total Execution Time: {}'.format(endTime - startTime))

        cont = input('\nCONTINUE? ( Y / N ) : ')
        if cont.upper() == 'Y':
                newTable = input('Do you want to profile a different table? ( Y / N ) : ')
                if newTable.upper() == 'Y':
                        tableConnect = True
                        newServer = input('Is a different server required? ( Y / N ) : ')
                        if newServer.upper() == 'Y':
                                serverConnect = True
                continue
        elif cont.upper() == 'N':
                break

print(Fore.YELLOW + Style.BRIGHT)
input('PRESS ENTER TO EXIT')
print(Style.RESET_ALL)