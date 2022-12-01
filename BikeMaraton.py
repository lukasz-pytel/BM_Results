from multiprocessing.util import is_exiting
from sqlite3 import Timestamp
import tabnanny
import pandas as pd
import re
import requests
import warnings
import datetime
 


pd.set_option('mode.chained_assignment',None )
warnings.simplefilter(action='ignore', category=FutureWarning)


datetime_object = str(datetime.datetime.now())
datetime_object = datetime_object.replace(':','.',10)
filename = 'BM_'+datetime_object+'.xlsx'

numerWyscigu = input('Wprowadź numer wyścigu: ')
#Grupa 2 : MEGA
#Grupa 3 : CLASSIC
grupy = {2: 'MEGA', 3:"Classic"}
od = 0
step = 200
max = 2000 
url_prefix = 'https://wyniki.datasport.pl/results'+str(numerWyscigu)+'/'
url_sufix = 'src/lista.php?grupa='
url_sufix_2 = '&co=open&od='
url_array = []


tab_mega = ''
tab_classic = ''

for grupa in grupy:
    for i in range(0,2000,200):
        url = url_prefix + url_sufix +  str(grupa)  + url_sufix_2 + str(i) 
        ru = requests.get(url)
        if ru.text != "":
            if grupa == 2:
                tab_mega = tab_mega + ru.text
            if grupa == 3:
                tab_classic = tab_classic + ru.text


def get_sec(time_str):
    try:
        h, m, s = time_str.split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)
    except:
        print ('throw: ' + time_str)




def BM(tab, grupa):
    tab = '<table>' + tab + '</table>'
    
    #czyszczenie danych
    sp_list = pd.read_html(tab)
    df = (sp_list[0])
    df =  df[(df['CzasTime'] != 'DNS')]
    df =  df[(df['CzasTime'] != 'DNF')]
    df =  df[(df['CzasTime'] != 'DSQ')]
    df =  df[(df['MscPos'] != 'NZ')] 


    #Nadanie nazw klumn
    df.columns = ['Miejsce', 'Kraj','Nazwa', 'Klub','Rocznik', 'Ranking','Czas', 'NaN']
   
    #Czyszczenie zbędnych kolumn
    df.pop('NaN')
    df.pop('Kraj')
    df.pop('Miejsce')  
    
    #Wyciągnięcie kategorii
    df['Kat'] = df['Ranking']
    i=0
    for item in df['Kat']:
        items = [item[0:3]]
        items = str(items[0]).strip()
        df.loc[i,'Kat'] = items
        i=i+1

    #Dystans
    if grupa == 'MEGA':
        df['Dystans'] = 'MEGA'
    else:
        df['Dystans'] = 'CLASSIC'       

    #Wyciąnięcie numerów
    df['Numer'] = df['Nazwa']
    i=0
    for element in df['Numer']:
        element = re.findall('[0-9]+', element)
        if element[0].isdigit():
            #print (str(element[0]))
            df.loc[i,'Numer'] = str(element[0])
        i=i+1


    #Punkty OPEN
    df['Punkty'] = 0
    df = df.astype({'Punkty': int})

    df['PunktySektorOpen'] = 0.0
    df = df.astype({'PunktySektorOpen': float})

    bestPointsOpen = get_sec(df['Czas'][0])
    #print (bestPointsOpen)
    i=0
    for item in df['Czas']:
        secound = get_sec(item[:8])
        df.loc[i,'Punkty'] = secound
        df.loc[i,'PunktySektorOpen'] =  bestPointsOpen / secound 
        if grupa != 'MEGA':
            df.loc[i,'PunktySektorOpen'] = df.loc[i,'PunktySektorOpen'] * 0.920
        i=i+1
    

    #inicjacja dataframe
    lst = []
    df_all = pd.DataFrame(lst, columns=['Miejsce', 'Nazwa', 'Klub', 'Rocznik', 'Ranking', 'Czas', 'Kat','Numer', 'Punkty', 'PunktySektorOpen', 'PunktyKategoria','WspolczynnikKat'])

    kategorie = ['M0', 'M1', 'M2','M3', 'M4', 'M45', 'M5', 'M55', 'M6', 'M65', 'M7','KM','K0', 'K1', 'K2','K3', 'K4', 'K45', 'K5', 'K55', 'K6', 'K65', 'K7']

    df['PunktyKategoria'] = 0
    df['WspolczynnikKat'] = 0.00  
    df = df.astype({'PunktyKategoria': float})
    df = df.astype({'WspolczynnikKat': float})

    i=0
    for items in kategorie:
        result = df[(df['Kat'] == items)]
        result.reset_index(inplace = True, drop = True)

        i+=1
        result['Czas'].astype(str)
        if len(result) > 0:
            bestInCategory = get_sec(str(result['Czas'].values[0]))
            if grupa == 'MEGA':
                if items[0:1] == 'K':
                    punkty = 700
                else:
                    punkty = 500
            else: 
                punkty = 300
            j=0
            for element in result['Czas']:
                sec = get_sec(str(element[:8]))
                #print(str(j) +'-'+ items +'-'+ str(sec) +'-'+ str(bestInCategory))
    
                result.loc[j,'PunktyKategoria'] = (bestInCategory / sec) * punkty
                result.loc[j,'WspolczynnikKat'] = (bestInCategory / sec)
                j=j+1
               # print (result)       
        df_all = pd.concat([df_all, result])

        df_all.reset_index(inplace = True, drop = True)

    return df_all


result1 = BM(tab_classic,'claccic')
result2 = BM(tab_mega,'MEGA')

result = pd.concat([result1,result2])
result['Miedzyczasy'] = url_prefix + 'times.php?numer=' + result['Numer']
result.reset_index(inplace = True, drop = True)

result_druzyny = result.groupby(['Klub'], as_index=False)['PunktyKategoria'].agg(['sum', 'mean', 'count'])#.apply(lambda x: x)
result_druzyny.sort_values(by='count',inplace=True, ascending=False)
result_druzyny.columns = [ 'suma', 'średnia', 'ilość zawodników na mecie']


def kondycja(cell):
    highlight='background-color: red;'
    if cell == 'Kondycja. Net Team':
        return highlight
    else:
        return ''


#result = result.style.applymap(kondycja, subset=['Klub'])
#result_druzyny = result_druzyny.style.applymap(kondycja, subset=['Klub'])

print (result)
print(result_druzyny)

#Przygotuj Excela
with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:  
    result.to_excel(writer, sheet_name='Indywidualne', index=True)
    result_druzyny.to_excel(writer, sheet_name='Druzynowe', index=True )


