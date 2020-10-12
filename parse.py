import requests
from  bs4 import BeautifulSoup
import subprocess
import csv
import os
from datetime import datetime
import re

exclude = ["собственник", "хозяин", "не агент", "не агентство", "без посредников", "хозяйка"]

def __clean(x):
    # функция разбора главной строки объявления, делим на 5 столбцов
    arr = x.replace('(', ',').replace(')', '').split(',')
    arr = list(map(lambda x: x.strip(), arr))
    if arr[-1] in exclude:
        a5 = arr.pop(len(arr) - 1)
    else:
        a5 = ''
    arr = list(filter(lambda x: not x in exclude, arr))
    a1 = arr.pop(0)
    a4 = arr.pop(len(arr) - 1)
    if len(arr) > 1:
        a2 = arr.pop(0)
        a3 = ' '.join(arr)
    elif len(arr) == 1:
        if arr[0].find("ЖК") >=0:
            a2 = arr[0]
            a3 = ''
        else:
            a2 = ''
            a3 = arr[0]
    else:
        a2 = ''
        a3 = ''
    return [a1,a2,a3,a4,a5]

def __clear_data(path):
    subprocess.call(['spark-submit', 'clear_data.py', path], shell=True)

def __append_row(path, row):
    with open(path, 'a') as file:
        writer = csv.writer(file)
        writer.writerow(row)

def __parse_page(url, page_num):

    result = requests.get(url.format(page_num))
    soup = BeautifulSoup(result.content, 'html.parser')
    resultApartsList = []
    apartsList = soup.select('tr')
    if len(apartsList) == 0:
        return -1

    for apart in apartsList:
        resultrow = []
        try:
            resultrow.append(re.search("([0-9]{2}.[0-9]{2}.[0-9]{4})"\
                ,apart.select_one('td#date')['title']).group())
            resultrow.append(__clean(re.sub(r'\n', '', apart.select_one('h2#title').text)))
            resultrow.append(apart.select_one('td#price')\
                .select_one('span[itemprop="price"]')['content'])
            resultrow.append(apart.select_one('td#district').select_one('a').text)
            resultrow.append(re.sub(r'[\n\r\xa0;]', '', apart.select_one('td#area').text))
            resultrow.append(apart.select_one('td#cont')\
                .select_one('link[itemprop="sku"]')['content'])
            resultApartsList.append(resultrow)            
        except Exception:
            pass

    return resultApartsList


def get_data(page_num, filePath):

    url = "https://neagent.info/krasnodar/prodam-odno-komnatnuyu-kvartiru/?page={}&"

    for current_page in range(1, page_num +1):

        result = __parse_page(url,current_page)

        if result == -1:
            break
        else:
            for row in result:
                __append_row(filePath, row)
		
        print(str(current_page) + '='*20)
     
def getNewData(page_num):
    dirname = r'results/' + datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
    os.mkdir(dirname)

    get_data( page_num, dirname+'/result.csv')
    __clear_data(dirname)