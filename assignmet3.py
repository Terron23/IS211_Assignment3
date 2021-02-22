import urllib.request
import datetime
import argparse
import csv
import re
import requests

CSV_URL = 'http://samplecsvs.s3.amazonaws.com/Sacramentorealestatetransactions.csv'




#http://s3.amazonaws.com/cuny-is211-spring2015/weblog.csv

def downloadData(url):
    """
    Function - downloads data from a csv via url
    args:
        url - api to call in order to download csv
    vars:
        context - Added to circumvent an ssl error in the the urlib package
        csv_data - data from url in binary format
        csv_content - data from url in human readable format
    """
    with requests.Session() as s:
        download = s.get(url)

        decoded_content = download.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
       
    return list(cr)


def processData(data):
    main_ls  = []
    cols = ["file_type", "datetime_accessed", "browser", "status", "bytes"]
    for row in data:
        
        day = datetime.datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
        # dayformat = ls[1].split(" ")[0]
        #f'{day.month}/{day.day}/{day.year}'
        # ls.append(dayformat)
        d1=zip(cols, row)
        obj = dict(d1)
        obj["day"] = day
        
        main_ls.append(obj) 
      
    return main_ls

def searchImageData(data):
    total = len(data)
    image_count = 0
    for obj in data:
        match = re.search(r"\.(?:png|jpg|gif)$", obj["file_type"].lower())
        if match:
            image_count += 1
    print(f'Image requests account for {image_count / total * 100}% of all requests total')
    return image_count / total 



def searchBrowserData(data):
    browser_obj ={}
    for obj in data:
        browser = [[m.group(1), m.group(2)] for m in re.finditer(r'(?i)(firefox|msie|chrome|safari)[\/\s]([\d.]+)', obj["browser"])]
        if browser:
            if browser[0][0] in browser_obj.keys():
                browser_obj[browser[0][0]] = browser_obj[browser[0][0]] + 1
            else:
                browser_obj[browser[0][0]] =  1
    
    for k, v in browser_obj.items():
        print(f'{k} has {v} requests')
    
    print(f"The most popular browser is {max(browser_obj, key=browser_obj.get)}")
    return max(browser_obj, key=browser_obj.get)

def hitsPerHour(data):
    hours_obj = {}
    for obj in data:
        hours = obj["day"].hour
        if hours in hours_obj.keys():
            hours_obj[hours] = hours_obj[hours] + 1
        else:
            hours_obj[hours] =  1

    for k, v in hours_obj.items():
        print(f'Hour {k} has {v} hits')
    return hours_obj
            
def main(url):
    csv_data = downloadData(url)
    process_data = processData(csv_data)
    searchImageData(process_data)
    searchBrowserData(process_data)
    hitsPerHour(process_data)    

            

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.set_defaults(method = downloadData)
    parser.add_argument('--url', type = str, help="Url String")
    args = parser.parse_args()
    main(args.url)
