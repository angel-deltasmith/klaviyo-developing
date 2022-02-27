import requests
import json
import pandas
from io import StringIO
import re

headers = {"Accept": "application/json"}
events = "https://a.klaviyo.com/api/v1/metrics/timeline?since=2022-02-01&count=500&sort=desc&api_key=pk_296e06abb5be056c96a767ba5b7db01abc"
events_json = requests.request("GET", events, headers=headers).json()

with open('events_json_original.json', 'w') as json_file:
    json.dump(events_json, json_file,indent=4, sort_keys=True)

json_data_list = events_json['data']

with open('events_json_data_list.json', 'w') as json_file:
    json.dump(json_data_list, json_file,indent=4, sort_keys=True)

json_new = []
schema = ['datetime','event_name','event-properties','id','object','person-inf','statistic_id','timestamp','uuid']
schema_event_properties = ['event_id','message','variation','CampaignName','ClientName','ClientOS','EmailDomain','Subject']
for pos in json_data_list:
    # now song is a dictionary
    element = {}
    for key, value in pos.items():
        #print(key)
        #print(type(key))
       # if key == 'event_properties':
            #print(type(key))

        key = key.replace(" ","")
        key = key.replace("$","")
        element[key] = value
        json_new.append(element)

#print(json_new)
with open('events_json_new_clean.json', 'w') as json_file:
    json.dump(json_new, json_file,indent=4, sort_keys=True)


json_data = []
for row in json_data_list:
    element = {}
    #print(row)
    for key in schema:
        k = key.split("-")
        if len(k) == 1:
            element[k[0]] = row[k[0]]
            
        else:
            if k[0] == 'event':
                list_tem = row[k[0]+"_"+k[1]];
                element_nested = {}
                for key,value in list_tem.items():
                    
                    key = key.replace(" ","")
                    key = key.replace("$","")
                    element_nested[key] = value
                    #print(key+ ":"+ value)
                    print(element_nested)
                
                element[k[0]+"_"+k[1]] = element_nested
            else:
                element[k[0]+"_"+k[1]] = row[k[0]]

    json_data.append(element)

with open('events_jesus.json', 'w') as json_file:
    json.dump(json_data, json_file,indent=4, sort_keys=True)