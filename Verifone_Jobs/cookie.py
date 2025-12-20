import re
import os, sys
import json
import pymongo
import requests
import xmltodict
from pymongo import MongoClient
from bson.objectid import ObjectId

client = pymongo.MongoClient("mongodb+srv://rtnsmart:elneebekRf3dH30z@rtn.gfl4v.mongodb.net/")
db = client["verifone"]
store = db["stores"]

stores = ['dcrStat', 'fpHoseTest', 'blendProduct', 'summary', 'department', 'tax', 'netProd', 'hourly', 'network', 'deal', 'plu', 'pluPromo', 'popDisc',
          'fpHose', 'fpHoseRunning', 'prPriceLvl', 'slPriceLvl', 'tierProduct', 'autoCollect', 'tank', 'tankMonitor',
          'tankRec', 'fpDispenser', 'allProd', ]

storescoll = ['dcrst', 'fptHose', 'blend', 'summary', 'department', 'tax', 'netProd', 'hourly', 'network', 'deal', 'plu', 'pluPromo', 'popDisc',
              'fpHose', 'fpHoseRunning', 'prPriceLvl', 'slPriceLvl', 'tierProduct', 'autoCollect', 'tank', 'tankMonitor',
              'tankRec', 'fpDispenser', 'allProd', ]

# Hardcoded values
storeid = "66337c59c31dc3e37229f275"

store_document = store.find_one({"_id": ObjectId(storeid)})

pos_username = store_document.get("pos_username")
pos_password = store_document.get("pos_password")

corpid = {"corpid":"66542a1557028503d23e93d9", "storeid": storeid ,"userid": f"{pos_username}" ,"paswrd": f"{pos_password}"}
# First link
xmlinput = f"http://192.168.31.11/cgi-bin/CGILink?cmd=validate&user={pos_username}&passwd={pos_password}"

def xmlfilename(xmlinput):
    response = requests.get(xmlinput)
    data = xmltodict.parse(response.content)
    # print(data)
    for i in range(0, len(stores)):
        print(stores[i])
        for reptnum in range(2, 21):  # loop over desired reptnum values
            url2 = "http://192.168.31.11/cgi-bin/CGILink?cmd=vrubyrept&reptname={0}&period=2&reptnum={1}&cookie={2}".format(
                stores[i], reptnum, (data["domain:credential"]["cookie"]))
            # print(data["domain:credential"]["cookie"])

            response = requests.get(url2)
            data2 = xmltodict.parse(response.content)
            # print(data2)
            s = json.dumps(data2)

            spcl_chrt = re.sub(r"[@]", "", s)
            cln_txt = json.loads(spcl_chrt)

            cln_txt["pd:{}Pd".format(storescoll[i])].update(corpid)
            # print(cln_txt["pd:{}Pd".format(storescoll[i])]["corpid"])
            # print(cln_txt)
            # mydb = client['{0}'.format(cln_txt["pd:{}Pd".format(storescoll[i])]["vs:site"])]
            mydb = client['verifone']
            coll = mydb["{}".format(stores[i])]
            # Create a query to check for the existence of the document
            query = {"pd:{}Pd".format(storescoll[i]): cln_txt["pd:{}Pd".format(storescoll[i])]}
            if coll.find_one(query) is None:
                coll.insert_one(cln_txt)
                print(f"Inserted document for {stores[i]} with reptnum {reptnum}")
            else:
                print(f"Document for {stores[i]} with reptnum {reptnum} already exists")

try:
    xmlfilename(xmlinput)
    print("**Loaded Successfully**")

except Exception as e:
    print(e)