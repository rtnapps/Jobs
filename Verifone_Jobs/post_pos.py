import os, sys, json, time
import requests, xmltodict, pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId

client = pymongo.MongoClient("mongodb+srv://rtnsmart:elneebekRf3dH30z@rtn.gfl4v.mongodb.net/")

corpid = "66542a1557028503d23e93d9"
storeid = "66337c59c31dc3e37229f275"

print("Started")
while True:
    try:
        db = client["verifone"]  # DB Name
        col = db["send_item_to_pos"]  # Collection Name
        pb_coll = db["vfpricebook"]  # Collection Name

        # Debugging: Print the query being used
        print("Querying for pending documents...")
        f_data = col.find_one({"Status": "Pending", "storeid": storeid}, sort=[("created_at", pymongo.DESCENDING)])

        # Debugging: Print the fetched document
        if f_data:
            print("Fetched document:", f_data)
        else:
            print("No pending document found.")
            
        # exit()
        
        if f_data:
            _idd = f_data['_id']
            required_keys = ['get_cookies_url', 'xml_url', 'post_data_url', 'corpid', 'storeid']
            if all(key in f_data for key in required_keys):
                cooki_url = f_data['get_cookies_url']
                xml_url = f_data['xml_url']
                frst_post = f_data['post_data_url']
                crop_id = f_data['corpid']
                store_id = f_data['storeid']

                if crop_id == corpid and store_id == storeid:
                    response = requests.get(cooki_url)
                    data = xmltodict.parse(response.content)
                    response1 = frst_post + data["domain:credential"]["cookie"]
                    f = requests.get(xml_url)
                    post_response = requests.post(response1, data=f.content)
                    print("XML UPDATED SUCCESSFULLY")

                    if post_response.status_code == 200:
                        post_response_data = xmltodict.parse(post_response.content)
                        if "VFI:Fault" in post_response_data["VFI:Response"]:
                            fault = post_response_data["VFI:Response"]["VFI:Fault"]
                            fault_code = fault["faultCode"]
                            fault_string = fault["faultString"]
                            print(f"XML UPDATE FAILED: {fault_code} - {fault_string}")
                            col.update_one(
                                {'_id': ObjectId(_idd)},
                                {'$set': {'Status': "Failed", 'Response': f"XML UPDATE FAILED: {fault_code} - {fault_string}"}}
                            )
                        else:
                            print("XML UPDATED SUCCESSFULLY")
                            col.update_one(
                                {'_id': ObjectId(_idd)},
                                {'$set': {'Status': "Success", 'Response': "Success"}}
                            )
                            print("Document with _id {} updated".format(_idd))

                            # Define the field mapping between dataobject and vfpricebook
                            field_mapping = {
                                "poscode": "POSCode",
                                "price": "RegularSellPrice"
                            }

                            # Check action and update vfpricebook collection
                            if f_data['action'] == "Bulk Add":
                                for item in f_data['dataobject']:
                                    pos_code = item['POSCode']
                                    store_id = item['storeid']
                                    existing_item = pb_coll.find_one({"POSCode": pos_code, "storeid": storeid})
                                    
                                    if existing_item:
                                        update_fields = {field_mapping[k]: v for k, v in item.items() if k in field_mapping}
                                        pb_coll.update_one(
                                            {"_id": existing_item["_id"]},
                                            {"$set": update_fields}
                                        )
                                        print(f"Updated item with POSCode {pos_code} in vfpricebook")
                                    else:
                                        new_item = {field_mapping[k]: v for k, v in item.items() if k in field_mapping}
                                        new_item["POSCode"] = pos_code
                                        new_item["storeid"] = storeid
                                        pb_coll.insert_one(new_item)
                                        print(f"Inserted new item with POSCode {pos_code} in vfpricebook")

                            elif f_data['action'] == "Item Add":
                                pos_code = f_data['dataobject']['POSCode']
                                store_id = f_data['dataobject']['storeid']
                                existing_item = pb_coll.find_one({"POSCode": pos_code, "storeid": storeid})
                                
                                if existing_item:
                                    update_fields = {field_mapping[k]: v for k, v in f_data['dataobject'].items() if k in field_mapping}
                                    pb_coll.update_one(
                                        {"_id": existing_item["_id"]},
                                        {"$set": update_fields}
                                    )
                                    print(f"Updated item with POSCode {pos_code} in vfpricebook")
                                else:
                                    new_item = {field_mapping[k]: v for k, v in f_data['dataobject'].items() if k in field_mapping}
                                    new_item["POSCode"] = pos_code
                                    new_item["storeid"] = storeid
                                    pb_coll.insert_one(new_item)
                                    print(f"Inserted new item with POSCode {pos_code} in vfpricebook")

                            elif f_data['action'] == "Item Update":
                                item_id = f_data['dataobject']['_id']['$oid']
                                update_fields = {field_mapping[k]: v for k, v in f_data['dataobject'].items() if k in field_mapping}
                                pb_coll.update_one(
                                    {"_id": ObjectId(item_id)},
                                    {"$set": update_fields}
                                )
                                print(f"Updated item with _id {item_id} in vfpricebook")

                            elif f_data['action'] == "Item Delete":
                                item_id = f_data['dataobject']['_id']['$oid']
                                pb_coll.update_one(
                                    {"_id": ObjectId(item_id)},
                                    {"$set": {"ActiveFlag": "no"}}
                                )
                                print(f"Updated item with _id {item_id} to set ActiveFlag to no in vfpricebook")

                            elif f_data['action'] == "Bulk Update":
                                for item in f_data['dataobject']:
                                    pos_code = item['poscode']
                                    update_fields = {field_mapping[k]: v for k, v in item.items() if k in field_mapping}
                                    pb_coll.update_one(
                                        {"POSCode": pos_code, "storeid": storeid},
                                        {"$set": update_fields}
                                    )
                                    print(f"Updated item with POSCode {pos_code} and storeid {storeid} in vfpricebook")

                            elif f_data['action'] == "Deals Add":
                                item_id = f_data['dataobject']['_id']['$oid']
                                pb_coll.update_one(
                                    {"_id": ObjectId(item_id)},
                                    {"$set": {"ActiveFlag": "no"}},
                                    {'$set': {'Status': "Success", 'Response': "Success"}}
                                )
                                print(f"Updated item with _id {item_id} to set ActiveFlag to no in vfpricebook")

                    else:
                        print(f"POST request failed with status code {post_response.status_code}")
                        col.update_one(
                            {'_id': ObjectId(_idd)},
                            {'$set': {'Status': "Failed", 'Response': f"POST request failed with status code {post_response.status_code}"}}
                        )
            else:
                print("Required keys are missing in the fetched document.")
                col.update_one(
                    {'_id': ObjectId(_idd)},
                    {'$set': {'Status': "Failed", 'Response': "Required keys are missing"}}
                )
        else:
            # No document found, wait before checking again
            time.sleep(10)  # Wait for 30 seconds before checking again
    except Exception as e:
        print(e)
        if '_idd' in locals():
            col.update_one(
                {'_id': ObjectId(_idd)},
                {'$set': {'Response': str(e)}}
            )
        time.sleep(10)  # Wait for 30 seconds before trying again