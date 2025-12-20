import time
import requests
import datetime
import xml.etree.ElementTree as ET
import xmltodict
import json
from pymongo import MongoClient
from bson.objectid import ObjectId

# Connect to MongoDB
client = MongoClient('mongodb+srv://rtnsmart:elneebekRf3dH30z@rtn.gfl4v.mongodb.net/')  
db = client['verifone']  
collection = db['vflivescan']  
upc_collection = db['upc']
pb_coll = db['vfpricebook']
store = db['stores']

# Current date and time
current_date = datetime.datetime.now()

# Format the current month and year
current_month_year = current_date.strftime('%B%Y')

storeid = "66337c59c31dc3e37229f275"
store_document = store.find_one({"_id": ObjectId(storeid)})

pos_username = store_document.get("pos_username")
pos_password = store_document.get("pos_password")

# First link
xml_link = f"http://192.168.31.11/cgi-bin/CGILink?cmd=validate&user={pos_username}&passwd={pos_password}"

response = requests.get(xml_link)

if response.status_code == 200:
    root = ET.fromstring(response.content)
    cookie_element = root.find('.//cookie')
    if cookie_element is not None:
        cookie = cookie_element.text
        print(cookie)

        for reptnum in range(2, 14):
            # Second link
            xml_link_2 = f"http://192.168.31.11/cgi-bin/CGILink?cmd=vposjournal&reptname=vposjournal&period=2&reptnum={reptnum}&cookie={cookie}"
            response_2 = requests.get(xml_link_2)

            if response_2.status_code == 200:
                root_2 = ET.fromstring(response_2.content)

                # Define the namespace
                ns = {'nax': 'http://www.naxml.org/POSBO/Vocabulary/2003-10-16'}

                # Extract specific elements
                store_data = {
                    "StoreLocationID": root_2.find('.//nax:StoreLocationID', ns).text,
                    "corpid": "66542a1557028503d23e93d9",
                    "storeid": storeid,
                }

                # Find all SaleEvent elements
                sale_events = root_2.findall('.//nax:SaleEvent', ns)

                # Fetch the UPC codes
                upc_codes = upc_collection.find_one({"storeid": "65d83ff360d8fb8e5b10b00d"}, {"UPCCodes": 1, "_id": 0})["UPCCodes"]

                specific_merchandise_codes = ["2", "11"] 

                # Loop over all SaleEvent elements
                for event in sale_events:
                    transaction_detail_group = event.find('.//nax:TransactionDetailGroup', ns)
                    if transaction_detail_group is not None:
                        transaction_line = transaction_detail_group.findall('.//nax:TransactionLine', ns)

                        # Initialize an empty list to hold the data from all TransactionLine elements
                        transaction_line_data = []

                        # Fetch the required fields from each SaleEvent
                        transaction_id = event.find('.//nax:TransactionID', ns).text if event.find('.//nax:TransactionID', ns) is not None else None
                        event_start_date = event.find('.//nax:EventStartDate', ns).text if event.find('.//nax:EventStartDate', ns) is not None else None
                        event_start_time = event.find('.//nax:EventStartTime', ns).text if event.find('.//nax:EventStartTime', ns) is not None else None
                        event_end_date = event.find('.//nax:EventEndDate', ns).text if event.find('.//nax:EventEndDate', ns) is not None else None
                        event_end_time = event.find('.//nax:EventEndTime', ns).text if event.find('.//nax:EventEndTime', ns) is not None else None

                        # Loop over all TransactionLine elements
                        for line in transaction_line:
                            if line.get('status') != 'cancel':
                                # Check for ItemLine
                                if line.find('.//nax:ItemLine', ns) is not None:
                                    merchandise_code = line.find('.//nax:MerchandiseCode', ns).text
                                    if merchandise_code in specific_merchandise_codes:
                                        pos_code = line.find('.//nax:ItemCode//nax:POSCode', ns).text if line.find('.//nax:ItemCode//nax:POSCode', ns) is not None else None
                                        sales_quantity = int(float(line.find('.//nax:SalesQuantity', ns).text))

                                        # Query vfpricebook collection
                                        vfpricebook_doc = db.vfpricebook.find_one({"POSCode": pos_code, "storeid": "66337c59c31dc3e37229f275"})
                                        # Get img_url if it exists, otherwise use an empty string
                                        img_url = vfpricebook_doc.get("img_url", "") if vfpricebook_doc else ""
                                        
                                        matched_upc_code = None
                                        for upc_code in upc_codes:
                                            if pos_code[2:] == upc_code:
                                                matched_upc_code = upc_code
                                                break

                                        document = pb_coll.find_one({"POSCode": {"$regex": f"^{pos_code}"}})

                                        manufacturer = document.get("Manufacturer", "") if document else ""
                                        company = document.get("Company", "") if document else ""
                                        
                                        item_line = {
                                            "POSCode": pos_code,
                                            "Description": line.find('.//nax:Description', ns).text,
                                            "Manufacturer": manufacturer,
                                            "Brand": company,
                                            "ActualSalesPrice": "{:.2f}".format(float(line.find('.//nax:ActualSalesPrice', ns).text)),
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text,
                                            "SellingUnits": line.find('.//nax:SellingUnits', ns).text,
                                            "RegularSellPrice": "{:.2f}".format(float(line.find('.//nax:RegularSellPrice', ns).text)),
                                            "SalesQuantity": sales_quantity,
                                            "SalesAmount": "{:.2f}".format(float(line.find('.//nax:SalesAmount', ns).text)),
                                            "PromotionAmount": sales_quantity * 0.25 if sales_quantity >= 2 and matched_upc_code else 0,
                                            "ImageURL": img_url
                                        }

                                        # Append the item_line to transaction_lines only if its POSCode matches with any of the upc_codes
                                        transaction_line_data.append({
                                            "ItemLine": item_line,
                                        })

                        # Only append transaction_line_data to sale_event_data if it's not empty
                        if transaction_line_data:
                            sale_event_data = {
                                "TransactionID": transaction_id,
                                "EventStartDate": event_start_date,
                                "EventStartTime": event_start_time,
                                "EventEndDate": event_end_date,
                                "EventEndTime": event_end_time,
                                "TransactionLine": transaction_line_data
                            }

                            # Add the SaleEvent data to the main data dictionary
                            data = store_data.copy()
                            data["SaleEvent"] = sale_event_data

                            # Check if the document already exists in the collection
                            if not collection.find_one({"SaleEvent.TransactionID": transaction_id}):
                                # Insert the document if it does not exist
                                collection.insert_one(data)
                                print("JSON data inserted into MongoDB collection successfully.")
                            else:
                                print("Document already exists in the collection.")
                    else:
                        print("No line with non-zero PromotionAmount found.")
            else:
                print("Failed to fetch XML data from the second link. HTTP status code:", response_2.status_code)
    else:
        print("Cookie element not found in the XML data.")
else:
    print(f"Failed to fetch XML data from the first link. HTTP status code: {response.status_code}")