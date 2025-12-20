import time
import requests
import xml.etree.ElementTree as ET
import xmltodict
import json
from pymongo import MongoClient
from bson.objectid import ObjectId

# Insert JSON into MongoDB collection
client = MongoClient('mongodb+srv://rtnsmart:elneebekRf3dH30z@rtn.gfl4v.mongodb.net/')  # Connect to MongoDB
db = client['verifone']  # Replace with your database name
collection = db['verifonels']  # Replace with your collection name
store = db['stores']

while True:
    # First link
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

            # Second link
            xml_link_2 = f"http://192.168.31.11/cgi-bin/CGILink?cmd=vposjournal&reptname=vposjournal&period=2&reptnum=1&cookie={cookie}"
            response_2 = requests.get(xml_link_2)
            #print(response_2.content)
            #print(response_2.status_code)
            if response_2.status_code == 200:
                root_2 = ET.fromstring(response_2.content)

                # Define the namespace
                ns = {'nax': 'http://www.naxml.org/POSBO/Vocabulary/2003-10-16'}

                # Extract specific elements
                data = {
                    "StoreLocationID": root_2.find('.//nax:StoreLocationID', ns).text,
                    "corpid": "66542a1557028503d23e93d9",
                    "storeid": "66337c59c31dc3e37229f275",
                    "ReportSequenceNumber": root_2.find('.//nax:ReportSequenceNumber', ns).text,
                    "BeginDate": root_2.find('.//nax:BeginDate', ns).text,
                    "BeginTime": root_2.find('.//nax:BeginTime', ns).text,
                }

                # Find all OtherEvent elements
                other_events = root_2.findall('.//nax:OtherEvent', ns)

               # Check if there are any SaleEvent elements
                if other_events:
                    # Get the last SaleEvent element
                    other_events = [other_events[-1]]

                # Initialize an empty list to hold the data from all OtherEvent elements
                other_event_data = []

                # Loop over all OtherEvent elements
                for event in other_events:
                    # Extract specific elements from each OtherEvent element and append the data to the list
                    cashier_detail = event.find('.//nax:CashierDetail', ns)
                    event_data = {
                        "EventSequenceID": event.find('.//nax:EventSequenceID', ns).text if event.find('.//nax:EventSequenceID', ns) is not None else None,
                        "CashierID": event.find('.//nax:CashierID', ns).text if event.find('.//nax:CashierID', ns) is not None else None,
                        "RegisterID": event.find('.//nax:RegisterID', ns).text if event.find('.//nax:RegisterID', ns) is not None else None,
                        "TillID": event.find('.//nax:TillID', ns).text if event.find('.//nax:TillID', ns) is not None else None,
                        "EventStartDate": event.find('.//nax:EventStartDate', ns).text if event.find('.//nax:EventStartDate', ns) is not None else None,
                        "EventStartTime": event.find('.//nax:EventStartTime', ns).text if event.find('.//nax:EventStartTime', ns) is not None else None,
                        "EventEndDate": event.find('.//nax:EventEndDate', ns).text if event.find('.//nax:EventEndDate', ns) is not None else None,
                        "EventEndTime": event.find('.//nax:EventEndTime', ns).text if event.find('.//nax:EventEndTime', ns) is not None else None,
                        "CashInDrawer": cashier_detail.find('.//nax:CashInDrawer', ns).text if cashier_detail is not None and cashier_detail.find('.//nax:CashInDrawer', ns) is not None else None,
                        "FoodStampsInDrawer": cashier_detail.find('.//nax:FoodStampsInDrawer', ns).text if cashier_detail is not None and cashier_detail.find('.//nax:FoodStampsInDrawer', ns) is not None else None
                    }

                    existing_data = collection.find_one({"OtherEvent": event_data})

                    if not existing_data:
                        other_event_data.append(event_data)

                # Add the OtherEvent data to the main data dictionary
                if other_event_data:    
                    data["OtherEvent"] = other_event_data

                # Find all SaleEvent elements
                sale_events = root_2.findall('.//nax:SaleEvent', ns)

                # Check if there are any SaleEvent elements
                if sale_events:
                    # Get the last SaleEvent element
                    sale_events = [sale_events[-1]]

                # Initialize an empty list to hold the data from all SaleEvent elements
                sale_event_data = []

                # Loop over all SaleEvent elements
                for event in sale_events:
                    transaction_detail_group = event.find('.//nax:TransactionDetailGroup', ns)
                    if transaction_detail_group is not None:
                        transaction_line = transaction_detail_group.findall('.//nax:TransactionLine', ns)

                        # Initialize an empty list to hold the data from all TransactionLine elements
                        transaction_line_data = []

                        # Loop over all TransactionLine elements
                        for line in transaction_line:
                            if line.get('status') != 'cancel':
                                # Check for ItemLine
                                if line.find('.//nax:ItemLine', ns) is not None:

                                    pos_code = line.find('.//nax:ItemCode//nax:POSCode', ns).text if line.find('.//nax:ItemCode//nax:POSCode', ns) is not None else None
                                    # Query vfpricebook collection
                                    vfpricebook_doc = db.vfpricebook.find_one({"POSCode": pos_code, "storeid": "66337c59c31dc3e37229f275"})
                                    # Get img_url if it exists, otherwise use an empty string
                                    img_url = vfpricebook_doc.get("img_url", "") if vfpricebook_doc else ""

                                    tax_level_id = line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None
                                    sales_amount = float(line.find('.//nax:SalesAmount', ns).text)
                                    if tax_level_id == '1':
                                        tax = sales_amount * 0.08
                                    elif tax_level_id == '2':
                                        tax = sales_amount * 0.01
                                    else:
                                        tax = 0

                                    transaction_line_data.append({
                                        "ItemLine": {
                                            "POSCode": pos_code,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(float(line.find('.//nax:ActualSalesPrice', ns).text)) if line.find('.//nax:ActualSalesPrice', ns) is not None else None,
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "SellingUnits": line.find('.//nax:SellingUnits', ns).text if line.find('.//nax:SellingUnits', ns) is not None else None,
                                            "PromotionID": line.find('.//nax:Promotion//nax:PromotionID', ns).text if line.find('.//nax:PromotionID', ns) is not None else None,
                                            "PromotionAmount": "{:.2f}".format(float(line.find('.//nax:Promotion//nax:PromotionAmount', ns).text)) if line.find('.//nax:Promotion//nax:PromotionAmount', ns) is not None else None,
                                            "RegularSellPrice": "{:.2f}".format(float(line.find('.//nax:RegularSellPrice', ns).text)) if line.find('.//nax:RegularSellPrice', ns) is not None else None,
                                            "SalesQuantity": line.find('.//nax:SalesQuantity', ns).text if line.find('.//nax:SalesQuantity', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(float(line.find('.//nax:SalesAmount', ns).text)) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                            "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None,
                                            "ImageUrl": img_url
                                        }
                                    })
                                # Check for MerchandiseCodeLine
                                elif line.find('.//nax:MerchandiseCodeLine', ns) is not None:

                                    sales_restriction = line.find('.//nax:SalesRestriction', ns)
                                    item_tax = line.find('.//nax:ItemTax', ns)
                                    sales_amount = float(line.find('.//nax:SalesAmount', ns).text)
                                    tax_level_id = line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None
                                    if tax_level_id == '1':
                                        tax = sales_amount * 0.08
                                    elif tax_level_id == '2':
                                        tax = sales_amount * 0.01
                                    else:
                                        tax = 0

                                    transaction_line_data.append({
                                        "MerchandiseCodeLine": {
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(float(line.find('.//nax:ActualSalesPrice', ns).text)),
                                            "RegularSellPrice": "{:.2f}".format(float(line.find('.//nax:RegularSellPrice', ns).text)),
                                            "SalesQuantity": line.find('.//nax:SalesQuantity', ns).text,
                                            "SalesAmount": "{:.2f}".format(float(line.find('.//nax:SalesAmount', ns).text)),
                                            "MinimumCustomerAge": sales_restriction.find('.//nax:MinimumCustomerAge', ns).text if sales_restriction is not None and sales_restriction.find('.//nax:MinimumCustomerAge', ns) is not None else None,
                                            "TaxLevelID": item_tax.find('.//nax:TaxLevelID', ns).text if item_tax is not None else None,
                                            # Add more fields as needed
                                        }
                                    })
                                # Check for FuelLine
                                elif line.find('.//nax:FuelLine', ns) is not None:
                                    # Extract specific elements for FuelLine and append the data to the list
                                    # Replace the fields in the dictionary below with the actual fields for FuelLine
                                    transaction_line_data.append({
                                        "FuelLine": {
                                            "FuelGradeID": line.find('.//nax:FuelGradeID', ns).text,
                                            "FuelPositionID": line.find('.//nax:FuelPositionID', ns).text,
                                            "PriceTierCode": line.find('.//nax:PriceTierCode', ns).text,
                                            "TimeTierCode": line.find('.//nax:TimeTierCode', ns).text,
                                            "ServiceLevelCode": line.find('.//nax:ServiceLevelCode', ns).text,
                                            "Description": line.find('.//nax:Description', ns).text,
                                            "ActualSalesPrice": "{:.2f}".format(float(line.find('.//nax:ActualSalesPrice', ns).text)),
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text,
                                            "RegularSellPrice": "{:.2f}".format(float(line.find('.//nax:RegularSellPrice', ns).text)),
                                            "SalesQuantity": float(line.find('.//nax:SalesQuantity', ns).text),
                                            "SalesAmount": "{:.2f}".format(float(line.find('.//nax:SalesAmount', ns).text)),
                                            # Add more fields as needed
                                        }
                                    })
                                # Check for TransactionTax
                                elif line.find('.//nax:TransactionTax', ns) is not None:
                                    transaction_line_data.append({
                                        "TransactionTax": {
                                            "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None,
                                            "TaxableSalesAmount": "{:.2f}".format(float(line.find('.//nax:TaxableSalesAmount', ns).text)) if line.find('.//nax:TaxableSalesAmount', ns) is not None else None,
                                            "TaxCollectedAmount": "{:.2f}".format(float(line.find('.//nax:TaxCollectedAmount', ns).text)) if line.find('.//nax:TaxCollectedAmount', ns) is not None else None
                                        }
                                    })
                                # Check for TenderInfo
                                elif line.find('.//nax:TenderInfo', ns) is not None:
                                    transaction_line_data.append({
                                        "TenderInfo": {
                                            "TenderSubCode": line.find('.//nax:Tender/nax:TenderSubCode', ns).text if line.find('.//nax:Tender/nax:TenderSubCode', ns) is not None else None,
                                            "TenderAmount": "{:.2f}".format(float(line.find('.//nax:TenderAmount', ns).text)) if line.find('.//nax:TenderAmount', ns) is not None else None
                                        }
                                    })
                        else:
                            transaction_line_data = []

                    # Extract TransactionSummary
                    transaction_summary_data = {}
                    transaction_summary = event.find('.//nax:TransactionSummary', ns)
                    if transaction_summary is not None:
                        transaction_summary_data = {
                            "TransactionTotalGrossAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalGrossAmount', ns).text)) if transaction_summary.find('nax:TransactionTotalGrossAmount', ns) is not None else None,
                            "TransactionTotalNetAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalNetAmount', ns).text)) if transaction_summary.find('nax:TransactionTotalNetAmount', ns) is not None else None,
                            "TransactionTotalTaxSalesAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns).text)) if transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns) is not None else None,
                            "TransactionTotalTaxNetAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalTaxNetAmount', ns).text)) if transaction_summary.find('nax:TransactionTotalTaxNetAmount', ns) is not None else None,
                            "TransactionTotalGrandAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalGrandAmount', ns).text)) if transaction_summary.find('nax:TransactionTotalGrandAmount', ns) is not None else None
                        }

                    # Extract specific elements from each SaleEvent element and append the data to the list
                    event_data = {
                        "EventSequenceID": event.find('.//nax:EventSequenceID', ns).text if event.find('.//nax:EventSequenceID', ns) is not None else None,
                        "CashierID": event.find('.//nax:CashierID', ns).text if event.find('.//nax:CashierID', ns) is not None else None,
                        "RegisterID": event.find('.//nax:RegisterID', ns).text if event.find('.//nax:RegisterID', ns) is not None else None,
                        "TillID": event.find('.//nax:TillID', ns).text if event.find('.//nax:TillID', ns) is not None else None,
                        "TransactionID": event.find('.//nax:TransactionID', ns).text if event.find('.//nax:TransactionID', ns) is not None else None,
                        "EventStartDate": event.find('.//nax:EventStartDate', ns).text if event.find('.//nax:EventStartDate', ns) is not None else None,
                        "EventStartTime": event.find('.//nax:EventStartTime', ns).text if event.find('.//nax:EventStartTime', ns) is not None else None,
                        "EventEndDate": event.find('.//nax:EventEndDate', ns).text if event.find('.//nax:EventEndDate', ns) is not None else None,
                        "EventEndTime": event.find('.//nax:EventEndTime', ns).text if event.find('.//nax:EventEndTime', ns) is not None else None,
                        "ReceiptDate": event.find('.//nax:ReceiptDate', ns).text if event.find('.//nax:ReceiptDate', ns) is not None else None,
                        "ReceiptTime": event.find('.//nax:ReceiptTime', ns).text if event.find('.//nax:ReceiptTime', ns) is not None else None,
                        "TransactionLine": transaction_line_data,
                        "TransactionSummary": transaction_summary_data
                    }

                    existing_data = collection.find_one({"SaleEvent": event_data})

                    if not existing_data:
                        sale_event_data.append(event_data)

                # Add the SaleEvent data to the main data dictionary
                if sale_event_data:
                    data["SaleEvent"] = sale_event_data

                # Find all OtherEvent elements
                financial_events = root_2.findall('.//nax:FinancialEvent', ns)

               # Check if there are any financialEvent elements
                if financial_events:
                    # Get the last financialEvent element
                    financial_events = [financial_events[-1]]

                # Initialize an empty list to hold the data from all OtherEvent elements
                financial_event_data = []

                # Loop over all OtherEvent elements
                for event in financial_events:
                    # Extract FinancialEventDetail
                    financial_event_detail = event.find('.//nax:FinancialEventDetail', ns)
                    if financial_event_detail is not None:
                        payout_detail = financial_event_detail.find('nax:PayOutDetail', ns)
                        if payout_detail is not None:
                            tender_info = payout_detail.find('nax:TenderInfo/nax:Tender', ns)
                            if tender_info is not None:
                                tender_sub_code = tender_info.find('nax:TenderSubCode', ns).text if tender_info.find('nax:TenderSubCode', ns) is not None else None
                            else:
                                tender_sub_code = None

                            payout_detail_data = {
                                "DetailAmount": payout_detail.find('nax:DetailAmount', ns).text,
                                "TenderSubCode": tender_sub_code,
                                "TenderAmount": payout_detail.find('nax:TenderInfo/nax:Tender/nax:TenderAmount', ns).text if payout_detail.find('nax:TenderInfo/nax:Tender/nax:TenderAmount', ns) is not None else None,
                                "PayOutReason": payout_detail.find('nax:PayOutReason', ns).text,
                            }
                        else:
                            payout_detail_data = None
                    else:
                        payout_detail_data = None

                    event_data = {
                        "EventSequenceID": event.find('.//nax:EventSequenceID', ns).text if event.find('.//nax:EventSequenceID', ns) is not None else None,
                        "TransactionID": event.find('.//nax:TransactionID', ns).text if event.find('.//nax:TransactionID', ns) is not None else None,
                        "CashierID": event.find('.//nax:CashierID', ns).text if event.find('.//nax:CashierID', ns) is not None else None,
                        "RegisterID": event.find('.//nax:RegisterID', ns).text if event.find('.//nax:RegisterID', ns) is not None else None,
                        "TillID": event.find('.//nax:TillID', ns).text if event.find('.//nax:TillID', ns) is not None else None,
                        "EventStartDate": event.find('.//nax:EventStartDate', ns).text if event.find('.//nax:EventStartDate', ns) is not None else None,
                        "EventStartTime": event.find('.//nax:EventStartTime', ns).text if event.find('.//nax:EventStartTime', ns) is not None else None,
                        "EventEndDate": event.find('.//nax:EventEndDate', ns).text if event.find('.//nax:EventEndDate', ns) is not None else None,
                        "EventEndTime": event.find('.//nax:EventEndTime', ns).text if event.find('.//nax:EventEndTime', ns) is not None else None,
                        "ReceiptDate": event.find('.//nax:ReceiptDate', ns).text if event.find('.//nax:ReceiptDate', ns) is not None else None,
                        "ReceiptTime": event.find('.//nax:ReceiptTime', ns).text if event.find('.//nax:ReceiptTime', ns) is not None else None,
                        "FinancialEventDetail": payout_detail_data
                    }

                    existing_data = collection.find_one({"FinancialEvent": event_data})

                    if not existing_data:
                        financial_event_data.append(event_data)

                # Add the OtherEvent data to the main data dictionary
                if financial_event_data:
                    data["FinancialEvent"] = financial_event_data

                # Find all OtherEvent elements
                void_events = root_2.findall('.//nax:VoidEvent', ns)

               # Check if there are any  voidEvent elements
                if void_events:
                    # Get the last voidEvent element
                    void_events = [void_events[-1]]

                # Initialize an empty list to hold the data from all OtherEvent elements
                void_event_data = []

                # Loop over all OtherEvent elements
                for event in void_events:
                    transaction_detail_group = event.find('.//nax:TransactionDetailGroup', ns)
                    transaction_line = transaction_detail_group.findall('.//nax:TransactionLine', ns)

                    # Initialize an empty list to hold the data from all TransactionLine elements
                    transaction_line_data = []

                    # Loop over all TransactionLine elements
                    for line in transaction_line:
                        if line.get('status') != 'cancel':
                            # Check for ItemLine
                            if line.find('.//nax:ItemLine', ns) is not None:

                                pos_code = line.find('.//nax:ItemCode//nax:POSCode', ns).text if line.find('.//nax:ItemCode//nax:POSCode', ns) is not None else None
                                # Query vfpricebook collection
                                vfpricebook_doc = db.vfpricebook.find_one({"POSCode": pos_code, "storeid": "66337c59c31dc3e37229f275"})
                                # Get img_url if it exists, otherwise use an empty string
                                img_url = vfpricebook_doc.get("img_url", "") if vfpricebook_doc else ""

                                tax_level_id = line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None
                                sales_amount = float(line.find('.//nax:SalesAmount', ns).text)
                                if tax_level_id == '1':
                                    tax = sales_amount * 0.08
                                elif tax_level_id == '2':
                                    tax = sales_amount * 0.01
                                else:
                                    tax = 0

                                transaction_line_data.append({
                                    "ItemLine": {
                                        "POSCode": pos_code,
                                        "Description": line.find('.//nax:Description', ns).text,
                                        "ActualSalesPrice": "{:.2f}".format(float(line.find('.//nax:ActualSalesPrice', ns).text)),
                                        "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text,
                                        "SellingUnits": line.find('.//nax:SellingUnits', ns).text,
                                        "RegularSellPrice": "{:.2f}".format(float(line.find('.//nax:RegularSellPrice', ns).text)),
                                        "SalesQuantity": line.find('.//nax:SalesQuantity', ns).text,
                                        "SalesAmount": "{:.2f}".format(float(line.find('.//nax:SalesAmount', ns).text)),
                                        "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None,
                                        "ImageUrl": img_url
                                    }
                                })
                            # Check for MerchandiseCodeLine
                            elif line.find('.//nax:MerchandiseCodeLine', ns) is not None:

                                sales_restriction = line.find('.//nax:SalesRestriction', ns)
                                item_tax = line.find('.//nax:ItemTax', ns)
                                sales_amount = float(line.find('.//nax:SalesAmount', ns).text)
                                tax_level_id = line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None
                                if tax_level_id == '1':
                                    tax = sales_amount * 0.08
                                elif tax_level_id == '2':
                                    tax = sales_amount * 0.01
                                else:
                                    tax = 0

                                transaction_line_data.append({
                                    "MerchandiseCodeLine": {
                                        "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text,
                                        "Description": line.find('.//nax:Description', ns).text,
                                        "ActualSalesPrice": "{:.2f}".format(float(line.find('.//nax:ActualSalesPrice', ns).text)),
                                        "RegularSellPrice": "{:.2f}".format(float(line.find('.//nax:RegularSellPrice', ns).text)),
                                        "SalesQuantity": line.find('.//nax:SalesQuantity', ns).text,
                                        "SalesAmount": "{:.2f}".format(float(line.find('.//nax:SalesAmount', ns).text)),
                                        "MinimumCustomerAge": sales_restriction.find('.//nax:MinimumCustomerAge', ns).text if sales_restriction is not None and sales_restriction.find('.//nax:MinimumCustomerAge', ns) is not None else None,
                                        "TaxLevelID": item_tax.find('.//nax:TaxLevelID', ns).text if item_tax is not None else None,
                                        # Add more fields as needed
                                    }
                                })
                            # Check for FuelLine
                            elif line.find('.//nax:FuelLine', ns) is not None:
                                # Extract specific elements for FuelLine and append the data to the list
                                # Replace the fields in the dictionary below with the actual fields for FuelLine
                                transaction_line_data.append({
                                    "FuelLine": {
                                        "FuelGradeID": line.find('.//nax:FuelGradeID', ns).text,
                                        "FuelPositionID": line.find('.//nax:FuelPositionID', ns).text,
                                        "PriceTierCode": line.find('.//nax:PriceTierCode', ns).text,
                                        "TimeTierCode": line.find('.//nax:TimeTierCode', ns).text,
                                        "ServiceLevelCode": line.find('.//nax:ServiceLevelCode', ns).text,
                                        "Description": line.find('.//nax:Description', ns).text,
                                        "ActualSalesPrice": "{:.2f}".format(float(line.find('.//nax:ActualSalesPrice', ns).text)),
                                        "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text,
                                        "RegularSellPrice": "{:.2f}".format(float(line.find('.//nax:RegularSellPrice', ns).text)),
                                        "SalesQuantity": float(line.find('.//nax:SalesQuantity', ns).text),
                                        "SalesAmount": "{:.2f}".format(float(line.find('.//nax:SalesAmount', ns).text)),
                                        # Add more fields as needed
                                    }
                                })
                            # Check for TransactionTax
                            elif line.find('.//nax:TransactionTax', ns) is not None:
                                transaction_line_data.append({
                                    "TransactionTax": {
                                        "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None,
                                        "TaxableSalesAmount": "{:.2f}".format(float(line.find('.//nax:TaxableSalesAmount', ns).text)) if line.find('.//nax:TaxableSalesAmount', ns) is not None else None,
                                        "TaxCollectedAmount": "{:.2f}".format(float(line.find('.//nax:TaxCollectedAmount', ns).text)) if line.find('.//nax:TaxCollectedAmount', ns) is not None else None
                                    }
                                })
                            # Check for TenderInfo
                            elif line.find('.//nax:TenderInfo', ns) is not None:
                                transaction_line_data.append({
                                    "TenderInfo": {
                                        "TenderSubCode": line.find('.//nax:Tender/nax:TenderSubCode', ns).text if line.find('.//nax:Tender/nax:TenderSubCode', ns) is not None else None,
                                        "TenderAmount": "{:.2f}".format(float(line.find('.//nax:TenderAmount', ns).text)) if line.find('.//nax:TenderAmount', ns) is not None else None
                                    }
                                })

                    # Extract TransactionSummary
                    transaction_summary = event.find('.//nax:TransactionSummary', ns)
                    if transaction_summary is not None:
                        transaction_summary_data = {
                            "TransactionTotalGrossAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalGrossAmount', ns).text)) if transaction_summary.find('nax:TransactionTotalGrossAmount', ns) is not None else None,
                            "TransactionTotalNetAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalNetAmount', ns).text)) if transaction_summary.find('nax:TransactionTotalNetAmount', ns) is not None else None,
                            "TransactionTotalTaxSalesAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns).text)) if transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns) is not None else None,
                            "TransactionTotalTaxNetAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalTaxNetAmount', ns).text)) if transaction_summary.find('nax:TransactionTotalTaxNetAmount', ns) is not None else None,
                            "TransactionTotalGrandAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalGrandAmount', ns).text)) if transaction_summary.find('nax:TransactionTotalGrandAmount', ns) is not None else None,
                        }

                    event_data = {
                        "EventSequenceID": event.find('.//nax:EventSequenceID', ns).text if event.find('.//nax:EventSequenceID', ns) is not None else None,
                        "CashierID": event.find('.//nax:CashierID', ns).text if event.find('.//nax:CashierID', ns) is not None else None,
                        "RegisterID": event.find('.//nax:RegisterID', ns).text if event.find('.//nax:RegisterID', ns) is not None else None,
                        "TillID": event.find('.//nax:TillID', ns).text if event.find('.//nax:TillID', ns) is not None else None,
                        "TransactionID": event.find('.//nax:TransactionID', ns).text if event.find('.//nax:TransactionID', ns) is not None else None,
                        "EventStartDate": event.find('.//nax:EventStartDate', ns).text if event.find('.//nax:EventStartDate', ns) is not None else None,
                        "EventStartTime": event.find('.//nax:EventStartTime', ns).text if event.find('.//nax:EventStartTime', ns) is not None else None,
                        "EventEndDate": event.find('.//nax:EventEndDate', ns).text if event.find('.//nax:EventEndDate', ns) is not None else None,
                        "EventEndTime": event.find('.//nax:EventEndTime', ns).text if event.find('.//nax:EventEndTime', ns) is not None else None,
                        "ReceiptDate": event.find('.//nax:ReceiptDate', ns).text if event.find('.//nax:ReceiptDate', ns) is not None else None,
                        "ReceiptTime": event.find('.//nax:ReceiptTime', ns).text if event.find('.//nax:ReceiptTime', ns) is not None else None,
                        "TransactionLine": transaction_line_data,
                        "TransactionSummary": transaction_summary_data
                    }

                    existing_data = collection.find_one({"VoidEvent": event_data})

                    if not existing_data:
                        void_event_data.append(event_data)

                # Add the OtherEvent data to the main data dictionary
                if void_event_data:
                    data["VoidEvent"] = void_event_data

                # Find all SaleEvent elements
                refund_events = root_2.findall('.//nax:RefundEvent', ns)

               # Check if there are any refundEvent elements
                if refund_events:
                    # Get the last refundEvent element
                    refund_events = [refund_events[-1]]

                # Initialize an empty list to hold the data from all SaleEvent elements
                refund_event_data = []

                # Loop over all SaleEvent elements
                for event in refund_events:
                    transaction_detail_group = event.find('.//nax:TransactionDetailGroup', ns)
                    transaction_line = transaction_detail_group.findall('.//nax:TransactionLine', ns)

                    # Initialize an empty list to hold the data from all TransactionLine elements
                    transaction_line_data = []

                    # Loop over all TransactionLine elements
                    for line in transaction_line:
                        if line.get('status') != 'cancel':
                            # Check for ItemLine
                            if line.find('.//nax:ItemLine', ns) is not None:

                                pos_code = line.find('.//nax:ItemCode//nax:POSCode', ns).text if line.find('.//nax:ItemCode//nax:POSCode', ns) is not None else None
                                # Query vfpricebook collection
                                vfpricebook_doc = db.vfpricebook.find_one({"POSCode": pos_code, "storeid": "66337c59c31dc3e37229f275"})
                                # Get img_url if it exists, otherwise use an empty string
                                img_url = vfpricebook_doc.get("img_url", "") if vfpricebook_doc else ""

                                tax_level_id = line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None
                                sales_amount = float(line.find('.//nax:SalesAmount', ns).text)
                                if tax_level_id == '1':
                                    tax = sales_amount * 0.08
                                elif tax_level_id == '2':
                                    tax = sales_amount * 0.01
                                else:
                                    tax = 0

                                transaction_line_data.append({
                                    "ItemLine": {
                                        "POSCode": pos_code,
                                        "Description": line.find('.//nax:Description', ns).text,
                                        "ActualSalesPrice": "{:.2f}".format(float(line.find('.//nax:ActualSalesPrice', ns).text)),
                                        "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text,
                                        "SellingUnits": line.find('.//nax:SellingUnits', ns).text,
                                        "RegularSellPrice": "{:.2f}".format(float(line.find('.//nax:RegularSellPrice', ns).text)),
                                        "SalesQuantity": line.find('.//nax:SalesQuantity', ns).text,
                                        "SalesAmount": "{:.2f}".format(float(line.find('.//nax:SalesAmount', ns).text)),
                                        "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None,
                                        "ImageUrl": img_url
                                    }
                                })
                            # Check for MerchandiseCodeLine
                            elif line.find('.//nax:MerchandiseCodeLine', ns) is not None:

                                sales_restriction = line.find('.//nax:SalesRestriction', ns)
                                item_tax = line.find('.//nax:ItemTax', ns)
                                sales_amount = float(line.find('.//nax:SalesAmount', ns).text)
                                tax_level_id = line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None
                                if tax_level_id == '1':
                                    tax = sales_amount * 0.08
                                elif tax_level_id == '2':
                                    tax = sales_amount * 0.01
                                else:
                                    tax = 0

                                transaction_line_data.append({
                                    "MerchandiseCodeLine": {
                                        "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text,
                                        "Description": line.find('.//nax:Description', ns).text,
                                        "ActualSalesPrice": "{:.2f}".format(float(line.find('.//nax:ActualSalesPrice', ns).text)),
                                        "RegularSellPrice": "{:.2f}".format(float(line.find('.//nax:RegularSellPrice', ns).text)),
                                        "SalesQuantity": line.find('.//nax:SalesQuantity', ns).text,
                                        "SalesAmount": "{:.2f}".format(float(line.find('.//nax:SalesAmount', ns).text)),
                                        "MinimumCustomerAge": sales_restriction.find('.//nax:MinimumCustomerAge', ns).text if sales_restriction is not None and sales_restriction.find('.//nax:MinimumCustomerAge', ns) is not None else None,
                                        "TaxLevelID": item_tax.find('.//nax:TaxLevelID', ns).text if item_tax is not None else None,
                                        # Add more fields as needed
                                    }
                                })
                            # Check for FuelLine
                            elif line.find('.//nax:FuelLine', ns) is not None:
                                # Extract specific elements for FuelLine and append the data to the list
                                # Replace the fields in the dictionary below with the actual fields for FuelLine
                                transaction_line_data.append({
                                    "FuelLine": {
                                        "FuelGradeID": line.find('.//nax:FuelGradeID', ns).text,
                                        "FuelPositionID": line.find('.//nax:FuelPositionID', ns).text,
                                        "PriceTierCode": line.find('.//nax:PriceTierCode', ns).text,
                                        "TimeTierCode": line.find('.//nax:TimeTierCode', ns).text,
                                        "ServiceLevelCode": line.find('.//nax:ServiceLevelCode', ns).text,
                                        "Description": line.find('.//nax:Description', ns).text,
                                        "ActualSalesPrice": "{:.2f}".format(float(line.find('.//nax:ActualSalesPrice', ns).text)),
                                        "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text,
                                        "RegularSellPrice": "{:.2f}".format(float(line.find('.//nax:RegularSellPrice', ns).text)),
                                        "SalesQuantity": float(line.find('.//nax:SalesQuantity', ns).text),
                                        "SalesAmount": "{:.2f}".format(float(line.find('.//nax:SalesAmount', ns).text)),
                                        # Add more fields as needed
                                    }
                                })
                            # Check for TransactionTax
                            elif line.find('.//nax:TransactionTax', ns) is not None:
                                transaction_line_data.append({
                                    "TransactionTax": {
                                        "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text,
                                        "TaxableSalesAmount": "{:.2f}".format(float(line.find('.//nax:TaxableSalesAmount', ns).text)),
                                        "TaxCollectedAmount": "{:.2f}".format(float(line.find('.//nax:TaxCollectedAmount', ns).text))
                                    }
                                })
                            # Check for TenderInfo
                            elif line.find('.//nax:TenderInfo', ns) is not None:
                                transaction_line_data.append({
                                    "TenderInfo": {
                                        "TenderSubCode": line.find('.//nax:Tender/nax:TenderSubCode', ns).text if line.find('.//nax:Tender/nax:TenderSubCode', ns) is not None else None,
                                        "TenderAmount": "{:.2f}".format(float(line.find('.//nax:TenderAmount', ns).text)) if line.find('.//nax:TenderAmount', ns) is not None else None
                                    }
                                })

                    # Extract TransactionSummary
                    transaction_summary = event.find('.//nax:TransactionSummary', ns)
                    if transaction_summary is not None:
                        transaction_total_grand_amount = transaction_summary.find('nax:TransactionTotalGrandAmount', ns)
                        transaction_summary_data = {
                            "TransactionTotalGrossAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalGrossAmount', ns).text)),
                            "TransactionTotalNetAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalNetAmount', ns).text)),
                            "TransactionTotalTaxSalesAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns).text)),
                            "TransactionTotalTaxNetAmount": "{:.2f}".format(float(transaction_summary.find('nax:TransactionTotalTaxNetAmount', ns).text)),
                            #"TransactionTotalGrandAmount": transaction_summary.find('nax:TransactionTotalGrandAmount', ns).text,
                            "TransactionTotalGrandAmount": {
                                "@direction": transaction_total_grand_amount.attrib['direction'],
                                "#text": transaction_total_grand_amount.text
                            }
                        }

                    # Extract specific elements from each SaleEvent element and append the data to the list
                    event_data = {
                        "EventSequenceID": event.find('.//nax:EventSequenceID', ns).text if event.find('.//nax:EventSequenceID', ns) is not None else None,
                        "CashierID": event.find('.//nax:CashierID', ns).text if event.find('.//nax:CashierID', ns) is not None else None,
                        "RegisterID": event.find('.//nax:RegisterID', ns).text if event.find('.//nax:RegisterID', ns) is not None else None,
                        "TillID": event.find('.//nax:TillID', ns).text if event.find('.//nax:TillID', ns) is not None else None,
                        "TransactionID": event.find('.//nax:TransactionID', ns).text if event.find('.//nax:TransactionID', ns) is not None else None,
                        "EventStartDate": event.find('.//nax:EventStartDate', ns).text if event.find('.//nax:EventStartDate', ns) is not None else None,
                        "EventStartTime": event.find('.//nax:EventStartTime', ns).text if event.find('.//nax:EventStartTime', ns) is not None else None,
                        "EventEndDate": event.find('.//nax:EventEndDate', ns).text if event.find('.//nax:EventEndDate', ns) is not None else None,
                        "EventEndTime": event.find('.//nax:EventEndTime', ns).text if event.find('.//nax:EventEndTime', ns) is not None else None,
                        "ReceiptDate": event.find('.//nax:ReceiptDate', ns).text if event.find('.//nax:ReceiptDate', ns) is not None else None,
                        "ReceiptTime": event.find('.//nax:ReceiptTime', ns).text if event.find('.//nax:ReceiptTime', ns) is not None else None,
                        "TransactionLine": transaction_line_data,
                        "TransactionSummary": transaction_summary_data,
                        "ApproverID": event.find('.//nax:ApproverID', ns).text if event.find('.//nax:ApproverID', ns) is not None else None,
                        "RefundReason": event.find('.//nax:RefundReason', ns).text if event.find('.//nax:RefundReason', ns) is not None else None
                    }

                    existing_data = collection.find_one({"RefundEvent": event_data})

                    if not existing_data:
                        refund_event_data.append(event_data)

                # Add the SaleEvent data to the main data dictionary
                if refund_event_data:
                    data["RefundEvent"] = refund_event_data
                print(data)
                if "RefundEvent" in data or "SaleEvent" in data or "FinancialEvent" in data or "VoidEvent" in data or "OtherEvent" in data:
                    # Insert the data into the MongoDB collection
                    collection.insert_one(data)

                    print("JSON data inserted into MongoDB collection successfully.")
                else:
                    print("No event data appended. Skipping insertion.")
                #collection.insert_one(data)

                #print("JSON data inserted into MongoDB collection successfully.")
            else:
                print(f"Failed to fetch XML data from the second link. HTTP status code: {response_2.status_code}")
        else:
            print("Cookie element not found in the XML data.")
    else:
        print(f"Failed to fetch XML data from the first link. HTTP status code: {response.status_code}")

    # Sleep for 3 seconds before the next iteration
    time.sleep(60)
