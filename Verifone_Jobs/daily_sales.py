import requests
import xml.etree.ElementTree as ET
from pymongo import MongoClient
from bson.objectid import ObjectId

client = MongoClient('mongodb+srv://rtnsmart:elneebekRf3dH30z@rtn.gfl4v.mongodb.net/')  # Connect to MongoDB
db = client['verifone']
store = db['stores']

corpid = "66542a1557028503d23e93d9"
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

        for reptnum in range(2, 11):
            # Second link
            xml_link_2 = f"http://192.168.31.11/cgi-bin/CGILink?cmd=vposjournal&reptname=vposjournal&period=2&reptnum={reptnum}&cookie={cookie}"
            response_2 = requests.get(xml_link_2)

            if response_2.status_code == 200:
                try:
                    root_2 = ET.fromstring(response_2.content)
                except ET.ParseError as e:
                    print(f"Failed to parse XML for reptnum {reptnum}: {e}")
                    continue

                # Define the namespace
                ns = {'nax': 'http://www.naxml.org/POSBO/Vocabulary/2003-10-16'}
                
                # Extract specific elements
                data = {
                    "StoreLocationID": root_2.find('.//nax:StoreLocationID', ns).text,
                    "corpid": corpid,
                    "storeid": storeid,
                    "ReportSequenceNumber": root_2.find('.//nax:ReportSequenceNumber', ns).text,
                    "BeginDate": root_2.find('.//nax:BeginDate', ns).text,
                    "BeginTime": root_2.find('.//nax:BeginTime', ns).text,
                    "EndDate": root_2.find('.//nax:EndDate', ns).text,
                    "EndTime": root_2.find('.//nax:EndTime', ns).text,
                }

                # Check if the data already exists in the collection
                if not db.vfdailysales.find_one(data):
                    # Find all OtherEvent elements
                    other_events = root_2.findall('.//nax:OtherEvent', ns)

                    # Initialize an empty list to hold the data from all OtherEvent elements
                    other_event_data = []

                    # Loop over all OtherEvent elements
                    for event in other_events:
                        # Extract specific elements from each OtherEvent element and append the data to the list
                        cashier_detail = event.find('.//nax:CashierDetail', ns)
                        other_event_data.append({
                            "EventSequenceID": event.find('.//nax:EventSequenceID', ns).text,
                            "CashierID": event.find('.//nax:CashierID', ns).text,
                            "RegisterID": event.find('.//nax:RegisterID', ns).text,
                            "TillID": event.find('.//nax:TillID', ns).text,
                            "CashInDrawer": cashier_detail.find('.//nax:CashInDrawer', ns).text if cashier_detail is not None and cashier_detail.find('.//nax:CashInDrawer', ns) is not None else None,
                            "FoodStampsInDrawer": cashier_detail.find('.//nax:FoodStampsInDrawer', ns).text if cashier_detail is not None and cashier_detail.find('.//nax:FoodStampsInDrawer', ns) is not None else None
                        })

                    # Add the OtherEvent data to the main data dictionary
                    if other_event_data:    
                        data["OtherEvent"] = other_event_data

                    # Find all SaleEvent elements
                    sale_events = root_2.findall('.//nax:SaleEvent', ns)

                    # Initialize an empty list to hold the data from all SaleEvent elements
                    sale_event_data = []

                    # Loop over all SaleEvent elements
                    for event in sale_events:
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
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(abs(float(line.find('.//nax:ActualSalesPrice', ns).text))) if line.find('.//nax:ActualSalesPrice', ns) is not None else None,
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "SellingUnits": line.find('.//nax:SellingUnits', ns).text if line.find('.//nax:SellingUnits', ns) is not None else None,
                                            "PromotionID": line.find('.//nax:Promotion//nax:PromotionID', ns).text if line.find('.//nax:PromotionID', ns) is not None else None,
                                            "PromotionAmount": "{:.2f}".format(abs(float(line.find('.//nax:Promotion//nax:PromotionAmount', ns).text))) if line.find('.//nax:Promotion//nax:PromotionAmount', ns) is not None else None,
                                            "RegularSellPrice": "{:.2f}".format(abs(float(line.find('.//nax:RegularSellPrice', ns).text))) if line.find('.//nax:RegularSellPrice', ns) is not None else None,
                                            "SalesQuantity": abs(float(line.find('.//nax:SalesQuantity', ns).text)) if line.find('.//nax:SalesQuantity', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                            "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None,
                                            "SalesTax": "{:.2f}".format(abs(float(tax))),
                                            "ImageURL": img_url
                                        }
                                    })
                                # Check for MerchandiseCodeLine
                                elif line.find('.//nax:MerchandiseCodeLine', ns) is not None:
                                    # Extract specific elements for MerchandiseCodeLine and append the data to the list
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
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(abs(float(line.find('.//nax:ActualSalesPrice', ns).text))) if line.find('.//nax:ActualSalesPrice', ns) is not None else None,
                                            "RegularSellPrice": "{:.2f}".format(abs(float(line.find('.//nax:RegularSellPrice', ns).text))) if line.find('.//nax:RegularSellPrice', ns) is not None else None,
                                            "SalesQuantity": abs(float(line.find('.//nax:SalesQuantity', ns).text)) if line.find('.//nax:SalesQuantity', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                            "MinimumCustomerAge": sales_restriction.find('.//nax:MinimumCustomerAge', ns).text if sales_restriction is not None and sales_restriction.find('.//nax:MinimumCustomerAge', ns) is not None else None,
                                            "TaxLevelID": item_tax.find('.//nax:TaxLevelID', ns).text if item_tax is not None and item_tax.find('.//nax:TaxLevelID', ns) is not None else None,
                                            "SalesTax": "{:.2f}".format(abs(float(tax))),
                                        }
                                    })
                                # Check for FuelLine
                                elif line.find('.//nax:FuelLine', ns) is not None:
                                    # Extract specific elements for FuelLine and append the data to the list
                                    transaction_line_data.append({
                                        "FuelLine": {
                                            "FuelGradeID": line.find('.//nax:FuelGradeID', ns).text if line.find('.//nax:FuelGradeID', ns) is not None else None,
                                            "FuelPositionID": line.find('.//nax:FuelPositionID', ns).text if line.find('.//nax:FuelPositionID', ns) is not None else None,
                                            "PriceTierCode": line.find('.//nax:PriceTierCode', ns).text if line.find('.//nax:PriceTierCode', ns) is not None else None,
                                            "TimeTierCode": line.find('.//nax:TimeTierCode', ns).text if line.find('.//nax:TimeTierCode', ns) is not None else None,
                                            "ServiceLevelCode": line.find('.//nax:ServiceLevelCode', ns).text if line.find('.//nax:ServiceLevelCode', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(abs(float(line.find('.//nax:ActualSalesPrice', ns).text))) if line.find('.//nax:ActualSalesPrice', ns) is not None else None,
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "RegularSellPrice": "{:.2f}".format(abs(float(line.find('.//nax:RegularSellPrice', ns).text))) if line.find('.//nax:RegularSellPrice', ns) is not None else None,
                                            "SalesQuantity": abs(float(line.find('.//nax:SalesQuantity', ns).text)) if line.find('.//nax:SalesQuantity', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                        }
                                    })

                                elif line.find('.//nax:FuelPrepayLine', ns) is not None:
                                    transaction_line_data.append({
                                        "FuelPrepayLine": {
                                            "FuelPositionID": line.find('.//nax:FuelPositionID', ns).text if line.find('.//nax:FuelPositionID', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                        }
                                    })
                            
                                # Check for TransactionTax
                                elif line.find('.//nax:TransactionTax', ns) is not None:
                                    transaction_line_data.append({
                                        "TransactionTax": {
                                            "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text,
                                            "TaxableSalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:TaxableSalesAmount', ns).text))) if line.find('.//nax:TaxableSalesAmount', ns) is not None else None,
                                            "TaxCollectedAmount": "{:.2f}".format(abs(float(line.find('.//nax:TaxCollectedAmount', ns).text))) if line.find('.//nax:TaxCollectedAmount', ns) is not None else None
                                        }
                                    })
                                # Check for TenderInfo
                                elif line.find('.//nax:TenderInfo', ns) is not None:
                                    transaction_line_data.append({
                                        "TenderInfo": {
                                            "TenderSubCode": line.find('.//nax:Tender/nax:TenderSubCode', ns).text if line.find('.//nax:Tender/nax:TenderSubCode', ns) is not None else None,
                                            "TenderAmount": "{:.2f}".format(abs(float(line.find('.//nax:TenderAmount', ns).text))) if line.find('.//nax:TenderAmount', ns) is not None else None
                                        }
                                    })

                        # Extract TransactionSummary
                        transaction_summary = event.find('.//nax:TransactionSummary', ns)
                        if transaction_summary is not None:
                            transaction_summary_data = {
                                "TransactionTotalGrossAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalGrossAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalGrossAmount', ns) is not None else None,
                                "TransactionTotalNetAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalNetAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalNetAmount', ns) is not None else None,
                                "TransactionTotalTaxSalesAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns) is not None else None,
                                "TransactionTotalTaxNetAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalTaxNetrAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalTaxNetrAmount', ns) is not None else None,
                                "TransactionTotalGrandAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalGrandAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalGrandAmount', ns) is not None else None,
                            }

                        # Extract specific elements from each SaleEvent element and append the data to the list
                        sale_event_data.append({
                            "EventSequenceID": event.find('.//nax:EventSequenceID', ns).text,
                            "CashierID": event.find('.//nax:CashierID', ns).text,
                            "RegisterID": event.find('.//nax:RegisterID', ns).text,
                            "TillID": event.find('.//nax:TillID', ns).text,
                            "TransactionID": event.find('.//nax:TransactionID', ns).text,
                            "EventStartDate": event.find('.//nax:EventStartDate', ns).text,
                            "EventStartTime": event.find('.//nax:EventStartTime', ns).text,
                            "EventEndDate": event.find('.//nax:EventEndDate', ns).text,
                            "EventEndTime": event.find('.//nax:EventEndTime', ns).text,
                            "ReceiptDate": event.find('.//nax:ReceiptDate', ns).text,
                            "ReceiptTime": event.find('.//nax:ReceiptTime', ns).text,
                            "TransactionLine": transaction_line_data,
                            "TransactionSummary": transaction_summary_data
                        })

                    # Add the SaleEvent data to the main data dictionary
                    if sale_event_data:
                        data["SaleEvent"] = sale_event_data
                        
                    # Find all OtherEvent elements
                    financial_events = root_2.findall('.//nax:FinancialEvent', ns)

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

                        financial_event_data.append({
                            "EventSequenceID": event.find('.//nax:EventSequenceID', ns).text,
                            "TransactionID": event.find('.//nax:TransactionID', ns).text,
                            "CashierID": event.find('.//nax:CashierID', ns).text,
                            "RegisterID": event.find('.//nax:RegisterID', ns).text,
                            "TillID": event.find('.//nax:TillID', ns).text,
                            "ReceiptDate": event.find('.//nax:ReceiptDate', ns).text,
                            "ReceiptTime": event.find('.//nax:ReceiptTime', ns).text,
                            "FinancialEventDetail": payout_detail_data
                        })

                    # Add the OtherEvent data to the main data dictionary
                    if financial_event_data:
                        data["FinancialEvent"] = financial_event_data

                    # Find all OtherEvent elements
                    void_events = root_2.findall('.//nax:VoidEvent', ns)

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
                                            "POSCode": line.find('.//nax:ItemCode//nax:POSCode', ns).text if line.find('.//nax:ItemCode//nax:POSCode', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(abs(float(line.find('.//nax:ActualSalesPrice', ns).text))) if line.find('.//nax:ActualSalesPrice', ns) is not None else None,
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "SellingUnits": line.find('.//nax:SellingUnits', ns).text if line.find('.//nax:SellingUnits', ns) is not None else None,
                                            "RegularSellPrice": "{:.2f}".format(abs(float(line.find('.//nax:RegularSellPrice', ns).text))) if line.find('.//nax:RegularSellPrice', ns) is not None else None,
                                            "SalesQuantity": abs(float(line.find('.//nax:SalesQuantity', ns).text)) if line.find('.//nax:SalesQuantity', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                            "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None,
                                            "SalesTax": "{:.2f}".format(abs(float(tax))),
                                            "ImageURL": img_url
                                        }
                                    })
                                elif line.find('.//nax:MerchandiseCodeLine', ns) is not None:
                                    # Extract specific elements for MerchandiseCodeLine and append the data to the list
                                    sales_restriction = line.find('.//nax:SalesRestriction', ns)
                                    item_tax = line.find('.//nax:ItemTax', ns)
                                    # Replace the fields in the dictionary below with the actual fields for MerchandiseCodeLine
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
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(abs(float(line.find('.//nax:ActualSalesPrice', ns).text))) if line.find('.//nax:ActualSalesPrice', ns) is not None else None,
                                            "RegularSellPrice": "{:.2f}".format(abs(float(line.find('.//nax:RegularSellPrice', ns).text))) if line.find('.//nax:RegularSellPrice', ns) is not None else None,
                                            "SalesQuantity": abs(float(line.find('.//nax:SalesQuantity', ns).text)) if line.find('.//nax:SalesQuantity', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                            "MinimumCustomerAge": sales_restriction.find('.//nax:MinimumCustomerAge', ns).text if sales_restriction is not None and sales_restriction.find('.//nax:MinimumCustomerAge', ns) is not None else None,
                                            "TaxLevelID": item_tax.find('.//nax:TaxLevelID', ns).text if item_tax is not None and item_tax.find('.//nax:TaxLevelID', ns) is not None else None,
                                            "SalesTax": "{:.2f}".format(abs(float(tax))),
                                        }
                                    })
                                # Check for FuelLine
                                elif line.find('.//nax:FuelLine', ns) is not None:
                                    # Extract specific elements for FuelLine and append the data to the list
                                    # Replace the fields in the dictionary below with the actual fields for FuelLine
                                    transaction_line_data.append({
                                        "FuelLine": {
                                            "FuelGradeID": line.find('.//nax:FuelGradeID', ns).text if line.find('.//nax:FuelGradeID', ns) is not None else None,
                                            "FuelPositionID": line.find('.//nax:FuelPositionID', ns).text if line.find('.//nax:FuelPositionID', ns) is not None else None,
                                            "PriceTierCode": line.find('.//nax:PriceTierCode', ns).text if line.find('.//nax:PriceTierCode', ns) is not None else None,
                                            "TimeTierCode": line.find('.//nax:TimeTierCode', ns).text if line.find('.//nax:TimeTierCode', ns) is not None else None,
                                            "ServiceLevelCode": line.find('.//nax:ServiceLevelCode', ns).text if line.find('.//nax:ServiceLevelCode', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(abs(float(line.find('.//nax:ActualSalesPrice', ns).text))) if line.find('.//nax:ActualSalesPrice', ns) is not None else None,
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "RegularSellPrice": "{:.2f}".format(abs(float(line.find('.//nax:RegularSellPrice', ns).text))) if line.find('.//nax:RegularSellPrice', ns) is not None else None,
                                            "SalesQuantity": abs(float(line.find('.//nax:SalesQuantity', ns).text)) if line.find('.//nax:SalesQuantity', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                            # Add more fields as needed
                                        }
                                    })

                                elif line.find('.//nax:FuelPrepayLine', ns) is not None:
                                    transaction_line_data.append({
                                        "FuelPrepayLine": {
                                            "FuelPositionID": line.find('.//nax:FuelPositionID', ns).text if line.find('.//nax:FuelPositionID', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                        }
                                    })

                                # Check for TransactionTax
                                elif line.find('.//nax:TransactionTax', ns) is not None:
                                    transaction_line_data.append({
                                        "TransactionTax": {
                                            "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text,
                                            "TaxableSalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:TaxableSalesAmount', ns).text))) if line.find('.//nax:TaxableSalesAmount', ns) is not None else None,
                                            "TaxCollectedAmount": "{:.2f}".format(abs(float(line.find('.//nax:TaxCollectedAmount', ns).text))) if line.find('.//nax:TaxCollectedAmount', ns) is not None else None
                                        }
                                    })
                                # Check for TenderInfo
                                elif line.find('.//nax:TenderInfo', ns) is not None:
                                    transaction_line_data.append({
                                        "TenderInfo": {
                                            "TenderSubCode": line.find('.//nax:Tender/nax:TenderSubCode', ns).text if line.find('.//nax:Tender/nax:TenderSubCode', ns) is not None else None,
                                            "TenderAmount": "{:.2f}".format(abs(float(line.find('.//nax:TenderAmount', ns).text))) if line.find('.//nax:TenderAmount', ns) is not None else None
                                        }
                                    })

                        # Extract TransactionSummary
                        transaction_summary = event.find('.//nax:TransactionSummary', ns)
                        if transaction_summary is not None:
                            transaction_summary_data = {
                                "TransactionTotalGrossAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalGrossAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalGrossAmount', ns) is not None else None,
                                "TransactionTotalNetAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalNetAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalNetAmount', ns) is not None else None,
                                "TransactionTotalTaxSalesAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns) is not None else None,
                                "TransactionTotalTaxNetAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalTaxNetrAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalTaxNetrAmount', ns) is not None else None,
                                "TransactionTotalGrandAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalGrandAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalGrandAmount', ns) is not None else None,
                            }

                        void_event_data.append({
                            "EventSequenceID": event.find('.//nax:EventSequenceID', ns).text,
                            "CashierID": event.find('.//nax:CashierID', ns).text,
                            "RegisterID": event.find('.//nax:RegisterID', ns).text,
                            "TillID": event.find('.//nax:TillID', ns).text,
                            "TransactionID": event.find('.//nax:TransactionID', ns).text,
                            "EventStartDate": event.find('.//nax:EventStartDate', ns).text,
                            "EventStartTime": event.find('.//nax:EventStartTime', ns).text,
                            "EventEndDate": event.find('.//nax:EventEndDate', ns).text,
                            "EventEndTime": event.find('.//nax:EventEndTime', ns).text,
                            "ReceiptDate": event.find('.//nax:ReceiptDate', ns).text,
                            "ReceiptTime": event.find('.//nax:ReceiptTime', ns).text,
                            "TransactionLine": transaction_line_data,
                            "TransactionSummary": transaction_summary_data
                        })

                    # Add the OtherEvent data to the main data dictionary
                    if void_event_data:
                        data["VoidEvent"] = void_event_data

                    # Find all SaleEvent elements
                    refund_events = root_2.findall('.//nax:RefundEvent', ns)

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
                                            "POSCode": line.find('.//nax:ItemCode//nax:POSCode', ns).text if line.find('.//nax:ItemCode//nax:POSCode', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(abs(float(line.find('.//nax:ActualSalesPrice', ns).text))) if line.find('.//nax:ActualSalesPrice', ns) is not None else None,
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "SellingUnits": line.find('.//nax:SellingUnits', ns).text if line.find('.//nax:SellingUnits', ns) is not None else None,
                                            "RegularSellPrice": "{:.2f}".format(abs(float(line.find('.//nax:RegularSellPrice', ns).text))) if line.find('.//nax:RegularSellPrice', ns) is not None else None,
                                            "SalesQuantity": abs(float(line.find('.//nax:SalesQuantity', ns).text)) if line.find('.//nax:SalesQuantity', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                            "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None,
                                            "SalesTax": "{:.2f}".format(abs(float(tax))),
                                            "ImageURL": img_url
                                        }
                                    })
                                elif line.find('.//nax:MerchandiseCodeLine', ns) is not None:
                                    # Extract specific elements for MerchandiseCodeLine and append the data to the list
                                    sales_restriction = line.find('.//nax:SalesRestriction', ns)
                                    item_tax = line.find('.//nax:ItemTax', ns)
                                    # Replace the fields in the dictionary below with the actual fields for MerchandiseCodeLine
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
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(abs(float(line.find('.//nax:ActualSalesPrice', ns).text))) if line.find('.//nax:ActualSalesPrice', ns) is not None else None,
                                            "RegularSellPrice": "{:.2f}".format(abs(float(line.find('.//nax:RegularSellPrice', ns).text))) if line.find('.//nax:RegularSellPrice', ns) is not None else None,
                                            "SalesQuantity": abs(float(line.find('.//nax:SalesQuantity', ns).text)) if line.find('.//nax:SalesQuantity', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                            "MinimumCustomerAge": sales_restriction.find('.//nax:MinimumCustomerAge', ns).text if sales_restriction is not None and sales_restriction.find('.//nax:MinimumCustomerAge', ns) is not None else None,
                                            "TaxLevelID": item_tax.find('.//nax:TaxLevelID', ns).text if item_tax is not None and item_tax.find('.//nax:TaxLevelID', ns) is not None else None,
                                            "SalesTax": "{:.2f}".format(abs(float(tax))),
                                        }
                                    })
                                # Check for FuelLine
                                elif line.find('.//nax:FuelLine', ns) is not None:
                                    # Extract specific elements for FuelLine and append the data to the list
                                    # Replace the fields in the dictionary below with the actual fields for FuelLine
                                    transaction_line_data.append({
                                        "FuelLine": {
                                            "FuelGradeID": line.find('.//nax:FuelGradeID', ns).text if line.find('.//nax:FuelGradeID', ns) is not None else None,
                                            "FuelPositionID": line.find('.//nax:FuelPositionID', ns).text if line.find('.//nax:FuelPositionID', ns) is not None else None,
                                            "PriceTierCode": line.find('.//nax:PriceTierCode', ns).text if line.find('.//nax:PriceTierCode', ns) is not None else None,
                                            "TimeTierCode": line.find('.//nax:TimeTierCode', ns).text if line.find('.//nax:TimeTierCode', ns) is not None else None,
                                            "ServiceLevelCode": line.find('.//nax:ServiceLevelCode', ns).text if line.find('.//nax:ServiceLevelCode', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "ActualSalesPrice": "{:.2f}".format(abs(float(line.find('.//nax:ActualSalesPrice', ns).text))) if line.find('.//nax:ActualSalesPrice', ns) is not None else None,
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "RegularSellPrice": "{:.2f}".format(abs(float(line.find('.//nax:RegularSellPrice', ns).text))) if line.find('.//nax:RegularSellPrice', ns) is not None else None,
                                            "SalesQuantity": abs(float(line.find('.//nax:SalesQuantity', ns).text)) if line.find('.//nax:SalesQuantity', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                            # Add more fields as needed
                                        }
                                    })

                                elif line.find('.//nax:FuelPrepayLine', ns) is not None:
                                    transaction_line_data.append({
                                        "FuelPrepayLine": {
                                            "FuelPositionID": line.find('.//nax:FuelPositionID', ns).text if line.find('.//nax:FuelPositionID', ns) is not None else None,
                                            "Description": line.find('.//nax:Description', ns).text if line.find('.//nax:Description', ns) is not None else None,
                                            "MerchandiseCode": line.find('.//nax:MerchandiseCode', ns).text if line.find('.//nax:MerchandiseCode', ns) is not None else None,
                                            "SalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:SalesAmount', ns).text))) if line.find('.//nax:SalesAmount', ns) is not None else None,
                                        }
                                    })

                                # Check for TransactionTax
                                elif line.find('.//nax:TransactionTax', ns) is not None:
                                    transaction_line_data.append({
                                        "TransactionTax": {
                                            "TaxLevelID": line.find('.//nax:TaxLevelID', ns).text if line.find('.//nax:TaxLevelID', ns) is not None else None,
                                            "TaxableSalesAmount": "{:.2f}".format(abs(float(line.find('.//nax:TaxableSalesAmount', ns).text))) if line.find('.//nax:TaxableSalesAmount', ns) is not None else None,
                                            "TaxCollectedAmount": "{:.2f}".format(abs(float(line.find('.//nax:TaxCollectedAmount', ns).text))) if line.find('.//nax:TaxCollectedAmount', ns) is not None else None
                                        }
                                    })
                                # Check for TenderInfo
                                elif line.find('.//nax:TenderInfo', ns) is not None:
                                    transaction_line_data.append({
                                        "TenderInfo": {
                                            "TenderSubCode": line.find('.//nax:Tender/nax:TenderSubCode', ns).text if line.find('.//nax:Tender/nax:TenderSubCode', ns) is not None else None,
                                            "TenderAmount": "{:.2f}".format(abs(float(line.find('.//nax:TenderAmount', ns).text))) if line.find('.//nax:TenderAmount', ns) is not None else None
                                        }
                                    })

                        # Extract TransactionSummary
                        transaction_summary = event.find('.//nax:TransactionSummary', ns)
                        if transaction_summary is not None:
                            transaction_total_grand_amount = transaction_summary.find('nax:TransactionTotalGrandAmount', ns)
                            transaction_summary_data = {
                                "TransactionTotalGrossAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalGrossAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalGrossAmount', ns) is not None else None,
                                "TransactionTotalNetAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalNetAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalNetAmount', ns) is not None else None,
                                "TransactionTotalTaxSalesAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalTaxSalesAmount', ns) is not None else None,
                                "TransactionTotalTaxNetAmount": "{:.2f}".format(abs(float(transaction_summary.find('nax:TransactionTotalTaxNetrAmount', ns).text))) if transaction_summary.find('nax:TransactionTotalTaxNetrAmount', ns) is not None else None,
                                #"TransactionTotalGrandAmount": transaction_summary.find('nax:TransactionTotalGrandAmount', ns).text,
                                "TransactionTotalGrandAmount": {
                                    "@direction": transaction_total_grand_amount.attrib['direction'],
                                    "#text": transaction_total_grand_amount.text
                                }
                            }

                        # Extract specific elements from each SaleEvent element and append the data to the list
                        refund_event_data.append({
                            "EventSequenceID": event.find('.//nax:EventSequenceID', ns).text,
                            "CashierID": event.find('.//nax:CashierID', ns).text,
                            "RegisterID": event.find('.//nax:RegisterID', ns).text,
                            "TillID": event.find('.//nax:TillID', ns).text,
                            "TransactionID": event.find('.//nax:TransactionID', ns).text,
                            "EventStartDate": event.find('.//nax:EventStartDate', ns).text,
                            "EventStartTime": event.find('.//nax:EventStartTime', ns).text,
                            "EventEndDate": event.find('.//nax:EventEndDate', ns).text,
                            "EventEndTime": event.find('.//nax:EventEndTime', ns).text,
                            "ReceiptDate": event.find('.//nax:ReceiptDate', ns).text,
                            "ReceiptTime": event.find('.//nax:ReceiptTime', ns).text,
                            "TransactionLine": transaction_line_data,
                            
                            "TransactionSummary": transaction_summary_data,
                            "ApproverID": event.find('.//nax:ApproverID', ns).text,
                            "RefundReason": event.find('.//nax:RefundReason', ns).text
                        })

                    # Add the SaleEvent data to the main data dictionary
                    if refund_event_data:
                        data["RefundEvent"] = refund_event_data
                    print(data)

                    # Insert JSON into MongoDB collection
                    collection = db['vfdailysales']
                    collection.insert_one(data)

                    print(f"JSON data for reptnum {reptnum} inserted into MongoDB collection successfully.")
                else:
                    print(f"Data for reptnum {reptnum} already exists in the MongoDB collection.")
            else:
                print(f"Failed to fetch XML data from the second link for reptnum {reptnum}. HTTP status code: {response_2.status_code}")
    else:
        print("Cookie element not found in the XML data.")
else:
    print(f"Failed to fetch XML data from the first link. HTTP status code: {response.status_code}")