from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb+srv://rtnsmart:elneebekRf3dH30z@rtn.gfl4v.mongodb.net/')

# Access the sales data collection
db = client['verifone']
collection = db['vfdailysales']

# Get the latest document
storeid = '66337c59c31dc3e37229f275'  # replace with your actual store id
latest_document = collection.find({'storeid': storeid}).sort('_id', -1).limit(1)[0]

# Connect to the department_config collection
vposcfg_collection = db['vposcfg']

# Get the department_config document for the store
vposcfg_document = vposcfg_collection.find_one({'storeid': storeid})

# Extract the department sysid and name to create the merchandise_names dictionary
merchandise_names = {int(department['sysid']): department['name'] for department in vposcfg_document['domain:posConfig']['departments']['department']}

# Connect to the fuel_price collection
vfuelprices_collection = db['vfuelprices']

# Get the fuel_price document for the store
vfuelprices_document = vfuelprices_collection.find_one({'storeid': storeid})

# Extract the fuel product sysid and name to create the fuel_names dictionary
fuel_names = {int(product['sysid']): product['name'] for product in vfuelprices_document['fuel:fuelPrices']['fuelProducts']['fuelProduct']}

# Calculate merchandise code counts for each document
#for document in collection.find():
if 'SaleEvent' in latest_document:
    merchandise_code_counts = {}
    merchandise_code_quantity = {}
    merchandise_code_amounts = {}
    merchandise_code_tax = {}
    for sale_event in latest_document['SaleEvent']:
        for transaction_line in sale_event['TransactionLine']:
            if 'ItemLine' in transaction_line:
                merchandise_code = transaction_line['ItemLine']['MerchandiseCode']
                merchandise_code_counts[merchandise_code] = merchandise_code_counts.get(merchandise_code, 0) + 1
                merchandise_code_amounts[merchandise_code] = merchandise_code_amounts.get(merchandise_code, 0) + float(transaction_line['ItemLine']['SalesAmount'])
                sales_quantity = float(transaction_line['ItemLine']['SalesQuantity'])
                merchandise_code_quantity[merchandise_code] = merchandise_code_quantity.get(merchandise_code, 0) + sales_quantity
                merchandise_code_tax[merchandise_code] = merchandise_code_tax.get(merchandise_code, 0) + float(transaction_line['ItemLine']['SalesTax'])  # Add tax to total
            if 'MerchandiseCodeLine' in transaction_line:
                merchandise_code = transaction_line['MerchandiseCodeLine']['MerchandiseCode']
                merchandise_code_counts[merchandise_code] = merchandise_code_counts.get(merchandise_code, 0) + 1
                merchandise_code_amounts[merchandise_code] = merchandise_code_amounts.get(merchandise_code, 0) + float(transaction_line['MerchandiseCodeLine']['SalesAmount'])
                sales_quantity = float(transaction_line['MerchandiseCodeLine']['SalesQuantity'])
                merchandise_code_quantity[merchandise_code] = merchandise_code_quantity.get(merchandise_code, 0) + sales_quantity
                merchandise_code_tax[merchandise_code] = merchandise_code_tax.get(merchandise_code, 0) + float(transaction_line['MerchandiseCodeLine']['SalesTax'])  # Add tax to total
            if 'FuelLine' in transaction_line:
                merchandise_code = transaction_line['FuelLine']['MerchandiseCode']
                merchandise_code_counts[merchandise_code] = merchandise_code_counts.get(merchandise_code, 0) + 1
                merchandise_code_amounts[merchandise_code] = merchandise_code_amounts.get(merchandise_code, 0) + float(transaction_line['FuelLine']['SalesAmount'])
                sales_quantity = float(transaction_line['FuelLine']['SalesQuantity'])
                merchandise_code_quantity[merchandise_code] = merchandise_code_quantity.get(merchandise_code, 0) + sales_quantity

#Fuel Summary
#for document in collection.find():
if 'SaleEvent' in latest_document:
    fuel_code_counts = {}
    fuel_grade_sales_quantity = {}
    fuel_grade_sales_amount = {}
    for sale_event in latest_document['SaleEvent']:
        for transaction_line in sale_event['TransactionLine']:
            if 'FuelLine' in transaction_line:
                fuel_grade_id = transaction_line['FuelLine']['FuelGradeID']                   
                fuel_code_counts[fuel_grade_id] = fuel_code_counts.get(fuel_grade_id, 0) + 1
                fuel_grade_sales_quantity[fuel_grade_id] = fuel_grade_sales_quantity.get(fuel_grade_id, 0) + float(transaction_line['FuelLine']['SalesQuantity'])
                fuel_grade_sales_amount[fuel_grade_id] = fuel_grade_sales_amount.get(fuel_grade_id, 0) + float(transaction_line['FuelLine']['SalesAmount'])

# Void Summary
#for document in collection.find():
if 'VoidEvent' in latest_document:
    void_code_counts = {}
    void_code_quantity = {}
    void_code_amounts = {}
    void_code_tax = {}
    for void_event in latest_document['VoidEvent']:
        for transaction_line in void_event['TransactionLine']:
            if 'ItemLine' in transaction_line:
                void_code = transaction_line['ItemLine']['MerchandiseCode']
                void_code_counts[void_code] = void_code_counts.get(void_code, 0) + 1
                void_code_amounts[void_code] = void_code_amounts.get(void_code, 0) + float(transaction_line['ItemLine']['SalesAmount'])
                sales_quantity = float(transaction_line['ItemLine']['SalesQuantity'])
                void_code_quantity[void_code] = void_code_quantity.get(void_code, 0) + sales_quantity
                void_code_tax[void_code] = void_code_tax.get(void_code, 0) + float(transaction_line['ItemLine']['SalesTax'])  # Add tax to total
            if 'MerchandiseCodeLine' in transaction_line:
                void_code = transaction_line['MerchandiseCodeLine']['MerchandiseCode']
                void_code_counts[void_code] = void_code_counts.get(void_code, 0) + 1
                void_code_amounts[void_code] = void_code_amounts.get(void_code, 0) + float(transaction_line['MerchandiseCodeLine']['SalesAmount'])
                sales_quantity = float(transaction_line['MerchandiseCodeLine']['SalesQuantity'])
                void_code_quantity[void_code] = void_code_quantity.get(void_code, 0) + sales_quantity
                void_code_tax[void_code] = void_code_tax.get(void_code, 0) + float(transaction_line['MerchandiseCodeLine']['SalesTax'])  # Add tax to total
            if 'FuelLine' in transaction_line:
                void_code = transaction_line['FuelLine']['MerchandiseCode']
                void_code_counts[void_code] = void_code_counts.get(void_code, 0) + 1
                void_code_amounts[void_code] = void_code_amounts.get(void_code, 0) + float(transaction_line['FuelLine']['SalesAmount'])
                sales_quantity = float(transaction_line['FuelLine']['SalesQuantity'])
                void_code_quantity[void_code] = void_code_quantity.get(void_code, 0) + sales_quantity

# Refvoid
refund_code_counts = {}
refund_code_quantity = {}
refund_code_amounts = {}
refund_code_tax = {}
#for document in collection.find():
if 'RefundEvent' in latest_document:
    for refund_event in latest_document['RefundEvent']:
        for transaction_line in refund_event['TransactionLine']:
            if 'ItemLine' in transaction_line:
                refund_code = transaction_line['ItemLine']['MerchandiseCode']
                refund_code_counts[refund_code] = refund_code_counts.get(refund_code, 0) + 1
                refund_code_amounts[refund_code] = refund_code_amounts.get(refund_code, 0) + float(transaction_line['ItemLine']['SalesAmount'])
                sales_quantity = float(transaction_line['ItemLine']['SalesQuantity'])
                refund_code_quantity[refund_code] = refund_code_quantity.get(refund_code, 0) + sales_quantity
                refund_code_tax[refund_code] = refund_code_tax.get(refund_code, 0) + float(transaction_line['ItemLine']['SalesTax'])  # Add tax to total
            if 'MerchandiseCodeLine' in transaction_line:
                refund_code = transaction_line['MerchandiseCodeLine']['MerchandiseCode']
                refund_code_counts[refund_code] = refund_code_counts.get(refund_code, 0) + 1
                refund_code_amounts[refund_code] = refund_code_amounts.get(refund_code, 0) + float(transaction_line['MerchandiseCodeLine']['SalesAmount'])
                sales_quantity = float(transaction_line['MerchandiseCodeLine']['SalesQuantity'])
                refund_code_quantity[refund_code] = refund_code_quantity.get(refund_code, 0) + sales_quantity
                refund_code_tax[refund_code] = refund_code_tax.get(refund_code, 0) + float(transaction_line['MerchandiseCodeLine']['SalesTax'])  # Add tax to total
            if 'FuelLine' in transaction_line:
                refund_code = transaction_line['FuelLine']['MerchandiseCode']
                refund_code_counts[refund_code] = refund_code_counts.get(refund_code, 0) + 1
                refund_code_amounts[refund_code] = refund_code_amounts.get(refund_code, 0) + float(transaction_line['FuelLine']['SalesAmount'])
                sales_quantity = float(transaction_line['FuelLine']['SalesQuantity'])
                refund_code_quantity[refund_code] = refund_code_quantity.get(refund_code, 0) + sales_quantity

# Convert merchandise code counts dictionary to list of dictionaries
sales_summary = [{'img': "", 'merchandisecode': code, 'description': merchandise_names.get(int(code), 'Unknown'), 'count': count, 'quantity': merchandise_code_quantity.get(code, 0), 'salesTax': round(merchandise_code_tax.get(code, 0), 2), 'amount': round(merchandise_code_amounts[code], 2)} for code, count in merchandise_code_counts.items()]

# Convert fuel code counts dictionary to list of dictionaries
fuel_summary = [{'img': "", 'fuelGradeID': id, 'description': fuel_names.get(int(id), 'Unknown'), 'count': count, 'gallons': round(fuel_grade_sales_quantity.get(id, 0), 2), 'amount': round(fuel_grade_sales_amount.get(id, 0), 2)} for id, count in fuel_code_counts.items()]

# Convert merchandise code counts dictionary to list of dictionaries
void_summary = [{'img': "", 'merchandisecode': code, 'description': merchandise_names.get(int(code), 'Unknown'), 'count': count, 'quantity': void_code_quantity.get(code, 0), 'salesTax': round(void_code_tax.get(code, 0), 2), 'amount': round(void_code_amounts[code], 2)} for code, count in void_code_counts.items()]

# Convert merchandise code counts dictionary to list of dictionaries
refund_summary = [{'img': "", 'merchandisecode': code, 'description': merchandise_names.get(int(code), 'Unknown'), 'count': count, 'quantity': refund_code_quantity.get(code, 0), 'salesTax': round(refund_code_tax.get(code, 0), 2), 'amount': round(refund_code_amounts[code], 2)} for code, count in refund_code_counts.items()]

# Calculate the summary totals
total_quantity = round(sum(item['quantity'] for item in sales_summary), 2)
total_sales_tax = round(sum(item['salesTax'] for item in sales_summary), 2)
total_amount = round(sum(item['amount'] for item in sales_summary), 2)

sales_summary_totals = {
    'totalQuantity': total_quantity,
    'totalSalesTax': total_sales_tax,
    'totalAmount': total_amount
}

# Calculate the fuel totals
total_count = round(sum(item['count'] for item in fuel_summary), 2)
total_gallons = round(sum(item['gallons'] for item in fuel_summary), 2)
total_amount = round(sum(item['amount'] for item in fuel_summary), 2)

fuel_summary_totals = {
    'totalCount': total_count,
    'totalGallons': total_gallons,
    'totalAmount': total_amount
}

# Calculate the Void totals
total_quantity = round(sum(item['quantity'] for item in void_summary), 2)
total_sales_tax = round(sum(item['salesTax'] for item in void_summary), 2)
total_amount = round(sum(item['amount'] for item in void_summary), 2)

void_summary_totals = {
    'totalQuantity': total_quantity,
    'totalSalesTax': total_sales_tax,
    'totalAmount': total_amount
}

# Calculate the Refund totals
total_quantity = round(sum(item['quantity'] for item in refund_summary), 2)
total_sales_tax = round(sum(item['salesTax'] for item in refund_summary), 2)
total_amount = round(sum(item['amount'] for item in refund_summary), 2)

refund_summary_totals = {
    'totalQuantity': total_quantity,
    'totalSalesTax': total_sales_tax,
    'totalAmount': total_amount
}

new_document = {
    'StoreLocationID': latest_document['StoreLocationID'],
    'corpid': latest_document['corpid'],
    'storeid': latest_document['storeid'], 
    'ReportSequenceNumber': latest_document['ReportSequenceNumber'],
    'BeginDate': latest_document['BeginDate'],
    'BeginTime': latest_document['BeginTime'],
    'EndDate': latest_document['EndDate'],
    'EndTime': latest_document['EndTime'],
}

# Only append sales summary and totals if they are not empty or zero
if sales_summary and sales_summary_totals['totalQuantity'] != 0 and sales_summary_totals['totalSalesTax'] != 0 and sales_summary_totals['totalAmount'] != 0:
    new_document['salesSummary'] = sales_summary
    new_document['salesSummaryTotals'] = sales_summary_totals

# Only append fuel summary and totals if they are not empty or zero
if fuel_summary and fuel_summary_totals['totalCount'] != 0 and fuel_summary_totals['totalGallons'] != 0 and fuel_summary_totals['totalAmount'] != 0:
    new_document['fuelSummary'] = fuel_summary
    new_document['fuelSummaryTotals'] = fuel_summary_totals

# Only append void summary and totals if they are not empty or zero
if void_summary and void_summary_totals['totalQuantity'] != 0 and void_summary_totals['totalSalesTax'] != 0 and void_summary_totals['totalAmount'] != 0:
    new_document['voidSummary'] = void_summary
    new_document['voidSummaryTotals'] = void_summary_totals

# Only append refund summary and totals if they are not empty or zero
if refund_summary and refund_summary_totals['totalQuantity'] != 0 and refund_summary_totals['totalSalesTax'] != 0 and refund_summary_totals['totalAmount'] != 0:
    new_document['refundSummary'] = refund_summary
    new_document['refundSummaryTotals'] = refund_summary_totals

# Connect to the other collection
other_collection = db['vfsummaryall']

# Insert the new document
other_collection.insert_one(new_document)

