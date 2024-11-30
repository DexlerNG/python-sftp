import requests
import json
import time

from utils import generate_access_token

def update_flight_records(access_token=''):
    # 50 minutes refresh time 
    interval_seconds = 50 * 60
    start_time = time.time()

    module_api_name = 'Flight_Information'

    # Zoho API URL to get records from a custom module
    get_records_url = f"https://www.zohoapis.com/crm/v5/{module_api_name}"

    # Define headers with Authorization and Content-Type
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }

    page_token = None
    records = []

    while True:
        # Fetch records for the current page
        params = {
            'per_page': 200,
            'fields': 'id,PNR_NO'
        }

        # If there is a page_token, add it to the request params for pagination
        if page_token:
            params['page_token'] = page_token

        response = requests.get(get_records_url, headers=headers, params=params)
        response_data = response.json()
        
        if response.status_code == 200:
            # Add the fetched records to the total list
            records.extend(response_data.get("data", []))

            # Check for the next_page token to continue fetching if available
            page_token = response_data.get('info', {}).get('next_page_token')

            # If there's no next_page_token, we've retrieved all records
            if not page_token:
                break

        else:
            print(f"Failed to retrieve records. Status code: {response.status_code}")
            print(f"Error text is {response.text}")
            break

    print(f"Total records retrieved: {len(records)}")

    # Now iterate through the records as before
    for record in records:
        # Check if access token is still valid
        elapsed_time = time.time() - start_time
        
        if elapsed_time >= interval_seconds:
            access_token = generate_access_token()
            start_time = time.time()  # Reset the start time

        print(f'Record {record} in Records')
        record_id = record.get("id")
        pnr_no = record.get("PNR_NO")
        customer = record.get("Customer")

        print(f"Record ID: {record_id}, RNR_NO: {pnr_no} and Customer: {customer}")

        # Get Customer lookup ID from Contacts module
        if not customer:
            customer_id = get_contact_id(pnr_no=pnr_no, access_token=access_token)
            print(f'Returned Customer ID is: {customer_id}')

            # Update the record
            update_customer_value(record_id, access_token, customer_id)


def get_contact_id(pnr_no='', access_token=''):
    customer_id = ''
    url = f'https://www.zohoapis.com/crm/v7/Contacts/search?criteria=PNR_NO:equals:{pnr_no}'

    headers = {
    'Authorization': f'Zoho-oauthtoken {access_token}'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        # Parse the JSON response
        result = response.json().get("data",[])

        # Retrieve the top-level 'id' only, ignoring nested dictionaries
        for item in result:
            if 'id' in item and isinstance(item['id'], str):  # Ensure 'id' exists and is a string
                customer_id = item['id']
                
        print(f"Customer ID for PNR_NO: {pnr_no} is {customer_id}:")
    else:
        print(f"Failed to retrieve fields. Status code: {response.status_code}")
        print(f"Error message inside get_contact_id: {response.text}")
    
    return customer_id

def update_customer_value(record_id='', access_token='', customer_id=''):
    print('Inside function update_customer_value')

    if customer_id:
        module_api_name = 'Flight_Information' 

        # Zoho API URL to update a record in a custom module
        update_url = f"https://www.zohoapis.com/crm/v5/{module_api_name}/{record_id}"

        # Data payload with the updated api_name field
        update_data = {
            "data": [
                {
                    "Customer": int(customer_id)
                }
            ]
        }

        # Convert the payload to JSON format
        json_data = json.dumps(update_data, default=set_default)

        # Define headers with Authorization and Content-Type
        headers = {
            'Authorization': f'Zoho-oauthtoken {access_token}',
            'Content-Type': 'application/json'
        }

        # Make the request to update the record
        response = requests.put(update_url, headers=headers, data=json_data)

        # Check response status
        if response.status_code == 200:
            print("Record updated successfully!")
            #print("Response:", response.json())
        else:
            print(f"Failed to update record. Status code: {response.status_code}")
            print("Response:", response.text)

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError
