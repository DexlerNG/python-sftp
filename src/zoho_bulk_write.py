import requests
import json

from module_mapping2 import get_module_mapping_fields

def create_bulk_write_jobs(file_ids, access_token=''):

    job_ids = []

    for key, value in file_ids.items():
        print(f'File name: {key} with File ID: {value} inside function create_bulk_write_jobs')
        if "contact" in key.lower() and value:
            # Bulk job request body
            bulk_write_job_data = {
                "operation": "upsert",
                "resource": [
                    {
                        "type": "data",
                        "module": {
                            "api_name": "Contacts"
                        },
                        "find_by": "id",
                        "file_id": value,
                        "field_mappings": [
                            {
                                "api_name": "id",
                                "index": 0
                            },
                            {
                                "api_name": "Last_Name",
                                "index": 2
                            },{
                                "api_name": "PNR_NO",
                                "index": 0
                            },
                            {
                                "api_name": "PAX_GENDER",
                                "index": 1
                            },
                            {
                                "api_name": "PAX_NAME",
                                "index": 2
                            },
                            {
                                "api_name": "PAX_BIRTHDAY",
                                "index": 3
                            },
                            {
                                "api_name": "Email",
                                "index": 4
                            },
                            {
                                "api_name": "MEMBER_ID",
                                "index": 5
                            },
                            {
                                "api_name": "Phone",
                                "index": 6
                            }
                        ]
                    }
                ]
            }

            job_id = write_json_data(json_data=bulk_write_job_data, access_token=access_token, value=value)
            job_ids.append(job_id)

        if "flight" in key.lower() and value:
            layout_id = get_module_mapping_fields(module_api_name='Flight_Information', access_token=access_token)

            print(f'Inside the FLIGHT IF stmt...flight in {key.lower()} and VALUE is {value}')  

        #for layout_id in layout_ids:
            # Bulk job request body
            bulk_write_job_data = {
                "operation": "upsert",
                "resource": [
                    {
                        "type": "data",
                        "module": {
                            "api_name": "Flight_Information"
                        },
                        "find_by": "id",
                        "file_id": value,
                        "field_mappings": [
                            {
                                "api_name": "Layout",
                                "default_value": {
                                    "value": layout_id
                                }
                            },
                            {
                                "api_name": "id",
                                "index": 0
                            },
                            {
                                "api_name": "Cpn_Status",
                                "index": 3
                            },
                            {
                                "api_name": "PNR_NO",
                                "index": 0
                            },
                            {
                                "api_name": "TKT_NO",
                                "index": 8
                            },
                            {
                                "api_name": "ARR_PORT",
                                "index": 1
                            },
                            {
                                "api_name": "DEP_PORT",
                                "index": 4
                            },
                            {
                                "api_name": "FARE_BASIS",
                                "index": 5
                            },
                            {
                                "api_name": "FLT_NO",
                                "index": 6
                            },
                            {
                                "api_name": "LEG_STATUS",
                                "index": 7
                            },
                            {
                                "api_name": "TKT_TYPE",
                                "index": 9
                            },
                            {
                                "api_name": "PAX_TYPE",
                                "index": 10
                            },
                            {
                                "api_name": "Sourc",
                                "index": 11
                            },
                            {
                                "api_name": "CLASS_CABIN_2",
                                "index": 2
                            },
                            {
                                "api_name": "Name",
                                "index": 12
                            },
                            {
                                "api_name": "PAX_NAME",
                                "index": 12
                            },
                            {
                                "api_name": "Customer",
                                "find_by": "id",
                                "index": 0
                            }
                        ]
                    }
                ]
            }

            job_id = write_json_data(json_data=bulk_write_job_data, access_token=access_token, value=value)
            job_ids.append(job_id)


    return job_ids


def write_json_data(json_data='', access_token='', value=''):
    job_id = None
    headers = {
    'Authorization': f'Zoho-oauthtoken {access_token}',
    'Content-Type': 'application/json'
    }

    print('Before writing bulk_write_job_data')    

    if json_data:
        print(json_data)

    # Make the API request to initiate the bulk write job
    response = requests.post(
        'https://www.zohoapis.com/crm/bulk/v5/write',
        headers=headers,
        data=json.dumps(json_data)
    )

    job_response = response.json()

    # Check the response
    # Response Status Code is: 201
    if job_response['status'] == 'success':
        job_id = job_response['details']['id']
        created_by_id = job_response['details']['created_by']['id']
        created_by_name = job_response['details']['created_by']['name']
        print(f'Job ID for file {value} is {job_id}; Created By id: {created_by_id} with name: {created_by_name}')
    else:
        print(f'Error Message for file {value}: {job_response['message']}')
        print(f'Error Status Code for file {value}: {response.status_code}')

    return job_id
