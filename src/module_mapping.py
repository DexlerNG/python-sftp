import requests

def get_module_mapping_fields(access_token=''):
    # Replace with the custom module API name
    module_api_name = 'Contacts'

    # Zoho API URL to get module metadata
    url = f'https://www.zohoapis.com/crm/v5/settings/modules/{module_api_name}'

    # Set up the headers with the OAuth token
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}'
    }

    # Send the request to Zoho API
    response = requests.get(url, headers=headers)

    # Check the status code of the response
    if response.status_code == 200:
        # Parse the JSON response
        module_metadata = response.json()
        print('Response Text')
        print(response.text)
        print(f'Module retrieved json data: {module_metadata}')
        # Extract the field mappings from the metadata
        fields = module_metadata.get('modules', [])[0].get('fields', [])
        
        # Print out the field mappings
        print(f"Field mappings for module: {module_api_name}")
        for field in fields:
            field_label = field.get('field_label')
            field_api_name = field.get('api_name')
            print(f"{field_label} ({field_api_name})")
    else:
        print(f"Failed to retrieve module metadata. Status code: {response.status_code}")
        print(response.text)