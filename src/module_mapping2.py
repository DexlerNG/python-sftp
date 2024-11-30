import requests

def get_module_mapping_fields(module_api_name='', access_token=''):
    lOut_ids = []

    # Zoho API URL to get layouts for the custom module
    url = f'https://www.zohoapis.com/crm/v5/settings/layouts?module={module_api_name}'

    # Set up the headers with the OAuth token
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}'
    }

    # Send the request to Zoho API
    response = requests.get(url, headers=headers)

    # Check the status code of the response
    if response.status_code == 200:
        # Parse the JSON response
        layouts = response.json().get('layouts', [])

        print(f'Length of Layout: {len(layouts)}')
#        print(f'Layouts: {layouts}')
        
        for layout in layouts:
            # Extract the first layout's ID (or you can loop through all layouts)
            layout_id = layout.get('id')
            lOut_ids.append(layout_id)
            print(f"Layout ID: {layout_id}")
            # Zoho API URL to get fields for the layout
            fields_url = f'https://www.zohoapis.com/crm/v5/settings/fields?layout_id={layout_id}&module={module_api_name}'

            # Send the request to Zoho API
            fields_response = requests.get(fields_url, headers=headers)

            # Check the status code of the response
            if fields_response.status_code == 200:
                # Parse the JSON response
                fields_data = fields_response.json().get('fields', [])
                
                print(f"Fields for layout {layout_id} in module {module_api_name}:")
                for field in fields_data:
                    field_label = field.get('field_label')
                    field_api_name = field.get('api_name')
                    print(f"{field_label} ({field_api_name})")
            else:
                print(f"Failed to retrieve fields. Status code: {fields_response.status_code}")
                print(fields_response.text)
    else:
        print(f"Failed to retrieve layouts. Status code: {response.status_code}")
        print(response.text)

        # Assuming you have already retrieved the layout_id and module_api_name
    print(f'Layout IDs for use in bulk write are: {lOut_ids[0]}')
    return lOut_ids[0]

