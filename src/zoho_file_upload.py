import requests
import os

def upload_file_to_zoho(file_name='', access_token=''):
    file_ids = {}

    # File to upload (CSV with contact data)
    upload_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'upload'))

    # Endpoint to upload files
    upload_url = 'https://content.zohoapis.com/crm/v5/upload'

    # Headers for the API call
    headers = {
    'Authorization': f'Zoho-oauthtoken {access_token}',
    'X-CRM-ORG': '868304027',
    'feature': 'bulk-write'
    }

    for file in os.listdir(upload_path):
        full_path = os.path.join(upload_path, file)
        #full_path = f'{upload_path}/{file}'
        print(f'Full path: {full_path}')
        if file_name in full_path:
            with open(full_path, 'rb') as file_data:
                files = {'file': file_data}
                response = requests.post(upload_url, headers=headers, files=files)

                # Check if the upload was successful
                if response.status_code == 200:
                    upload_result = response.json()
                    print(f'File {file} upload code: {upload_result['code']}')
                    # Retrieve the file ID
                    file_id = upload_result['details']['file_id']
                    file_ids[file] = file_id
                    print(f'File {file} File ID: {file_id}')
                else:
                    print('Error status code:', response.status_code)
                    print('Error status code:', response.headers)
                    print(f'Error uploading file {file}: {response.text}')

    return file_ids
