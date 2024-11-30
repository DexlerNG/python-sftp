import requests

def check_job_status(job_ids, access_token=''):
    for job_id in job_ids:
        # Headers for the API call
        headers = {
            'Authorization': f'Zoho-oauthtoken {access_token}'
        }

        # Make the API request to check job status
        response = requests.get(
            f'https://www.zohoapis.com/crm/bulk/v5/write/{job_id}',
            headers=headers
        )

        job_status = response.json()
        print(f'Write job status Status code for {job_id}: {response.status_code}')
        print(f'Write job status Response text for {job_id}: {response.text}')

        # Check the response
        if response.status_code == 200:
            job_status = response.json()
            print(f'{job_id} job Status: {job_status}')
        else:
            print(f'Error fetching write job status detail for{job_id} : {response.text}')
