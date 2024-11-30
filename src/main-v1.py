import configparser
import pysftp
import psycopg2.extras
import pyarrow.parquet as pq
import os

from datetime import datetime as dt
from zoho_bulk_write import create_bulk_write_jobs
from zoho_file_upload import upload_file_to_zoho
from zoho_job_status import check_job_status
from utils import get_connection_2_database, generate_access_token, split_csv_by_rows
from module_mapping2 import get_module_mapping_fields
from update_flight_info import update_flight_records

# Gets the full path to the directory this "main.py" file is contained in
# Because it is reading from a virtual environment
os_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(os_path)

print(f'OS Path: {os_path}')

# Initialize the config parser
config = configparser.ConfigParser()

# Read the config file
config.read('config.ini')

# Access the config values for SFTP
sftpHost = config['credentials']['sftpHost']
sftpPort = int(config['credentials']['sftpPort'])
sftpUname = config['credentials']['sftpUname']
sftpPassword = config['credentials']['sftpPassword']

conn = get_connection_2_database()
access_token = generate_access_token()

def get_files_from_sftp():
    # Setting the SFTP server hostkey to None
    cnOpts = pysftp.CnOpts()
    cnOpts.hostkeys = None

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        with pysftp.Connection(host=sftpHost, port=sftpPort, username=sftpUname, password=sftpPassword, cnopts=cnOpts) as sftp:
            print('Connection to the Zoho SFTP Server has been established!')
            #pwd = sftp.pwd

            # Only inserts the newly provided coupons to the Coupon table
            # Existing coupons are left as is.
            files = sftp.listdir_attr()
            for f in files:
                #print(f'Inserting file {f.filename} into database')
                load_time = str(dt.now()).split(".")[0]
                
                insert_script = f"""INSERT into public.coupon_details (coupon_name, cp_date, db_uploaded_date, cp_size, file_obtained_flag, file_converted_flag)
                VALUES ('{f.filename}', '{dt.fromtimestamp(f.st_mtime)}', '{dt.strptime(load_time, '%Y-%m-%d %H:%M:%S')}', {f.st_size}, 0, 0)
                ON CONFLICT (coupon_name) DO NOTHING;
                """

                cur.execute(insert_script)
                conn.commit()
            print(f'Total files available at AirPeace SFTP Server: {len(files)}')

            # Get files with ingestion_status 0 from the database into a directory on the local desktop
            # Files are read in chunks because connection timeout occurred while trying to read each file at once

            select_script = f"""
                    SELECT * FROM public.coupon_details WHERE file_obtained_flag = 0
            """
            cur.execute(select_script)
            for record in cur.fetchall():
                cp_name = record['coupon_name']
                cp_size = record['cp_size']
                localpath = f'../coupons/{cp_name}'
                #localpath = ''
                print(f'cp_name is: {cp_name}')
                print(f'localpath is: {localpath}')

                if sftp.lexists(cp_name):
                    print(f'Getting coupon file {cp_name} with size {cp_size} into the local coupon directory')
                    chunk_size = 1024*1024
                    with open(localpath, 'ab') as local_file:
                        start_pos = local_file.tell()
                        while start_pos < cp_size:
                            print(f"Downloading from position {start_pos}...")
                            with sftp.open(cp_name, 'r') as remote_file:
                                remote_file.seek(start_pos)
                                local_file.write(remote_file.read(chunk_size))
                                start_pos = local_file.tell()  # Update position
                                print(f"Downloaded {start_pos} of {cp_size} bytes")
                    #sftp.get(remotepath=cp_name, preserve_mtime=True, localpath=localpath)
                
                update_script = f"""
                        UPDATE public.coupon_details 
                        SET file_obtained_flag = 1
                        WHERE coupon_name = '{cp_name}'
                """
                cur.execute(update_script)
                conn.commit()

    except Exception as e:
        print(f"Connection failed: {e}")

def convert_2_csv():
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    converted_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'converted_csv'))
    coupon_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'coupons'))
    csv_required_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'csv_required_format'))

    try:
        # Get files with file_obtained_flag 1 from the database which are inside ./coupons directory and convert them to CSV
        select_script = f"""
            SELECT * FROM public.coupon_details WHERE file_obtained_flag = 1 and file_converted_flag = 0
        """
        cur.execute(select_script)
        for record in cur.fetchall():
            filename = str(record['coupon_name'])
            input_filepath = f'{os.path.join(coupon_path,filename)}'
            
            pq_file = pq.ParquetFile(input_filepath)
            record_count = pq_file.metadata.num_rows

            output_filepath = filename.split(".")[0]
            df = pq_file.read().to_pandas()

            # Select columns of interest from the file read
            contacts_columns = df[['PNR_NO','PAX_GENDER','PAX_NAME','PAX_BIRTHDAY','EMAIL','MEMBER_ID','PHONE','PNR_CREATED_BY']]
            contacts_df = contacts_columns.drop_duplicates()

            flights_columns = df[['PNR_NO','ARR_PORT','CABIN_CLASS','CPN_STATUS','DEP_PORT','FARE_BASIS','FLT_NO','LEG_STATUS','TKT_NO','TKT_TYPE','PAX_TYPE','SOURCE','PAX_NAME']]
            flights_df = flights_columns.drop_duplicates()
            
            #print(f'Read file {filename} headers are {contacts_columns.head()}')

            # Write both the original file and the selected columns to CSVs
            df.to_csv(f'{os.path.join(converted_path,output_filepath)}.csv', index=False)

            contacts_df.to_csv(f'{os.path.join(csv_required_path,output_filepath)}_contacts_info.csv', index=False)
            flights_df.to_csv(f'{os.path.join(csv_required_path,output_filepath)}_flights_info.csv', index=False)

            # Split each file to sub-parts each sub-part having a maximum of 25,000 rows
            split_csv_by_rows(input_csv=f'{os.path.join(csv_required_path,output_filepath)}_contacts_info.csv', rows_per_file=25000, file_name=f'{output_filepath}_contact')
            split_csv_by_rows(input_csv=f'{os.path.join(csv_required_path,output_filepath)}_flights_info.csv', rows_per_file=25000, file_name=f'{output_filepath}_flight')

            # Upload each day's sub-file parts to Zoho CRM
            #file_ids = upload_file_to_zoho(file_name=output_filepath, access_token=access_token)

            # Create Bulk Write Jobs based on the UPLOAD file IDs above
            #job_ids = create_bulk_write_jobs(file_ids=file_ids, access_token=access_token)
            #print(f'Job IDs are: {job_ids}')

            # Check Job Status
            #check_job_status(job_ids, access_token=access_token)
            print('About to go inside function update_flight_records')
            #update_flight_records(access_token=access_token)

            print('Successfully exited function update_flight_records')

            print(f'File {record['coupon_name']} has {record_count} records while its converted CSV has {df.shape[0]} records.')

            insert_script = f"""INSERT INTO public.coupon_conversion_details(
        original_coupon_name, converted_coupon_name, parquet_record_count, csv_record_count)
        VALUES ('{filename}', '{output_filepath}.csv', {record_count}, {df.shape[0]})
                    ON CONFLICT (original_coupon_name) DO NOTHING;
                    """

            cur.execute(insert_script)
            conn.commit()

            update_script = f"""
                            UPDATE public.coupon_details 
                            SET file_converted_flag = 1
                            WHERE coupon_name = '{filename}'
                    """
            cur.execute(update_script)
            conn.commit()
    except Exception as e:
        print(f"Error occurred: File {filename} - {e}")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    get_files_from_sftp()
    convert_2_csv()
    #get_module_mapping_fields(module_api_name='Flight_Information', access_token=access_token)