import pyarrow.parquet as pq
import os

from utils import split_csv_by_rows
# Gets the full path to the directory this "main.py" file is contained in
# Because it is reading from a virtual environment
os_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(os_path)

print(f'OS Path: {os_path}')


def convert_2_csv():
    files = os.listdir("../Sample_parquet")
    print(f'Files: {files}')
    converted_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'converted_csv'))
    coupon_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Sample_parquet'))
    csv_required_path = os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'csv_required_format'))

    try:
        for file in files:
            print(file)
            filename = file
            input_filepath = f'{os.path.join(coupon_path, filename)}'

            pq_file = pq.ParquetFile(input_filepath)
            record_count = pq_file.metadata.num_rows

            output_filepath = filename.split(".")[0]
            df = pq_file.read().to_pandas()

            # Select columns of interest from the file read
            contacts_columns = df[
                ['PNR_NO', 'PAX_GENDER', 'PAX_NAME', 'PAX_BIRTHDAY', 'EMAIL', 'MEMBER_ID', 'PHONE', 'PNR_CREATED_BY']]
            contacts_df = contacts_columns.drop_duplicates()

            flights_columns = df[
                ['PNR_NO', 'ARR_PORT', 'CABIN_CLASS', 'CPN_STATUS', 'DEP_PORT', 'FARE_BASIS', 'FLT_NO', 'LEG_STATUS',
                 'TKT_NO', 'TKT_TYPE', 'PAX_TYPE', 'SOURCE', 'PAX_NAME']]
            flights_df = flights_columns.drop_duplicates()

            # print(f'Read file {filename} headers are {contacts_columns.head()}')

            # Write both the original file and the selected columns to CSVs
            df.to_csv(f'{os.path.join(converted_path, output_filepath)}.csv', index=False)

            contacts_df.to_csv(f'{os.path.join(csv_required_path, output_filepath)}_contacts_info.csv', index=False)
            flights_df.to_csv(f'{os.path.join(csv_required_path, output_filepath)}_flights_info.csv', index=False)

            # Split each file to sub-parts each sub-part having a maximum of 25,000 rows
            split_csv_by_rows(input_csv=f'{os.path.join(csv_required_path, output_filepath)}_contacts_info.csv',
                              rows_per_file=25000, file_name=f'{output_filepath}_contact')
            split_csv_by_rows(input_csv=f'{os.path.join(csv_required_path, output_filepath)}_flights_info.csv',
                              rows_per_file=25000, file_name=f'{output_filepath}_flight')

            # Upload each day's sub-file parts to Zoho CRM
            # file_ids = upload_file_to_zoho(file_name=output_filepath, access_token=access_token)

            # Create Bulk Write Jobs based on the UPLOAD file IDs above
            # job_ids = create_bulk_write_jobs(file_ids=file_ids, access_token=access_token)
            # print(f'Job IDs are: {job_ids}')

            # Check Job Status
            # check_job_status(job_ids, access_token=access_token)
            print('About to go inside function update_flight_records')
            # update_flight_records(access_token=access_token)

            print('Successfully exited function update_flight_records')

            print(
                f'File {filename} has {record_count} records while its converted CSV has {df.shape[0]} records.')

        #     insert_script = f"""INSERT INTO public.coupon_conversion_details(
        # original_coupon_name, converted_coupon_name, parquet_record_count, csv_record_count)
        # VALUES ('{filename}', '{output_filepath}.csv', {record_count}, {df.shape[0]})
        #             ON CONFLICT (original_coupon_name) DO NOTHING;
        #             """

            # cur.execute(insert_script)
            # conn.commit()

            # update_script = f"""
            #                 UPDATE public.coupon_details
            #                 SET file_converted_flag = 1
            #                 WHERE coupon_name = '{filename}'
            #         """
            # cur.execute(update_script)
            # conn.commit()
    except Exception as e:
        print(f"Error occurred: File {filename} - {e}")
    finally:
        print("Finally")

if __name__ == '__main__':
    convert_2_csv()
    # get_module_mapping_fields(module_api_name='Flight_Information', access_token=access_token)