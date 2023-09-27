import urllib3,json,csv
import boto3
from datetime import datetime, timezone
from os import listdir

def lambda_handler(event, context):  
    http = urllib3.PoolManager()
    
    json_data_header=[  'active' , \
                        'vehicle_license_number' , \
                        'name' , \
                        'license_type' , \
                        'expiration_date' , \
                        'permit_license_number' , \
                        'dmv_license_plate_number' , \
                        'vehicle_vin_number' , \
                        'wheelchair_accessible' , \
                        'certification_date' , \
                        'hack_up_date' , \
                        'vehicle_year' , \
                        'base_number' , \
                        'base_name' , \
                        'base_type' , \
                        'veh' , \
                        'base_telephone_number' , \
                        'website' , \
                        'base_address' , \
                        'reason' , \
                        'order_date' , \
                        'last_date_updated' , \
                        'last_time_updated' , ]


    fhv_urlresponse = {"active": None, \
            "vehicle_license_number": None, \
            "name": None, \
            "license_type": None, \
            "expiration_date": None, \
            "permit_license_number": None, \
            "dmv_license_plate_number": None, \
            "vehicle_vin_number": None, \
            "wheelchair_accessible": None, \
            "certification_date": None, \
            "hack_up_date": None, \
            "vehicle_year": None, \
            "base_number": None, \
            "base_name": None, \
            "base_type": None, \
            "veh": None, \
            "base_telephone_number": None, \
            "website": None, \
            "base_address": None, \
            "reason": None, \
            "order_date": None, \
            "last_date_updated": None, \
            "last_time_updated": None      
        }

    fhv_dataurl="https://data.cityofnewyork.us/resource/8wbx-tsch.json"

    try:
        fhv_urlresponse = http.request( "GET", fhv_dataurl,  retries=urllib3.util.Retry(3))  
    except KeyError as e:
        print(f"Wrong format url {fhv_dataurl}", e)
    except urllib3.exceptions.MaxRetryError as e:
        print(f"API unavailable at {fhv_dataurl}", e) 

    

    file_loc ="/tmp/"
    file_name = "fhv_datafile.csv"  
    #file_name = "C:\\Users\\Naga\\Downloads\\BezosAcademy\\ForHireVehicles\\FetchData\\fhv_data.csv"
    data = json.loads(fhv_urlresponse.data.decode("utf8")) 

    with open(file_loc+file_name, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=json_data_header)
        writer.writeheader()
        writer.writerows(data)


    s3_client = boto3.client("s3") 
    S3_BUCKET = "mybucket-ns"      
    file_timestamp = (
            dt_now.strftime("%Y-%m-%d")
            + "/"
            + dt_now.strftime("%H")
            + "/"
            + dt_now.strftime("%M")
            + "/"
        )
    s3_client.upload_file(file_loc+file_name, S3_BUCKET, file_timestamp+file_name) 
    