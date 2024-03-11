import pyodbc
import mysql.connector
import time
import threading
import queue
import datetime
from dateutil.parser import parse
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import storage
from dotenv import dotenv_values
import cv2
import requests
import os
import shutil

def script_rfid (return_value, status, firebase_DB, env, return_people_list):
    empty_people_list = False
    initial = 0
    error = 0
    while(True):
        try:
            if initial == 0:
                number_of_people = 0
                people_list = []
                # Ref for new date check
                date_old = datetime.datetime.now().date()
                # Check number of RFID record one time to assign the variable "old_record_count"
                # 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ',' + port + ';DATABASE=' + database + ';UID='+ username + ';PWD=' + password
                mydb_only_first = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + env_dict["SERVER"] + ';DATABASE=' + env_dict["DATABASE"] + ';UID='+ env_dict["USERNAME"] + ';PWD=' + env_dict["PASSWORD"])
                mycursor_only_first = mydb_only_first.cursor()
                mycursor_only_first.execute("SELECT PersonCardID, MachID, TimeInOut FROM dbo.ZFP_TimeInOut WHERE MachID = 2 OR MachID = 3 ORDER BY TimeInOut DESC")
                myresult_only_first = mycursor_only_first.fetchall()
                columns_dict_convert = [column[0] for column in mycursor_only_first.description]
                mydb_only_first.close()
                old_record_count = len(myresult_only_first)
                # initial = 1
                print("Connect RFID database successfully")

            # Check time and date now
            timestamp_now = datetime.datetime.now()
            date_now = timestamp_now.date()
            # Check number of RFID record to assign the variable "ีupdated_record_count"
            mydb = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + env_dict["SERVER"] + ';DATABASE=' + env_dict["DATABASE"] + ';UID='+ env_dict["USERNAME"] + ';PWD=' + env_dict["PASSWORD"])
            mycursor = mydb.cursor()
            mycursor.execute("SELECT PersonCardID, MachID, TimeInOut FROM dbo.ZFP_TimeInOut WHERE MachID = 2 OR MachID = 3 ORDER BY TimeInOut DESC")
            myresult = mycursor.fetchall()
            updated_record_count = len(myresult)

            # Employee personal information
            mycursor_person = mydb.cursor()
            mycursor_person.execute("SELECT PersonCardID, FnameT, LnameT FROM dbo.ZFP_Person")
            myresult_person = mycursor_person.fetchall()

            if updated_record_count > old_record_count:
                num_of_new_record = updated_record_count - old_record_count

                # Convert item in myresult to dict
                myresult_dict = []
                for row_dict_convert in myresult:
                    myresult_dict.append(dict(zip(columns_dict_convert, row_dict_convert)))

                # Convert item in myresult_person to dict
                myresult_person_dict = []
                columns_person_dict_convert = [column[0] for column in mycursor_person.description]
                for row_dict_convert in myresult_person:
                    myresult_person_dict.append(dict(zip(columns_person_dict_convert, row_dict_convert)))

                # Check the person
                for person in myresult_dict[:num_of_new_record][::-1]:
                    # print(person["PersonCardID"])
                    # print(person["MachID"])
                    # If it is Yuanterเข้า (2)
                    if person["MachID"] == 2:
                        # Post record data to fire base
                        for person_info in myresult_person_dict:
                            # print(person_info["PersonCardID"])
                            # print(person["PersonCardID"] == int(person_info["PersonCardID"]))
                            if person["PersonCardID"] == int(person_info["PersonCardID"]):
                                firebase_db_doc_ref = firebase_DB.collection("RFID_Record").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
                                firebase_db_doc_ref.set({"PersonCardID": int(person["PersonCardID"]), 
                                                         "FnameT": person_info["FnameT"], 
                                                         "LnameT": person_info["LnameT"], 
                                                         "TimeInOut": str(datetime.datetime.now()), 
                                                         "Status" : "Check-in",
                                                         "Event": "",
                                                         "Note": ""})
                                # print("Post to firebase successful")

                        # Condition to add PersonCardID to people_list
                        if not(person["PersonCardID"] in people_list):
                            people_list.append(person["PersonCardID"])
                            number_of_people = len(people_list)

                    # If it is Yuanterออก (3)
                    elif person["MachID"] == 3:
                        for person_info in myresult_person_dict:
                            if person["PersonCardID"] == int(person_info["PersonCardID"]):
                                firebase_db_doc_ref = firebase_DB.collection("RFID_Record").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
                                firebase_db_doc_ref.set({"PersonCardID": int(person["PersonCardID"]), 
                                                         "FnameT": person_info["FnameT"], 
                                                         "LnameT": person_info["LnameT"], 
                                                         "TimeInOut": str(datetime.datetime.now()), 
                                                         "Status" : "Check-out",
                                                         "Event": "",
                                                         "Note": ""})
                                # print("Post to firebase successful")
                                
                        # Condition to add PersonCardID to people_list
                        if person["PersonCardID"] in people_list:
                            people_list.remove(person["PersonCardID"])
                            number_of_people = len(people_list)

            mydb.close()

            # Empty people list only one time at 6.00 p.m. if there are people who forget to scan out
            if (len(people_list) != 0) and (timestamp_now >= parse(str(date_now) + " 17:40:00")) and (timestamp_now < parse(str(date_now) + " 17:40:05")) and (empty_people_list == False):

                # Convert item in myresult_person to dict
                myresult_person_dict = []
                columns_person_dict_convert = [column[0] for column in mycursor_person.description]
                for row_dict_convert in myresult_person:
                    myresult_person_dict.append(dict(zip(columns_person_dict_convert, row_dict_convert)))

                for people_id in people_list:
                    # Post people who didn't scan out
                    for person_info in myresult_person_dict:
                        if people_id == int(person_info["PersonCardID"]):
                            firebase_db_doc_ref = firebase_DB.collection("RFID_Record").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
                            firebase_db_doc_ref.set({"PersonCardID": int(person_info["PersonCardID"]), 
                                                     "FnameT": person_info["FnameT"], 
                                                     "LnameT": person_info["LnameT"], 
                                                     "TimeInOut": str(datetime.datetime.now()), 
                                                     "Status" : "Abnormal",
                                                     "Event": "This person forgot to scan out when leave Yuanter",
                                                     "Note": ""})
                            # print("Post to firebase successful")
                people_list = []
                number_of_people = len(people_list)
                print("Empty list success")
                empty_people_list = True
            
            # Renew empty_people_list status when the date is changed (new day)
            elif (empty_people_list == True) and (date_now != date_old):
                empty_people_list = False

            # Bring number of people to main
            return_value.put(number_of_people)
            # Bring people list to main
            return_people_list.put(people_list)
            # Bring status success of 1 to main
            status.put(1)
            # Post RFID Status to firebase
            if initial == 0:
                firebase_db_doc_ref = firebase_DB.collection("System_Status").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
                firebase_db_doc_ref.set({"RFID": "Success"})
                initial = 1
            error = 0
            old_record_count = updated_record_count
            date_old = date_now
            # time.sleep(0.9)

        except Exception as e:
            status.put(0)
            print(str(e) + " (RFID code)\nRetrying\n")
            if error == 0:
                firebase_db_doc_ref = firebase_DB.collection("System_Status").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
                firebase_db_doc_ref.set({"RFID": f"Error: {e}"})
            error = 1
            # print("Retrying")
            initial = 0
            continue

def script_camera (return_value, status, record_status, firebase_DB, record_name, line_info, env):
    initial = 0
    error = 0
    initial_record = 0
    start_record = 0
    line = 0
    source = env["CAMERA_SOURCE"]
    cap = cv2.VideoCapture(source)
    if not cap.isOpened():
        raise Exception("Video device not open")

    fourcc = cv2.VideoWriter_fourcc('M','P','4','V') 
    # out = cv2.VideoWriter(f'{str(datetime.datetime.now())}.avi', fourcc, 40.0, (704, 576))

    while(True):
        try:  
            ret, frame = cap.read() 

            if not ret:
                print("No receiving frame and streaming ends")

            record_status_value = record_status.get(timeout=7)
            
            if record_status_value == 1 and initial_record == 0:
                Record_name = record_name.get(timeout=7)
                out = cv2.VideoWriter('Abnormal_videos/{}.mp4'.format(Record_name), fourcc, 30.0, (704, 576))
                initial_record = 1

            if record_status_value == 1:
                out.write(frame)
                if line == 0:
                    cv2.imwrite("./Abnormal_pics/abnormal_image.jpg" , frame)
                    #  Send line notify
                    requests.post(line_info[0], headers=line_info[1], files = {"imageFile": open("./Abnormal_pics/abnormal_image.jpg","rb")} , data={'message' : " "})
                    line = 1
                start_record = 1

            elif (record_status_value == 0) and (start_record == 1):
                out.release()
                # # Send abnormal video to firebase
                video_name = os.listdir("Abnormal_videos")[0]
                # blob = firebase_Bucket.blob(f"Abnormal_Videos/{video_name}")
                # blob.upload_from_filename(f"Abnormal_videos/{video_name}")
                # Remove abnormal video in the folder
                shutil.copy(f"Abnormal_videos/{video_name}", f"Video_to_Firebase/{video_name}")
                os.remove(f"Abnormal_videos/{video_name}")
                initial_record = 0
                start_record = 0
                line = 0

            mydb_only_first = mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                database="yuanter"
            )
            mycursor = mydb_only_first.cursor(dictionary=True)
            mycursor.execute("SELECT * FROM test_camera")
            myresult = mycursor.fetchall()
            people = myresult[len(myresult)-1]["people"]
            return_value.put(people)
            if initial == 0:
                firebase_db_doc_ref = firebase_DB.collection("System_Status").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
                firebase_db_doc_ref.set({"Camera": "Success"})
                print("Connect camera database successfully")
                initial = 1
            status.put(1)
            # time.sleep(0.9)

        except Exception as e:
            status.put(0)
            print(str(e) + " (Camera code)\nRetrying\n")
            if error == 0:
                firebase_db_doc_ref = firebase_DB.collection("System_Status").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
                firebase_db_doc_ref.set({"Camera": f"Error: {e}"})
                error = 1
            # print("Retrying")
            # cap.release()
            # cv2.destroyAllWindows()
            initial = 0
            continue

reset = 0
count = 0
env_dict = dotenv_values(".env")
success_main = 0
error_main = 0

# Initialize Firestore
firebase_cred = credentials.Certificate(env_dict["FIREBASE_CRED"])
firebase_app = firebase_admin.initialize_app(firebase_cred, {"storageBucket": "capstone-warehouse.appspot.com"})
firebase_db = firestore.client()
firebase_bucket = storage.bucket()

# Line Notification
line_url = "https://notify-api.line.me/api/notify"
line_token = env_dict["LINE_TOKEN"]
line_headers = {
    'Authorization': 'Bearer ' + line_token
}
line_info_list = [line_url, line_headers]

# Thread RFID
queue_num_rfid = queue.Queue(maxsize = 1)
queue_rfid_status = queue.Queue(maxsize = 1)
queue_rfid_people = queue.Queue(maxsize = 1)
t_rfid = threading.Thread(target=script_rfid, args=(queue_num_rfid, queue_rfid_status, firebase_db, env_dict, queue_rfid_people,))
t_rfid.setDaemon(True)  
t_rfid.start()

# Thread Camera
queue_num_camera = queue.Queue(maxsize = 1)
queue_camera_status = queue.Queue(maxsize = 1)
queue_camera_record = queue.Queue(maxsize = 1)
queue_camera_record_name = queue.Queue(maxsize = 1)
queue_camera_record.put(0)
t_camera = threading.Thread(target=script_camera, args=(queue_num_camera, queue_camera_status, queue_camera_record, firebase_db, queue_camera_record_name, line_info_list, env_dict))
t_camera.setDaemon(True)
t_camera.start()

while(True):
    try:
        rfid_status = queue_rfid_status.get(timeout=7)
        camera_status = queue_camera_status.get(timeout=7)

        if rfid_status == 1 and camera_status == 1:
        # if rfid_status == 1 :
            num_rfid = queue_num_rfid.get(timeout=7)
            num_camera = queue_num_camera.get(timeout=7)
            rfid_people = queue_rfid_people.get(timeout=7)
            print(str(num_rfid) + " " + str(num_camera))

            # Get current timestamp
            timestamp_now = datetime.datetime.now()
            date_now = str(timestamp_now.date())
            # Create timestamp of 8 a.m.
            date_now_8am = parse(date_now + " 8:00:00")
            # Create timestamp of 6 p.m.
            date_now_6pm = parse(date_now + " 18:00:06")

            # Condition abnormal in working time (after 8 a.m. and before 6 p.m.)
            if (num_rfid < num_camera) and (date_now_8am <= timestamp_now) and (timestamp_now <= date_now_6pm):
                count += 1
                reset = 0
                # Give name of video record
                abnormal_time = datetime.datetime.now()
                name_abnormal_time = abnormal_time.strftime("%Y-%m-%d %H.%M.%S.%f")
                # Tell camera to record
                queue_camera_record.put(1)
                queue_camera_record_name.put(name_abnormal_time)
                
                if count == 20:

                    # Employee personal information
                    mydb = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + env_dict["SERVER"] + ';DATABASE=' + env_dict["DATABASE"] + ';UID='+ env_dict["USERNAME"] + ';PWD=' + env_dict["PASSWORD"])
                    mycursor_person = mydb.cursor()
                    mycursor_person.execute("SELECT PersonCardID, FnameT, LnameT FROM dbo.ZFP_Person")
                    myresult_person = mycursor_person.fetchall()

                    # Convert item in myresult_person to dict
                    myresult_person_dict = []
                    columns_person_dict_convert = [column[0] for column in mycursor_person.description]
                    for row_dict_convert in myresult_person:
                        myresult_person_dict.append(dict(zip(columns_person_dict_convert, row_dict_convert)))

                    mydb.close()

                    if len(rfid_people) != 0: 
                        for person_info in myresult_person_dict:
                            if rfid_people[-1] == int(person_info["PersonCardID"]):
                                # Post request to firebase Name: last people, Status: abnormal, Note: Get in without permission
                                firebase_db_doc_ref = firebase_db.collection("RFID_Record").document(name_abnormal_time)
                                firebase_db_doc_ref.set({"PersonCardID": int(person_info["PersonCardID"]), 
                                                        "FnameT": person_info["FnameT"], 
                                                        "LnameT": person_info["LnameT"], 
                                                        "TimeInOut": str(datetime.datetime.now()), 
                                                        "Status" : "Abnormal",
                                                        "Event": f"Someone gets in without permission\nCamera detected ({str(num_camera)}) > RFID detected ({str(num_rfid)})",
                                                        "Note": ""})
                                
                                #  Send line notify
                                requests.post(line_url, headers=line_headers, data = {"message": f'Abnormal alert!\nวันที่: {str(datetime.datetime.now().date())}\n เวลา: {datetime.datetime.now().strftime("%H:%M:%S")}'})
                    
                    elif len(rfid_people) == 0:
                        # Post request to firebase Name: Unknown, Status: abnormal, Note: Get in without permission
                        firebase_db_doc_ref = firebase_db.collection("RFID_Record").document(name_abnormal_time)
                        firebase_db_doc_ref.set({"PersonCardID": "Unknown", 
                                                "FnameT": "Unknown", 
                                                "LnameT": "", 
                                                "TimeInOut": str(datetime.datetime.now()), 
                                                "Status" : "Abnormal",
                                                "Event": f"Someone gets in without permission\nCamera detected ({str(num_camera)}) > RFID detected ({str(num_rfid)})",
                                                "Note": ""})
                        
                        #  Send line notify
                        requests.post(line_url, headers=line_headers, data = {"message": f'Abnormal alert!\nวันที่: {str(datetime.datetime.now().date())}\n เวลา: {datetime.datetime.now().strftime("%H:%M:%S")}'})

            elif (num_rfid >= num_camera) and (date_now_8am <= timestamp_now) and (timestamp_now <= date_now_6pm):
                reset += 1
                # Tell camera to stop recording
                queue_camera_record.put(0)
                
                if reset == 20:
                    count = 0
                    reset = 0

            # Condition abnormal out of working time (before 8 a.m. and after 5 p.m.)
            if ((num_rfid > 0) or (num_camera > 0)) and ((date_now_8am > timestamp_now) or (timestamp_now > date_now_6pm) or (timestamp_now.strftime("%a") == "Sat") or (timestamp_now.strftime("%a") == "Sun")):
                count += 1
                reset = 0
                # Give name of video record
                abnormal_time = datetime.datetime.now()
                name_abnormal_time = abnormal_time.strftime("%Y-%m-%d %H.%M.%S.%f")
                # Tell camera to record
                queue_camera_record.put(1)
                
                if count == 20:

                    # Employee personal information
                    mydb = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + env_dict["SERVER"] + ';DATABASE=' + env_dict["DATABASE"] + ';UID='+ env_dict["USERNAME"] + ';PWD=' + env_dict["PASSWORD"])
                    mycursor_person = mydb.cursor()
                    mycursor_person.execute("SELECT PersonCardID, FnameT, LnameT FROM dbo.ZFP_Person")
                    myresult_person = mycursor_person.fetchall()

                    # Convert item in myresult_person to dict
                    myresult_person_dict = []
                    columns_person_dict_convert = [column[0] for column in mycursor_person.description]
                    for row_dict_convert in myresult_person:
                        myresult_person_dict.append(dict(zip(columns_person_dict_convert, row_dict_convert)))

                    mydb.close()

                    if len(rfid_people) != 0: 
                        for person_info in myresult_person_dict:
                            if rfid_people[-1] == int(person_info["PersonCardID"]):
                                # Post request to firebase Name: last people, Status: abnormal, Note: Get in after work time
                                firebase_db_doc_ref = firebase_db.collection("RFID_Record").document(name_abnormal_time)
                                firebase_db_doc_ref.set({"PersonCardID": int(person_info["PersonCardID"]), 
                                                        "FnameT": person_info["FnameT"], 
                                                        "LnameT": person_info["LnameT"], 
                                                        "TimeInOut": str(datetime.datetime.now()), 
                                                        "Status" : "Abnormal",
                                                        "Event": f"Someone gets in outside of work hours\nCamera detected ({str(num_camera)}), RFID detected ({str(num_rfid)})",
                                                        "Note": ""})
                                
                                #  Send line notify
                                requests.post(line_url, headers=line_headers, data = {"message": f'Abnormal alert!\nวันที่: {str(datetime.datetime.now().date())}\n เวลา: {datetime.datetime.now().strftime("%H:%M:%S")}'})
                            
                    elif len(rfid_people) == 0: 
                        # Post request to firebase Name: Unknown, Status: abnormal, Note: Get in after work time
                        firebase_db_doc_ref = firebase_db.collection("RFID_Record").document(name_abnormal_time)
                        firebase_db_doc_ref.set({"PersonCardID": "Unknown", 
                                                "FnameT": "Unknown", 
                                                "LnameT": "", 
                                                "TimeInOut": str(datetime.datetime.now()), 
                                                "Status" : "Abnormal",
                                                "Event": f"Someone gets in outside of work hours\nCamera detected ({str(num_camera)}), RFID detected ({str(num_rfid)})",
                                                "Note": ""})
                        
                        #  Send line notify
                        requests.post(line_url, headers=line_headers, data = {"message": f'Abnormal alert!\nวันที่: {str(datetime.datetime.now().date())}\n เวลา: {datetime.datetime.now().strftime("%H:%M:%S")}'})

            elif (num_rfid == 0) and (num_camera == 0) and ((date_now_8am > timestamp_now) or (timestamp_now > date_now_6pm) or (timestamp_now.strftime("%a") == "Sat") or (timestamp_now.strftime("%a") == "Sun")):
                reset += 1
                # Tell camera to stop recording
                queue_camera_record.put(0)

                if reset == 20:
                    count = 0
                    reset = 0
            
            # Post main system Status to firebase
            if success_main == 0:
                firebase_db_doc_ref = firebase_db.collection("System_Status").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
                firebase_db_doc_ref.set({"Main_system": "Success"})
                success_main = 1
                error_main = 0

        else:
            # Post main system Status to firebase
            if error_main == 0:
                firebase_db_doc_ref = firebase_db.collection("System_Status").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
                firebase_db_doc_ref.set({"Main_system": "RFID or Camera code are not ready"})
                success_main = 0
                error_main = 1

            if rfid_status == 0 and camera_status == 0:
                print("RFID and Camera code are not ready (Main)\n")
            elif rfid_status == 0:
                print("RFID code is not ready (Main)\n")
            elif camera_status == 0:
                print("Camera code is not ready (Main)\n")
        # time.sleep(1)

    except KeyboardInterrupt:
        print("Loop end by keyboard interupt (Main)")
        break

    except queue.Empty:
        # Post main system Status to firebase
        if error_main == 0:
            firebase_db_doc_ref = firebase_db.collection("System_Status").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
            firebase_db_doc_ref.set({"Main_system": f"Error: Queue is empty"})
            success_main = 0
            error_main = 1
        print("Queue is empty (Main code) \nretry\n")
        continue

    except Exception as e:
        # Post main system Status to firebase
        if error_main == 0:
            firebase_db_doc_ref = firebase_db.collection("System_Status").document(datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f"))
            firebase_db_doc_ref.set({"Main_system": f"Error: {e}"})
            success_main = 0
            error_main = 1
        print(str(e) + " (Main code)\n")
        continue
    
