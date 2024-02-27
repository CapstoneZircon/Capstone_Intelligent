import os
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
from dotenv import dotenv_values

env_dict = dotenv_values(".env")
firebase_cred = credentials.Certificate(env_dict["FIREBASE_CRED"])
firebase_app = firebase_admin.initialize_app(firebase_cred, {"storageBucket": "capstone-warehouse.appspot.com"})
firebase_bucket = storage.bucket()
while(True):
    try:
        if (not os.listdir("Video_to_Firebase")) == False:
            # Send abnormal video to firebase
            video_name = os.listdir("Video_to_Firebase")[0]
            blob = firebase_bucket.blob(f"Abnormal_Videos/{video_name}")
            blob.upload_from_filename(f"Video_to_Firebase/{video_name}")
            os.remove(f"Video_to_Firebase/{video_name}")
            print("Send success")

    except KeyboardInterrupt:
        print("Loop end by keyboard interupt (Main)")
        break
    
    except Exception as e:
        print(str(e) + " (video_to_firebase)")
        continue