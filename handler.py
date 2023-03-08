from boto3 import client as boto3_client
import face_recognition
import urllib.parse
import os
import pickle
import boto3
from boto3.dynamodb.conditions import Key
import csv
import json
import io


print("INSIDE HANDLER FILE")
input_bucket = "cse-546-p2-input"
output_bucket = "cse-546-p2-output"
aws_access_key_id = "AKIARTA6RNMBKHPHJ6XZ"
aws_secret_access_key = "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"
region_name = 'us-east-1'


s3_client = boto3.client('s3', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
s3 = boto3.resource(
    service_name='s3',
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
    )

TABLE_NAME = "students"

dynamodb = boto3.resource('dynamodb', region_name="us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
table = dynamodb.Table(TABLE_NAME)

# Function to read the 'encoding' file
def open_encoding(filename):
	
	file = open(filename, "rb")
	data = pickle.load(file)
	print(data)
	file.close()
	return data
	
def search_dynamo(key):
	print("HERE inside search_dynamo")
	data = []
	response = table.scan(FilterExpression=Key('name').eq(key))
	print("The query returned the following items:")
	for item in response['Items']:
		print(item)
		data.append(item)
	return data

def push_to_s3(data, key):
	print("HERE in push_to_s3")
	file_name = key.split(".")[0]+".csv"
	header = ["year", "major", "name"]
	print("HERE in push_to_s3 created headers")
	try: 
		with open("/tmp/"+file_name, 'w', encoding='utf-8-sig') as f:
			writer = csv.DictWriter(f, header)
			writer.writeheader()
			writer.writerow({'year': data[0]['year'], 'major': data[0]['major'], 'name': data[0]['name']})
		print("HERE in push_to_s3 created file")
		with open("/tmp/"+file_name, "rb") as f:
    	# s3.upload_fileobj(f, "BUCKET_NAME", "OBJECT_NAME")
			print("HERE in push_to_s3 uploadfile")
			s3_client.upload_fileobj(f, output_bucket, file_name)
	except Exception as e:
		print(e)
		print('Error in creating and uploading csv to bucket {}.'.format(output_bucket))
		raise e

def face_recognition_handler(event, context):
	print("HERE in face_recognition_handler")
	print(event)
	bucketName = event['Records'][0]['s3']['bucket']['name']
	key =  urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
	print("bucketName: " + bucketName + " \n key: " + key)
	print("HERE 1 ")

	try:
		print("HERE inside TRY")
		s3_client.download_file(bucketName, key, "/tmp/"+key)
		return recognize_face_from_video(key)
	except Exception as e:
		print(e)
		print('Error parsing object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucketName))
		raise e
	
def recognize_face_from_video(key):
	print("HERE inside recognize_face_from_video()")

	data = open_encoding('encoding')

	path = "/tmp/"
 
	os.system("ffmpeg -i " + (path+key) + " -r 1 " + str(path) + "image-%3d.jpeg")
	allfiles = os.listdir(path)
	files = [ fname for fname in allfiles if fname.endswith('.jpeg')]

	for f  in files:
		print("HERE inside recognize_face_from_video() File Loop")
		found_image = face_recognition.load_image_file(path+f)
		image_encoding = face_recognition.face_encodings(found_image)

		if len(image_encoding)==0:
			continue

		image_encoding = image_encoding[0]

		for i in range(len(data['encoding'])):
			print("HERE inside recognize_face_from_video() encoding Loop")
			known_ = data['encoding'][i]
			results = face_recognition.compare_faces([image_encoding], known_)

			if results[0] : 
				ans = data['name'][i]
				break

		if len(results)>0 and results[0]:
			print("HERE inside recognize_face_from_video() calling dynamoDb")
			searched_data = search_dynamo(ans)
			print("Here after searching dynamo")
			push_to_s3(searched_data, key)
			return
		
	return "no_face_found"