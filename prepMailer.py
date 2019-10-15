import smtplib, ssl, io, json, requests, pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
#smtp details.  for easiest setup, i used a gmail server
port = <smtp port>
smtp_server = <smtp server address>
sender_email = <email address to distribute via>
password = <smtp password>

#tableau server credentials.  these are only needed if you're going to be using the rest api to get email addresses
ts_username = <enter TS username here>
ts_password = <enter TS password here>
ts_url = <tableau server url>
context = ssl.create_default_context()
headers = {
	'accept': 'application/json',
	'content-type': 'application/json'}
	
#this function only takes a df because Tableau Prep requires it.  the function uses the REST API to find every email addresses in the listed groups
def GroupMailer(df):
	site = df['Site'][0]
	payload = { "credentials": {"name": ts_username, "password": ts_password, "site" :{"contentUrl": site} } }
	req = requests.post(ts_url + 'api/3.5/auth/signin', json=payload, headers=headers, verify=False)
	response =json.loads(req.content)
	token = response["credentials"]["token"]
	site_id = response["credentials"]["site"]["id"]
	groupList = '[' + df['Groups'][0] + ']'
	auth_headers = {
      'accept': 'application/json',
      'content-type': 'application/json',
      'x-tableau-auth': token
    } 
	groupIDcall = requests.get(ts_url + '/api/3.5/sites/' + site_id + '/groups?filter=name:in:'+groupList, headers = auth_headers, verify=False)
	groupResponse = json.loads(groupIDcall.text)['groups']
	groupIDList = []
	for i in groupResponse['group']:
		groupIDList.append(i['id'])
	groupIDarrayString = ""
	for i in groupIDList:
		groupIDarrayString += i + ','
	groupIDarrayString = groupIDarrayString[:-1]
	userIDlist=[]
	#make API call to return user IDs from relevant groups
	for g in groupIDList:
		userIDcall = requests.get(ts_url + '/api/3.5/sites/' + site_id + '/groups/' + g + '/users', headers = auth_headers, verify=False)
		userIDcall = json.loads(userIDcall.text)['users']['user']
		for m in userIDcall:
			userIDlist.append(m['id'])
	allUsers = requests.get(ts_url + '/api/3.5/sites/' + site_id + '/users?fields=id,email', headers = auth_headers, verify=False)
	allUsers = (json.loads(allUsers.text)['users']['user'])
	userEmails = []
	#find the intersection of 'all users' and 'users in groups', then return the emails from the intersection of both
	for u in allUsers:
		if (u['id'] in userIDlist):
			if 'email' in u.keys():
				userEmails.append(u['email'])
	#deduplicate users present in multiple groups
	userEmails = list(dict.fromkeys(userEmails))
	emailer(userEmails,df)
	return df

#this function is simple.  instead of needing to use APIs to create a comma-separated list of emails, it simply gets them from the dataframe
def PersonMailer(df):
	userEmails = df['EmailList']
	emailer(userEmails,df)
	return df

#this function takes the list of emails and a dataframe and creates the email
def emailer(mailList, frame):
	attachment_filename = frame['File Name'][0]
	#using io to avoid having to write to disk
	buffer = io.StringIO(pd.DataFrame.to_csv(frame))
	#split the mail list into a usable format for the outgoing list
	receiver_email = ", ".join(mailList)
	#create and populate the message, including csv attachment
	msg = MIMEMultipart()
	msg['From'] = sender_email
	msg['To'] = receiver_email
	msg['Subject'] = attachment_filename +  ' Distribution from Tableau'
	body = 'Congrats on your report!'
	msg.attach(MIMEText(body,'plain'))
	part = MIMEBase('text','csv')
	part.set_payload((buffer).read())
	part.add_header('Content-Disposition', 'attachment', filename=attachment_filename + '.csv')
	encoders.encode_base64(part)
	msg.attach(part)
	text = msg.as_string()
	#send the email
	with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
		server.login(sender_email, password)
		server.sendmail(sender_email, mailList, text)
	#return the original unaltered df to tableau
	return frame
