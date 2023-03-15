# fetch_data_from_api

### Purpose
This file is written to automatically fetch user data with two attributes from a SaaS product called "Chatfuel". 

### Background
Manually downloading file for user data for each projects takes a lot of time and by writing a script to do so reduce at least 2 hours of work everyday. 

### Prerequisite
You would need few things first in order to execute this file smoothly. 
1. Slack webhook URL - this is to send message in case anything went wrong during the process
2. Chatfuel Dashboard API Token - this is to access the download api from Chatfuel
3. Google Sheet URL (Optional) - if you have a list of chatfuel projects you want to download from, you may use google sheet to loop through them
4. Google Credential (Optional) - only if you decided to use a google sheet then you would need a credential json to access the content in google sheet
