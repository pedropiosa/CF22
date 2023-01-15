import base64
import requests
import time
import xml.etree.ElementTree as ET


# Function to obtain OPS auth token.
def authTokenObtain(username, password):
    # Use user credentials to generate the auth_token with the relevant encoding
    auth_token = base64.b64encode(f'{username}:{password}'.encode()).decode()

    # Post headers definition
    headers = {
        'Authorization': f'Basic {auth_token}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    # Parameters definition
    url = 'https://ops.epo.org/3.2/auth/accesstoken'
    data = 'grant_type=client_credentials'

    # Launch request
    response = requests.post(url, headers=headers, data=data)

    # If response is valid, print and return the token
    if response.status_code == 200:
        auth_token = response.json()['access_token']
        print(auth_token)
    else:
        print(f'Error: {response.status_code}')

    return auth_token


# Obtain XML files with patent information for the CPC classes selected, during 2013 and 2022 -- EP publications
# so that we can crosscheck the data with the fulll-text data files

# Carlos' credentials
username = 'EXKVW8f2VIQCtBzKqyPl0w7pnHmwBMBp'
password = 'FfwH49VPLGQbXI6A'


# Pedro's credentials
# username = 'rMcmXn7znBKgRIuGxB87IsarF9B8c0Uy'
# password = 'qdH0kshZ5LfVPyPv'

# Obtain authorization token for OPS
def obtain_xml_files(username, password):
    api_key = authTokenObtain(username, password)

    # Initialize an empty list to store the responses
    responses = []
    resposesText = []

    # Auxiliary variables to track any corrupted server responses
    iLoopOK = []
    iLoopNOK = []
    datesEndOK = []

    # Requests to OPS will iterate over time, per quarters, starting in 2013 until 2022
    for year in range(2013, 2021):
        for quarter in range(1, 5):
            if quarter == 1:
                dateInit = str(year) + '0101'
                dateEnd = str(year) + '0331'
            elif quarter == 2:
                dateInit = str(year) + '0401'
                dateEnd = str(year) + '0630'
            elif quarter == 3:
                dateInit = str(year) + '0701'
                dateEnd = str(year) + '0930'
            elif quarter == 4:
                dateInit = str(year) + '1001'
                dateEnd = str(year) + '1231'
            print(dateEnd)

            # First, we calculate how many entries there are:
            url = f'https://ops.epo.org/rest-services/published-data/search/biblio?q=cpc any "Y02W30/62 Y02W30/80 Y02W90/10 C08J11/14 B29B17/00",pd="{dateInit} {dateEnd}"&range=1-5&f=cpc.class,ti'  # loopeando
            headers = {'Authorization': f'Bearer {api_key}'}

            # Make a query and rescue the total count of entries found
            responseX = requests.get(url, headers=headers)
            responseTextX = responseX.text
            root = ET.fromstring(responseTextX)

            # Access the child elements of the root element in order to obtain the total of entries retrieved
            for child in root:
                count = child.attrib['total-result-count']
            print(count)

            # Calculate upperLimit based on the returned value. upperLimit is the number of patents to query the
            # information for, rounded to the next hundred. It's limited to 2000 on OPS, so it's being capped to 2000
            upperLimit = ((int(count) + 100) // 100) * 100
            if upperLimit > 2000:
                upperLimit = 2000
            print(upperLimit)

            # Query the EP publications belonging to the relevant CPC (see presentation). Loop over time (iteration are done per quarter from 2013 to 2022)
            for i in range(1, upperLimit, 100):
                time.sleep(5)
                url = f'https://ops.epo.org/rest-services/published-data/search/biblio?q=cpc any "Y02W30/62 Y02W30/80 Y02W90/10 C08J11/14 B29B17/00",pd="{dateInit} {dateEnd}"&range={i}-{i + 99}&f=cpc.class,ti'
                headers = {'Authorization': f'Bearer {api_key}'}

                # Query OPS to obtain the 100 following entries
                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    # print(response.text)
                    # Store the found xlm into the array responses
                    responses.append(response)

                    # Store the XML result for the (up to) 100 entries in resposesText
                    resposesText.append(response.text)

                    # For tracking and testing purposes, record that the iteration was successfully completed
                    iLoopOK.append(i)
                    datesEndOK.append(dateEnd)
                    if i == 1:
                        root = ET.fromstring(resposesText[0])

                        # For debug purposes and to track progress during execution, print how many entries are being targeted
                        for child in root:
                            print(child.attrib['total-result-count'])
                else:
                    print(f'Error: {response.status_code, response.text}')
                    iLoopNOK.append(i)


# Save the results to file_name file so that it can be further processed
def write_files(file_name: str, resposesText: str):
    with open(file_name, 'w', newline='') as f:
        f.writelines(resposesText)
