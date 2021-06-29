from datetime import datetime, timedelta
import boto3
import json
from pybluemo.aws_srp import AWSSRP
from python_graphql_client import GraphqlClient
import os
import threading
import time


class GetOrCreateDataSourceInput(object):
    DEVICE_ID_COHORT = "1e340cc4-ee98-4617-a96f-1ea6ecc19e79"

    def __init__(self, object_key, sort_key, cohort_id=DEVICE_ID_COHORT):
        self.params = {
            "cohortId": cohort_id,
            "objectKey": object_key,
            "sortKey": sort_key
        }

    def json(self):
        return self.params


class CreateDataStreamsInput(object):
    def __init__(self, data_label, channel, frequency, unit, values):
        self.params = {
            "dataLabel": data_label,
            "channel": channel,
            "frequency": frequency,
            "unit": unit,
            "values": values
        }

    def __len__(self):
        return len(self.params["values"])

    def timedelta(self):
        return timedelta(seconds=(1 / self.params["frequency"]) * len(self.params["values"]))

    def json(self):
        return self.params


class CreateDataStreamInput(object):
    def __init__(self, user_id, source_id, start, end, data_label, channel, frequency, unit, values, collaborators=[]):
        if end is None:
            self.end = datetime.utcnow().isoformat() + "Z"
        else:
            self.end = end.isoformat() + "Z"
        if start is None:
            self.start = (datetime.utcnow() - self.timedelta()).isoformat() + "Z"
        else:
            self.start = start.isoformat() + "Z"
        self.params = {
            "userId": user_id,
            "sourceId": source_id,
            "start": self.start,
            "end": self.end,
            "dataLabel": data_label,
            "channel": channel,
            "frequency": frequency,
            "unit": unit,
            "values": values
        }

    def __len__(self):
        return len(self.params["values"])

    def timedelta(self):
        return timedelta(seconds=(1 / self.params["frequency"]) * len(self))

    def json(self):
        return self.params


class SaveStreamsInput(object):
    def __init__(self, user_id, device_id, streams, start=None, end=None):
        self.streams = streams
        if end is None:
            self.end = datetime.utcnow().isoformat() + "Z"
        else:
            self.end = end.isoformat() + "Z"
        if start is None:
            self.start = (datetime.utcnow() - streams[0].timedelta()).isoformat() + "Z"
        else:
            self.start = start.isoformat() + "Z"
        self.user_id = user_id
        self.device_id = device_id


class CreateDataEventInput(object):
    def __init__(self, user_id, source_id, label, date_time=None, value=None, unit=None, channel=None):
        self.params = {
            "userId": user_id,
            "sourceId": source_id,
            "label": label
        }
        if date_time is not None:
            self.params['dateTime'] = date_time
        else:
            self.params['dateTime'] = datetime.utcnow().isoformat() + "Z"
        if value is not None:
            self.params['value'] = value
        if unit is not None:
            self.params['unit'] = unit
        if channel is not None:
            self.params['channel'] = channel

    def json(self):
        return self.params


class SensorDbClient(object):
    def __init__(self, cfg_json_filename, auth_token_filename="auth_token.json"):
        self.token = None
        with open(cfg_json_filename, "r") as cfg_fp:
            cfg_json = json.load(cfg_fp)
            username = cfg_json["Username"]
            password = cfg_json["Password"]
            pool_id = cfg_json["CognitoPoolId"]
            pool_region = cfg_json["CognitoPoolRegion"]
            client_id = cfg_json["CognitoClientId"]
            sensordb_api_url = cfg_json["SensorDbApiUrl"]
            self.bucket = cfg_json["S3DownloadsBucket"]
            if "CognitoClientSecret" in cfg_json:
                client_secret = cfg_json["CognitoClientSecret"]
            else:
                client_secret = None
        if os.path.isfile(auth_token_filename):
            with open(auth_token_filename, "r") as token_fp:
                result = json.load(token_fp)
                expires_in = timedelta(seconds=int(result['AuthenticationResult']['ExpiresIn']))
                header_date = datetime.strptime(result['ResponseMetadata']['HTTPHeaders']['date'], '%a, %d %b %Y %H:%M:%S GMT')
                print(header_date + expires_in, datetime.utcnow())
                if header_date + expires_in > datetime.utcnow():
                    self.token = result['AuthenticationResult']['AccessToken']
        if self.token is None:
            cognito_client = boto3.client('cognito-idp', pool_region)
            self.awssrp = AWSSRP(username, password, pool_id, client_id,
                                 client=cognito_client, client_secret=client_secret)
            result = self.awssrp.authenticate_user(client=cognito_client)
            with open(auth_token_filename, "w") as token_fp:
                json.dump(result, token_fp, indent=2)
            self.token = result['AuthenticationResult']['AccessToken']

        self.client = GraphqlClient(sensordb_api_url, headers={'authorization': self.token})

    def list_cohorts(self, query_filter=None, limit=None, next_token=None):
        query = """
query ListCohorts ($filter: ModelCohortFilterInput, $limit: Int, $nextToken: String) {
  listCohorts(filter: $filter, limit: $limit, nextToken: $nextToken) {
    items {
      id
      category
      collaborators
      createdAt
      label
      owner
      updatedAt
    }
    nextToken
  }
}"""
        variables = {}
        if query_filter is not None:
            variables['filter'] = query_filter
        if limit is not None:
            variables['limit'] = limit
        if next_token is not None:
            variables['nextToken'] = next_token
        result = self.client.execute(query=query, variables=variables)
        return result['data']['listCohorts']

    def list_sources(self, query_filter=None, limit=None, next_token=None):
        query = """
query ListSources($filter: ModelDataSourceFilterInput, $limit: Int, $nextToken: String) {
  listDataSources(filter: $filter, limit: $limit, nextToken: $nextToken) {
    items {
      id
      objectKey
      sortKey
      cohortId
    }
    nextToken
  }
}"""
        variables = {}
        if query_filter is not None:
            variables['filter'] = query_filter
        if limit is not None:
            variables['limit'] = limit
        if next_token is not None:
            variables['nextToken'] = next_token
        result = self.client.execute(query=query, variables=variables)
        return result['data']['listDataSources']

    def get_or_create_data_sources(self, mutation_input):
        mutation = """
mutation GetOrCreateDataSources($input: [GetOrCreateDataSourceInput]!) {
  getOrCreateDataSources(input: $input) {
    id
    metadata
    objectKey
    owner
    sortKey
    updatedAt
    cohortId
    collaborators
    createdAt
  }
}"""
        variables = {'input': [i.json() for i in mutation_input]}
        result = self.client.execute(query=mutation, variables=variables)
        return result['data']['getOrCreateDataSources']

    def save_data_stream(self, save_streams_input, linked_sources=None):
        mutation = """
mutation SaveDataStreams($userId: ID!, $deviceId: ID!, $start: AWSDateTime!, $end: AWSDateTime!,
                         $dataStreams: [CreateDataStreamsInput]!, $linkedSources: [ID]) {
  saveDataStreams(userId: $userId, deviceId: $deviceId, start: $start, end: $end, 
                  dataStreams: $dataStreams, linkedSources: $linkedSources) {
    channel
    collaborators
    createdAt
    dataLabel
    end
    frequency
    id
    owner
    sourceId
    start
    unit
    updatedAt
    userId
    values
  }
}"""
        variables = {
            'userId': save_streams_input.user_id,
            'deviceId': save_streams_input.device_id,
            'start': save_streams_input.start,
            'end': save_streams_input.end,
            'dataStreams': [i.json() for i in save_streams_input.streams]
        }
        if linked_sources is not None:
            variables['linkedSources'] = linked_sources
        else:
            variables['linkedSources'] = []
        result = self.client.execute(query=mutation, variables=variables)
        return result['data']['saveDataStreams']

    def create_data_stream(self, create_data_stream_input, condition=None):
        mutation = """
mutation CreateDataStream($input: CreateDataStreamInput!, $condition: ModelDataStreamConditionInput) {
    createDataStream(input: $input, condition: $condition) {
      id
      userId
      sourceId
      start
      end
      dataLabel
      channel
      frequency
      unit
    }
}
"""
        variables = {
            'input': create_data_stream_input.json()
        }
        if condition is not None:
            variables['condition'] = condition
        result = self.client.execute(query=mutation, variables=variables)
        print(result)
        return result['data']['createDataStream']

    def publish_streams(self, data_streams):
        mutation = """
mutation PublishStream($userId: ID!, $sourceId: ID!, $streamId: ID!) {
  publishStream(userId: $userId, sourceId: $sourceId, streamId: $streamId) {
    userId
    sourceId
    id
    start
    end
    dataLabel
    channel
    frequency
    unit
    values
  }
}"""
        results = []
        for stream in data_streams:
            variables = {
                "userId": stream["userId"],
                "sourceId": stream["sourceId"],
                "streamId": stream["id"]
            }
            result = self.client.execute(query=mutation, variables=variables)
            results.append(result)
        return results

    def create_data_event(self, create_data_event_input):
        mutation = """
mutation CreateDataEvent($input: CreateDataEventInput!, $condition: ModelDataEventConditionInput) {
  createDataEvent(input: $input, condition: $condition) {
    id
    userId
    sourceId
    dateTime
    label
    value
    unit
    channel
  }
}"""
        variables = {'input': create_data_event_input.json()}
        result = self.client.execute(query=mutation, variables=variables)
        return result['data']['createDataEvent']

    def publish_event(self, user_id, source_id, event_id):
        mutation = """
mutation PublishEvent($userId: ID!, $sourceId: ID!, $eventId: ID!) {
  publishEvent(userId: $userId, sourceId: $sourceId, eventId: $eventId) {
    id
    userId
    sourceId
    dateTime
    label
    value
    unit
    channel
  }
}"""
        variables = {'userId': user_id, 'sourceId': source_id, 'eventId': event_id}
        result = self.client.execute(query=mutation, variables=variables)
        return result

    def create_yasp_firmware(self, dfu_s3_key, hex_s3_key, yasp_json_string, fw_version, sw_version, hw_version):
        mutation = """
          mutation CreateYaspFirmware(
    $input: CreateYaspFirmwareInput!
    $condition: ModelYaspFirmwareConditionInput
  ) {
    createYaspFirmware(input: $input, condition: $condition) {
      id
      fwVersion
      swVersion
      hwVersion
      manufacturing
      DFUPackage
      yaspSpecification
      _version
      _deleted
      _lastChangedAt
      createdAt
      updatedAt
      owner
    }
  }"""
        variables = {
            "input": {
            "fwVersion": fw_version,
            "swVersion": sw_version,
            "hwVersion": hw_version,
            "manufacturing": hex_s3_key,
            "DFUPackage": dfu_s3_key,
            "yaspSpecification": yasp_json_string
            }
        }
        result = self.client.execute(query=mutation, variables=variables)
        if result['data'] is None:
            raise RuntimeError(result['errors'])

    def upload_fw_files(self, dfu_filename, hex_filename, dfu_s3_key, hex_s3_key):
        s3 = boto3.client("s3")
        s3.upload_file(dfu_filename, self.bucket, dfu_s3_key)
        s3.upload_file(hex_filename, self.bucket, hex_s3_key)

    def increase_fw_version(self, new_major_ver):
        ver_dict = {
            "MAJOR_VERSION": new_major_ver
        }
        with open("temp_s3_version.json", 'w') as file:
            json.dump(ver_dict, file)
        s3 = boto3.client("s3")
        s3.upload_file("temp_s3_version.json", self.bucket, "settings/firmwareVersions.json")
        os.remove("temp_s3_version.json")

    def get_fw_version(self):
        s3 = boto3.resource("s3")
        ver = json.loads(s3.Object(bucket_name=self.bucket, key="settings/firmwareVersions.json").get()['Body'].read())
        return ver


class CloudLogger(object):
    def __init__(self, user, user_cohort_id, device, sensordb_client, timeout=1):
        source_list = [GetOrCreateDataSourceInput(user, "User", user_cohort_id),
            GetOrCreateDataSourceInput(device, "Bluemo")]
        result = sensordb_client.get_or_create_data_sources(source_list)
        for source in result:
            if source['objectKey'] == user:
                self.user_id = source['id']
            elif source['objectKey'] == device:
                self.device_id = source['id']
            else:
                raise Exception("Get Source IDs error.")
        self.sensordb_client = sensordb_client
        self._continue = True
        self._timeout = timeout
        self._stream_list = []
        self._event_list = []
        self._semaphore = threading.Semaphore(1)
        self.t = None

    def __str__(self):
        return "CloudLogger(%s,%s)" % (self.user_id, self.device_id)

    def _run(self):
        while self._continue:
            if len(self._stream_list) > 0:
                self._semaphore.acquire()
                streams = self._stream_list
                self._stream_list = []
                self._semaphore.release()
                for stream in streams:
                    result = self.sensordb_client.create_data_stream(stream)
                    self.sensordb_client.publish_streams([result])
                    print(result)
            if len(self._event_list) > 0:
                self._semaphore.acquire()
                events = self._event_list
                self._event_list = []
                self._semaphore.release()
                for event in events:
                    result = self.sensordb_client.create_data_event(event)
                    #self.sensordb_client.publish_event(result['userId'], result['sourceId'], result['id'])
                    print(result)
            time.sleep(1)
            #print("Run...")

    def start_daemon(self):
        self._continue = True
        self.t = threading.Thread(target=self._run, args=())
        self.t.setDaemon(True)
        self.t.start()

    def stop_daemon(self):
        while len(self._stream_list) > 0 or len(self._event_list) > 0:
            time.sleep(0.1)
        self._continue = False
        self.t.join(self._timeout)

    def log_stream(self, start, end, label, channel, frequency, unit, values):
        self._semaphore.acquire()
        self._stream_list.append(CreateDataStreamInput(self.user_id, self.device_id, start, end, label, channel, frequency, unit, values))
        self._semaphore.release()

    def log_event(self, label, date_time=None, value=None, unit=None, channel=None):
        self._semaphore.acquire()
        create_data_event_input = CreateDataEventInput(self.user_id, self.device_id, label, date_time, value, unit, channel)
        self._event_list.append(create_data_event_input)
        self._semaphore.release()
