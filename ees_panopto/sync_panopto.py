import datetime
import glob
import html
import json
import os
import re
from collections import OrderedDict
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse

import pyodbc
import requests
from bs4 import BeautifulSoup

from tika import parser
from tika.tika import TikaException

from .utils import hash_id, run_tika

requests.packages.urllib3.disable_warnings()

# event target type
POWERPOINT = 1
TRANSCRIPT = 6
MACHINE_TRANSCRIPT = 8
USER_CREATED_TRANSCRIPT = 11
PRIMARY = 10

# group type
PUBLIC_GROUP = 6

query_videos = f"""
select 
    delivery.publicID,
	session.longName, 
	session.deletedByUserKey,
	session.abstract,
    sessionTimes.startTime,
    sessionTimes.endTime,
    session.publicID as sessionPublicID,
	delivery.lifeCycleState,
    delivery.hasCaptions,
    delivery.sessionID,
    [group].type as groupType
from aclGroupEntry
    inner join delivery on delivery.aclID = aclGroupEntry.aclID
    inner join session on session.id = delivery.sessionID and session.lifeCycleState = 0
    inner join sessiongroup on sessiongroup.id = session.sessiongroupid
    inner join sessionTimes on sessionTimes.sessionId = session.id
    inner join [group] on [group].id = aclGroupEntry.groupID and [group].type = 6
    inner join lkp_PlayableObjectType on lkp_PlayableObjectType.id = session.playableObjectType and lkp_PlayableObjectType.id = 0 -- 0 = video, 1 = playlist
where sessionTimes.startTime >= ? and sessionTimes.startTime <= ?
union all
select 
    delivery.publicID,
	session.longName, 
	session.deletedByUserKey,
	session.abstract,
    sessionTimes.startTime,
    sessionTimes.endTime,
    session.publicID as sessionPublicID,
	delivery.lifeCycleState,
    delivery.hasCaptions,
    delivery.sessionID,
    [group].type as groupType
from aclGroupEntry
    inner join sessionGroup on sessionGroup.aclID = aclGroupEntry.aclID
    inner join session on session.sessionGroupId = sessionGroup.id
    inner join delivery on delivery.sessioniD = session.id and session.lifeCycleState = 0
    inner join sessionTimes on sessionTimes.sessionId = session.id
    inner join [group] on [group].id = aclGroupEntry.groupID and [group].type = 6
    inner join lkp_PlayableObjectType on lkp_PlayableObjectType.id = session.playableObjectType and lkp_PlayableObjectType.id = 0 -- 0 = video, 1 = playlist
where sessionTimes.startTime >= ? and sessionTimes.startTime <= ? 
"""

query_event_targets = f"""
select
    ID as eventTargetId,
    eventTargetTypeID
from eventTarget
where
    sessionID = ?
"""

query_captions = f"""
select 
    data,
    eventTargetId,
    streamRelativeSeconds
from caption
where 
    eventTargetId = ?
order by streamRelativeSeconds
"""

query_events = f"""
select
    caption,
    eventTargetId,
    time
from event
where 
    eventTargetId = ?
order by time
"""

query_slides = f"""
select
    title,
    content,
    eventTargetId,
    absoluteSeconds
from slideEvent
where 
    eventTargetId = ?
order by absoluteSeconds
"""

thumbnail_root_dir = r'\\10.18.25.144\Web'


class SyncPanopto:
    def __init__(
        self,
        config,
        logger,
        mssql_client,
        indexing_rules,
        queue,
        leadtools_engine,
        panopto_client,
        start_time=None,
        end_time=None,
    ):
        self.logger = logger
        self.config = config
        self.mssql_client = mssql_client
        self.indexing_rules = indexing_rules
        self.panopto_sync_thread_count = config.get_value(
            "panopto_sync_thread_count")
        self.queue = queue
        self.leadtools_engine = leadtools_engine
        self.panopto_client = panopto_client

        self.host = config.get_value("panopto.host_url")
        self.thumbnail_root_url = f'{self.host}/Panopto/Content/Sessions'

        # for incremental sync
        self.start_time = start_time
        self.end_time = end_time

    def get_video_url(self, public_id):
        url = f'{self.host}//Panopto/Pages/Viewer.aspx?id={public_id}'
        return url

    def fetch_videos(self, duration):
        start_time, end_time = duration[0], duration[1]
        base_date = datetime.datetime(1600, 12, 31)

        start_date_time = datetime.datetime.strptime(
            start_time, "%Y-%m-%dT%H:%M:%SZ")
        time_difference = start_date_time - base_date
        start_time = time_difference.total_seconds()

        end_date_time = datetime.datetime.strptime(
            end_time, "%Y-%m-%dT%H:%M:%SZ")
        time_difference = end_date_time - base_date
        end_time = time_difference.total_seconds()

        docs = []

        conn = self.mssql_client.connect()
        videos = self.mssql_client.execute_query(
            conn, query_videos, (start_time, end_time, start_time, end_time))

        self.logger.info(f'Fetching videos from {start_time} to {end_time}')

        for video in videos:
            public_id = video.publicID
            session_public_id = video.sessionPublicID
            group_type = video.groupType

            doc = {}
            url = self.get_video_url(public_id)

            self.logger.info(
                f'Fetching video from {url} with public id {public_id}, session public id {session_public_id}, group type {group_type}')

            doc['category'] = self.get_category(url)

            date_time = base_date + datetime.timedelta(seconds=video.startTime)

            doc['id'] = public_id
            doc['date'] = date_time.isoformat(timespec='seconds') + 'Z'
            doc['title'] = video.longName
            doc['path'] = url
            doc['url'] = url
            doc['public_id'] = public_id
            doc['body'] = ''
            doc['_allow_permissions'] = []

            contents = []
            contents.append(video.longName)
            contents.append(video.abstract)

            event_targets = self.mssql_client.execute_query(
                conn, query_event_targets, (video.sessionID))

            for event_target in event_targets:
                event_target_id = event_target.eventTargetId
                type_id = event_target.eventTargetTypeID

                # TRANSCRIPT, MACHINE_TRANSCRIPT, USER_CREATED_TRANSCRIPT
                captions = self.mssql_client.execute_query(
                    conn, query_captions, (event_target_id))

                for caption in captions:
                    data = caption.data
                    contents.append(data)

                # PRIMARY
                events = self.mssql_client.execute_query(
                    conn, query_events, (event_target_id))

                for event in events:
                    caption = event.caption
                    contents.append(caption)

                # POWERPOINT
                slides = self.mssql_client.execute_query(
                    conn, query_slides, (event_target_id))
                for slide in slides:
                    slide_title = slide.title

                    if slide_title:
                        contents.append(slide_title)

                    slide_content = slide.content
                    if slide_content:
                        contents.append(slide_content)

            thumbnail_folder_path = thumbnail_root_dir + \
                f'/{session_public_id}/*_et/thumbs/*.jpg'
            thumbnail_paths = glob.glob(thumbnail_folder_path)
            thumbnail_paths = sorted(
                thumbnail_paths, key=lambda x: os.path.basename(x).lower())

            if thumbnail_paths:
                thumbnail_path = thumbnail_paths[0]
                relative_path = thumbnail_path.replace(
                    r'\\10.18.25.144\Web', '').replace('\\', '/')
                thumbnail_url = self.thumbnail_root_url + relative_path
                doc['thumbnail'] = thumbnail_url
            else:
                doc['thumbnail'] = ''

            # self.panopto_client.dowload_video_by_session_id(public_id)

            contents = list(
                filter(lambda item: item is not None and len(item) > 0, contents))
            html_string = '\n'.join(
                list(dict.fromkeys(contents))) + doc['body']
            soup = BeautifulSoup(html_string, 'html.parser')
            doc['body'] = soup.get_text()

            # source
            doc['source'] = 'training'

            docs.append(doc)

        conn.close()
        return docs

    def get_category(self, url):
        """Get the file type hierarchy of the given filename."""
        ext = os.path.splitext(url)[-1].lower()
        # TODO: Post

        # image
        if ext in ['.jpg', '.jpeg', '.png', '.bmp', '.gif']:
            return ['image', ext[1:]]
        # video
        elif ext in ['.mp4', '.avi', '.mov', '.wmv']:
            return ['video', ext[1:]]
        # office
        elif ext in ['.xls', '.xlsx', '.xlsm', '.xlsb']:
            return ['xlsx']
        elif ext in ['.doc', '.docx', '.docm']:
            return ['docx']
        elif ext in ['.ppt', '.pptx', '.pptm']:
            return ['pptx']
        # pdf, text, rtf, csv
        elif ext in ['.pdf', '.txt', '.rtf', '.csv']:
            return [ext[1:]]
        else:
            return [ext[1:]]

    def perform_sync(self, date_ranges):
        documents_to_index = []
        ids_storage = {}

        try:
            fetched_documents = self.fetch_videos(date_ranges)

            self.queue.append_to_queue(fetched_documents)
            documents_to_index.extend(fetched_documents)
        except Exception as exception:
            self.logger.error(
                f"Error while fetching videos. Error: {exception}")

        for doc in documents_to_index:
            ids_storage.update({doc["id"]: doc["url"]})

        return ids_storage
