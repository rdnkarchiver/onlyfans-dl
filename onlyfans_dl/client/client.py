import contextlib
from datetime import datetime
import functools
import hashlib
import logging
from operator import itemgetter
import pathlib
import re
import secrets
import sqlite3
import time
import urllib.parse

import msgspec
import requests

from onlyfans_dl.client.structs import (
    Chats,
    HeaderRules,
    HighlightCategory,
    Highlight,
    NormalizedMedia,
    Messages,
    Post,
    Story,
    User,
    normalize_archived_post_media,
    normalize_message_media,
    normalize_post_media,
    normalize_story_media,
)


LOGGER = logging.getLogger(__name__)

def sanitize_filename(file_name: str) -> str:
    '''
    Clean the post text.
    '''
    file_name = re.sub('\s', '_', file_name)
    file_name = re.sub('[^\w\d_.-]', '', file_name)
    file_name = re.sub('_+', '_', file_name)
    file_name = re.sub('\.+', '.', file_name)
    file_name = re.sub('(_\.|\._)', '.', file_name)
    return file_name.lower()


def get_header_rules(url: str = 'https://raw.githubusercontent.com/DATAHOARDERS/dynamic-rules/main/onlyfans.json') -> HeaderRules:
    response = requests.get(url)
    response.raise_for_status()
    header_rules: HeaderRules = msgspec.json.decode(response.content, type=HeaderRules)
    return header_rules


class ScrapingException(Exception):
    pass


class OnlyFansScraper:
    def __init__(self,
        name: str = __qualname__,  # type: ignore[name-defined]
        *,
        session: requests.Session,
        request_timeout: int = 10,
        header_rules: HeaderRules | None = None,
        cookie: str = '',
        user_agent: str = '',
        x_bc: str,
        download_root: str = 'downloads',
        download_template: str,
        skip_temporary: bool = False,
    ):
        self.session = session
        self.request_timeout = request_timeout
        self.name = name
        self.header_rules = header_rules
        self.cookie = cookie
        self.user_agent = user_agent
        self.x_bc = x_bc

        self.download_root = pathlib.Path(download_root)
        self.download_template = download_template
        self.skip_temporary = skip_temporary

        # msgspec decoders
        # ref: https://jcristharif.com/msgspec/perf-tips.html#reuse-encoders-decoders
        self.user_decoder = msgspec.json.Decoder(User)
        self.users_decoder = msgspec.json.Decoder(list[User])
        self.posts_decoder = msgspec.json.Decoder(list[Post])
        self.chats_decoder = msgspec.json.Decoder(Chats)
        self.messages_decoder = msgspec.json.Decoder(Messages)
        self.highlight_category_decoder = msgspec.json.Decoder(list[HighlightCategory])
        self.highlight_decoder = msgspec.json.Decoder(Highlight)
        self.stories_decoder = msgspec.json.Decoder(list[Story])

    def __str__(self) -> str:
        return f'{self.name}\ncookie: {self.cookie}\nuser-agent: {self.user_agent}\nx-bc: {self.x_bc}'

    def generate_headers(self, url: str) -> dict[str, str]:
        '''Generates required headers for a request to the OnlyFans API.

        Args:
            `url`: The URL to be requested.

        Returns:
            A dictionary of headers to be used by `self.session`. See `onlyfans_dl.structs.HeaderRules` for more details.

        Raises:
            `ScrapingException`: The client was not initialized with a `header_rules` object.
        '''
        if self.header_rules is None:
            raise ScrapingException('client not initialized with header rules')

        parsed_url = urllib.parse.urlparse(url)
        url_path = f'{parsed_url.path}?{parsed_url.query}' if parsed_url.query else parsed_url.path

        # DC's logic for generating the headers.
        # Only `time` and `sign` need to be generated for each request.
        # `x_bc` is a random 40-character string that can persist between requests.
        # https://github.com/DIGITALCRIMINALS/OnlyFans/blob/85aec02065f6065c5b99b6caedec20ce947ffc2d/apis/api_helper.py#L348-L373
        current_seconds = str(int(time.time()))
        digest_hex = hashlib.sha1('\n'.join([self.header_rules.static_param, current_seconds, url_path, '0']).encode('utf-8')).hexdigest()
        headers = {
            'accept': 'application/json, text/plain, */*',
            'app-token': self.header_rules.app_token,
            'sign': self.header_rules.format.format(digest_hex, abs(sum([digest_hex.encode('ascii')[num] for num in self.header_rules.checksum_indexes]) + self.header_rules.checksum_constant)),
            'time': current_seconds,
            'x-bc': self.x_bc,
        }
        if self.cookie:
            headers['cookie'] = self.cookie
        if self.user_agent:
            headers['user-agent'] = self.user_agent
        return headers

    def send_get_request(self, url: str, output_file: str = '') -> requests.Response:
        '''Sends a request to a URL with OnlyFans headers.

        Args:
            `url`: The URL to be requested.
            `output_file`: The file to save the response to.

        Returns:
            A Response object containing the response details.

        Raises:
            `requests.RequestException`: An error occurred while sending the request.
        '''
        with self.session.get(url, headers=self.generate_headers(url), timeout=self.request_timeout) as response:
            response.raise_for_status()
            if output_file:
                with open(output_file, 'wb') as f:
                    f.write(response.content)
            return response

    @functools.cache
    def get_user_details(self, user: int | str) -> User:
        '''Retrieves the details of a user.

        Args:
            `user`: The user's ID or username.

        Returns:
            A `User` object describing the specified user.

        Raises:
            `ScrapingException`: An error occurred while retrieving or deserializing the user's details.
        '''
        url = f'https://onlyfans.com/api2/v2/users/{user}'
        try:
            response = self.send_get_request(url)
            return self.user_decoder.decode(response.content)
        except requests.RequestException:
            raise ScrapingException('failed to retrieve user details for %s' % user)
        except msgspec.DecodeError:
            LOGGER.debug('get_user_details(%s) response.content: %s', user, response.content)
            raise ScrapingException('failed to deserialize user details for %s' % user)

    def get_subscriptions(self) -> list[User]:
        '''Retrieves all active subscriptions.

        Returns:
            A list of `User` objects describing the subscriptions available to the scraper.

        Raises:
            `ScrapingException`: An error occurred while retrieving or deserializing the subscriptions.
        '''
        subscriptions: list[User] = []

        url = 'https://onlyfans.com/api2/v2/subscriptions/subscribes?limit=10&offset={offset}&type=active&sort=desc'
        offset = 0
        while True:
            try:
                response = self.send_get_request(url.format(offset=offset))
                users = self.users_decoder.decode(response.content)
            except requests.RequestException as e:
                raise ScrapingException(f'failed to retrieve subscriptions with scraper "{self.name}" at offset {offset} - status {e.response.status_code}')
            except msgspec.DecodeError:
                LOGGER.debug('get_subscriptions() response.content: %s', response.content)
                raise ScrapingException(f'failed to deserialize subscriptions with scraper "{self.name}" at offset {offset}')

            if users:
                subscriptions.extend(users)
                offset += len(users)
            else:
                break

        return subscriptions

    def get_post_media_by_id(self, user_id: int, *, skip_db: bool = False) -> list[NormalizedMedia]:
        '''Retrieves all posts with viewable media by a user.

        Args:
            `user_id`: The user's ID.

        Returns:
            A list of `Post` objects describing the posts by the specified user that are available to the scraper.

        Raises:
            `ScrapingException`: An error occurred while retrieving or deserializing the posts.
        '''
        user = self.get_user_details(user_id)
        user_medias: list[NormalizedMedia] = []

        last_post_timestamp = 0
        if not skip_db:
            try:
                with contextlib.closing(sqlite3.connect(pathlib.Path(self.download_root, user.username, '.media.db'))) as database:
                    last_post_timestamp = database.execute('SELECT max(timestamp) FROM media WHERE source_type == "posts"').fetchone()[0] or last_post_timestamp
            except sqlite3.OperationalError:
                pass

        url = 'https://onlyfans.com/api2/v2/users/{user_id}/posts?limit=10&offset={offset}&order=publish_date_desc'
        offset = 0
        while True:
            try:
                response = self.send_get_request(url.format(user_id=user_id, offset=offset))
                decoded_posts = self.posts_decoder.decode(response.content)
            except requests.RequestException:
                raise ScrapingException(f'failed to retrieve posts for user {user_id} at offset {offset} with scraper "{self.name}"')
            except msgspec.DecodeError:
                with open(f'decoding_error-{int(time.time())}.json', 'w') as f:
                    f.write(response.text)
                raise ScrapingException(f'failed to deserialize posts for user {user_id} at offset {offset} with scraper "{self.name}"')

            if not decoded_posts:
                break
            for post in decoded_posts:
                if int(datetime.strptime(post.posted_at, '%Y-%m-%dT%H:%M:%S%z').timestamp()) > last_post_timestamp:
                    if post.media and any(media.can_view for media in post.media):
                        user_medias += normalize_post_media(post, self.skip_temporary)
                else:
                    return user_medias

            offset += 10
            LOGGER.debug('%s posts retrieved for user %s', len(user_medias), user_id)

        return user_medias

    def get_archived_post_media_by_id(self, user_id: int, *, skip_db: bool = False) -> list[NormalizedMedia]:
        '''Retrieves all archived posts with viewable media by a user.

        Args:
            `user_id`: The user's ID.

        Returns:
            A list of `Post` objects describing the archived posts by the specified user that are available to the scraper.

        Raises:
            `ScrapingException`: An error occurred while retrieving or deserializing the posts.
        '''
        user = self.get_user_details(user_id)
        user_medias: list[NormalizedMedia] = []

        last_post_timestamp = 0
        if not skip_db:
            try:
                with contextlib.closing(sqlite3.connect(pathlib.Path(self.download_root, user.username, '.media.db'))) as database:
                    last_post_timestamp = database.execute('SELECT max(timestamp) FROM media WHERE source_type == "archived"').fetchone()[0] or last_post_timestamp
            except sqlite3.OperationalError:
                pass

        url = 'https://onlyfans.com/api2/v2/users/{user_id}/posts/archived?limit=10&offset={offset}&order=publish_date_desc'
        offset = 0
        while True:
            try:
                response = self.send_get_request(url.format(user_id=user_id, offset=offset))
                decoded_posts = self.posts_decoder.decode(response.content)
            except requests.RequestException:
                raise ScrapingException(f'failed to retrieve archived posts for user {user_id} at offset {offset} with scraper "{self.name}"')
            except msgspec.DecodeError:
                with open(f'decoding_error-{int(time.time())}.json', 'w') as f:
                    f.write(response.text)
                raise ScrapingException(f'failed to deserialize archived posts for user {user_id} at offset {offset} with scraper "{self.name}"')

            if not decoded_posts:
                break
            for post in decoded_posts:
                if int(datetime.strptime(post.posted_at, '%Y-%m-%dT%H:%M:%S%z').timestamp()) > last_post_timestamp:
                    if post.media and any(media.can_view for media in post.media):
                        user_medias += normalize_archived_post_media(post, self.skip_temporary)
                else:
                    return user_medias

            offset += 10
            LOGGER.debug('%s archived posts retrieved for user %s', len(user_medias), user_id)

        return user_medias

    def get_chats(self) -> list[User]:
        '''Retrieves all active chats.

        Returns:
            A list of user IDs.

        Raises:
            `ScrapingException`: An error occurred while retrieving or deserializing the chats.
        '''
        chats: list[User] = []

        url = 'https://onlyfans.com/api2/v2/chats?offset={offset}'
        offset = 0
        while True:
            try:
                response = self.send_get_request(url.format(offset=offset))
                decoded_chats = self.chats_decoder.decode(response.content)
            except requests.RequestException:
                raise ScrapingException(f'failed to retrieve chats with scraper "{self.name}" at offset {offset}')
            except msgspec.DecodeError:
                with open(f'decoding_error-{int(time.time())}.json', 'w') as f:
                    f.write(response.text)
                raise ScrapingException(f'failed to deserialize chats with scraper "{self.name}" at offset {offset}')

            chats += [self.get_user_details(chat.with_user.id) for chat in decoded_chats.chats]
            if not decoded_chats.has_more:
                return chats
            offset = decoded_chats.next_offset

    def get_message_media_by_id(self, user_id: int, *, skip_db: bool = False) -> list[NormalizedMedia]:
        '''Retrieves all messages with viewable media from a user.

        Args:
            `user_id`: The user's ID.

        Returns:
            A list of `Message` objects describing the messages from the specified user that are available to the scraper.

        Raises:
            `ScrapingException`: An error occurred while retrieving or deserializing the messages.
        '''
        user = self.get_user_details(user_id)
        user_medias: list[NormalizedMedia] = []

        last_message_timestamp = 0
        if not skip_db:
            try:
                with contextlib.closing(sqlite3.connect(pathlib.Path(self.download_root, user.username, '.media.db'))) as database:
                    last_message_timestamp = database.execute('SELECT max(timestamp) FROM media WHERE source_type == "messages"').fetchone()[0] or last_message_timestamp
            except sqlite3.OperationalError:
                pass

        url = 'https://onlyfans.com/api2/v2/chats/{user_id}/messages?limit=10&offset={offset}&order=desc'
        offset = 0
        while True:
            try:
                response = self.send_get_request(url.format(user_id=user_id, offset=offset))
                decoded_messages = self.messages_decoder.decode(response.content)
            except requests.RequestException:
                raise ScrapingException(f'failed to retrieve messages for user {user_id} at offset {offset} with scraper "{self.name}"')
            except msgspec.DecodeError:
                with open(f'decoding_error-{int(time.time())}.json', 'w') as f:
                    f.write(response.text)
                raise ScrapingException(f'failed to deserialize messages for user {user_id} at offset {offset} with scraper "{self.name}"')

            for message in decoded_messages.messages:
                if int(datetime.strptime(message.created_at, '%Y-%m-%dT%H:%M:%S%z').timestamp()) > last_message_timestamp:
                    if message.from_user.id == user_id:
                        if message.media and any(media.can_view for media in message.media):
                            user_medias += normalize_message_media(message)
                else:
                    return user_medias
            if not decoded_messages.has_more:
                return user_medias
            offset += len(decoded_messages.messages)

    # def get_purchased_media(self, user_id: int, *, skip_db: bool = False) -> list[NormalizedMedia]:
    #     medias: list[NormalizedMedia] = []
    #     url = 'https://onlyfans.com/api2/v2/posts/paid?limit=10&offset={offset}'

    #     offset = 0
    #     while True:
    #         try:
    #             response = self.send_get_request(url.format(offset=offset))
    #             decoded_media = self.media_decoder(response.content)
    #         except requests.RequestException:
    #             raise ScrapingException(f'failed to get purchased media at offset {offset} for scraper "{self.name}"')
    #         except msgspec.DecodeError:
    #             raise ScrapingException(f'failed to deserialize purchased media at offset {offset} with scraper "{self.name}"')

    def get_highlight_media_by_id(self, user_id: int, *, skip_db: bool = False) -> list[NormalizedMedia]:
        user = self.get_user_details(user_id)
        user_medias: list[NormalizedMedia] = []

        last_highlight_timestamp = 0
        if not skip_db:
            try:
                with contextlib.closing(sqlite3.connect(pathlib.Path(self.download_root, user.username, '.media.db'))) as database:
                    last_highlight_timestamp = database.execute('SELECT max(timestamp) FROM media WHERE source_type == "highlights"').fetchone()[0] or last_highlight_timestamp
            except sqlite3.OperationalError:
                pass

        categories: list[HighlightCategory] = []
        categories_url = 'https://onlyfans.com/api2/v2/users/{user_id}/stories/highlights?limit=5&offset={offset}'
        offset = 0
        while True:
            formatted_categories_url = categories_url.format(user_id=user_id, offset=offset)
            try:
                response = self.send_get_request(formatted_categories_url)
                decoded_categories = self.highlight_category_decoder.decode(response.content)

            except requests.RequestException:
                raise ScrapingException(f'failed to retrieve highlights for user {user_id} with scraper "{self.name}"')
            except msgspec.DecodeError:
                raise ScrapingException(f'failed to deserialize highlights for user {user_id} with scraper "{self.name}"')

            if not decoded_categories:
                break
            categories += decoded_categories
            offset += 5

        highlights_url = 'https://onlyfans.com/api2/v2/stories/highlights/{id}'
        for category in categories:
            formatted_highlights_url = highlights_url.format(id=category.id)
            response = self.send_get_request(formatted_highlights_url)
            decoded_highlight = self.highlight_decoder.decode(response.content)
            for story in reversed(decoded_highlight.stories):
                if int(datetime.strptime(story.created_at, '%Y-%m-%dT%H:%M:%S%z').timestamp()) > last_highlight_timestamp:
                    user_medias += normalize_story_media(story, highlight_category=category.title)
                else:
                    break

        return user_medias

    def get_story_media_by_id(self, user_id: int, *, skip_db: bool = False) -> list[NormalizedMedia]:
        '''Fetches all stories from a user.

        Args:
            `user_id`: The user's ID.

        Returns:
            A list of `Story` objects describing the stories from the specified user.

        Raises:
            `ScrapingException`: An error occurred while retrieving or deserializing the stories.
        '''
        user = self.get_user_details(user_id)
        user_medias: list[NormalizedMedia] = []

        last_story_timestamp = 0
        if not skip_db:
            try:
                with contextlib.closing(sqlite3.connect(pathlib.Path(self.download_root, user.username, '.media.db'))) as database:
                    last_story_timestamp = database.execute('SELECT max(timestamp) FROM media WHERE source_type == "stories"').fetchone()[0] or last_story_timestamp
            except sqlite3.OperationalError:
                pass

        # TODO: Figure out how they paginate this endpoint.
        url = 'https://onlyfans.com/api2/v2/users/{user_id}/stories'
        try:
            response = self.send_get_request(url.format(user_id=user_id))
            decoded_stories = self.stories_decoder.decode(response.content)
        except requests.RequestException:
            raise ScrapingException(f'failed to retrieve stories for user {user.username} with scraper "{self.name}"')
        except msgspec.DecodeError:
            raise ScrapingException(f'failed to deserialize stories for user {user.username} with scraper "{self.name}"')

        for story in reversed(decoded_stories):
            if int(datetime.strptime(story.created_at, '%Y-%m-%dT%H:%M:%S%z').timestamp()) > last_story_timestamp:
                user_medias += normalize_story_media(story)
            else:
                return user_medias
        return user_medias

    def download_media(self, user: User, medias: list[NormalizedMedia]) -> None:
        '''
        Download media from a list of posts.
        '''
        if not medias:
            return
        LOGGER.info('downloading media for %s', user.username)
        user_dir = pathlib.Path(self.download_root, user.username)
        user_dir.mkdir(parents=True, exist_ok=True)
        with contextlib.closing(sqlite3.connect(pathlib.Path(self.download_root, user.username, '.media.db'))) as database:
            cur = database.execute('''
                CREATE TABLE IF NOT EXISTS media (
                    source_type TEXT,
                    timestamp INTEGER,
                    source_id INTEGER,
                    media_id INTEGER,
                    PRIMARY KEY (source_type, source_id, media_id)
                ) WITHOUT ROWID
            ''')
            cur.execute('''
                CREATE INDEX IF NOT EXISTS created_on
                ON media(timestamp)
            ''')

            if user.avatar:
                try:
                    response = self.session.get(user.avatar)
                    response.raise_for_status()
                    timestamp = int(datetime.strptime(response.headers['last-modified'], '%a, %d %b %Y %X GMT').timestamp())
                    dest_file = pathlib.Path(user_dir, 'avatar.jpg')
                    if not cur.execute('SELECT * FROM media WHERE source_type = ? AND source_id = ? AND media_id = ?', ('avatar', timestamp, timestamp)).fetchone():
                        if existing_avatars := cur.execute('SELECT * FROM media WHERE source_type = ?', ('avatar',)).fetchall():
                            current_avatar_timestamp: int = max(existing_avatars, key=itemgetter(1))[1]
                            old_avatar_file = pathlib.Path(dest_file.parent, f'avatar-{current_avatar_timestamp}.jpg')
                            dest_file.rename(old_avatar_file)
                        temp_file = pathlib.Path(dest_file.parent, f'{dest_file.name}.{secrets.token_urlsafe(6)}.part')
                        with temp_file.open('wb') as f:
                            f.write(response.content)
                        temp_file.rename(dest_file)
                        cur.execute('INSERT INTO media VALUES (?, ?, ?, ?)', ('avatar', timestamp, timestamp, timestamp))
                except requests.RequestException:
                    LOGGER.exception('error getting avatar')
                    exit(1)

            if user.header:
                try:
                    response = self.session.get(user.header)
                    response.raise_for_status()
                    timestamp = int(datetime.strptime(response.headers['last-modified'], '%a, %d %b %Y %X GMT').timestamp())
                    dest_file = pathlib.Path(user_dir, 'header.jpg')
                    if not cur.execute('SELECT * FROM media WHERE source_type = ? AND source_id = ? AND media_id = ?', ('header', timestamp, timestamp)).fetchone():
                        if existing_headers := cur.execute('SELECT * FROM media WHERE source_type = ?', ('header',)).fetchall():
                            current_header_timestamp: int = max(existing_headers, key=itemgetter(1))[1]
                            old_header_file = pathlib.Path(dest_file.parent, f'header-{current_header_timestamp}.jpg')
                            dest_file.rename(old_header_file)
                        temp_file = pathlib.Path(dest_file.parent, f'{dest_file.name}.{secrets.token_urlsafe(6)}.part')
                        with temp_file.open('wb') as f:
                            f.write(response.content)
                        temp_file.rename(dest_file)
                        cur.execute('INSERT INTO media VALUES (?, ?, ?, ?)', ('header', timestamp, timestamp, timestamp))
                except requests.RequestException:
                    LOGGER.exception('error getting header')
                    exit(1)

            for media in medias:
                if cur.execute('SELECT * FROM media WHERE source_type = ? AND source_id = ? AND media_id = ?', (media.source_type, media.source_id, media.id)).fetchone():
                    continue
                creation_date = datetime.strptime(media.created_at, '%Y-%m-%dT%H:%M:%S%z')
                match media.file_type:
                    case 'photo':
                        ext = 'jpg'
                    case 'video':
                        ext = 'mp4'
                    case 'audio':
                        ext = 'mp3'
                    case 'gif':
                        ext = 'mp4'
                    case _:
                        LOGGER.info(f'unknown media type: {media.file_type}')
                        continue

                dest_file = pathlib.Path(
                    user_dir,
                    media.source_type,
                    media.file_type + 's',
                    sanitize_filename(f'{creation_date.strftime("%Y-%m-%d")}.{media.id}.{media.text[:35]}.{ext}'),
                )
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                temp_file = pathlib.Path(dest_file.parent, f'{dest_file.name}.{secrets.token_urlsafe(6)}.part')
                try:
                    response = self.session.get(media.url, stream=True, timeout=10)
                    if dest_file.exists() and dest_file.stat().st_size == int(response.headers.get('content-length', '0')):
                        cur.execute('INSERT INTO media VALUES (?, ?, ?, ?)', (media.source_type, int(creation_date.timestamp()), media.source_id, media.id))
                        continue
                    response.raise_for_status()
                    with open(temp_file, 'wb') as f:
                        for chunk in response.iter_content(requests.models.CONTENT_CHUNK_SIZE):
                            f.write(chunk)
                    temp_file.rename(dest_file)
                    cur.execute('INSERT INTO media VALUES (?, ?, ?, ?)', (media.source_type, int(creation_date.timestamp()), media.source_id, media.id))
                except requests.RequestException:
                    LOGGER.exception('error getting media')
                    exit(1)
            database.commit()
        LOGGER.info('finished downloading media for %s', user.username)
