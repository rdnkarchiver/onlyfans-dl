import msgspec


class HeaderRules(msgspec.Struct):

    static_param: str
    format: str
    checksum_indexes: list[int]
    checksum_constant: int
    app_token: str


class User(msgspec.Struct):

    """Useful attributes from the OnlyFans API. Inherits `msgspec.Struct`."""

    id: int
    username: str | None
    name: str | None
    avatar: str | None
    header: str | None

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, User):
            return NotImplemented
        return self.id == other.id


class Post(msgspec.Struct, rename='camel'):

    """Useful attributes from the OnlyFans API. Inherits `msgspec.Struct`."""

    class Media(msgspec.Struct, rename='camel'):

        class Source(msgspec.Struct):

            source: str | None
            width: int
            height: int
            duration: int

        id: int
        type: str
        can_view: bool
        source: Source

    id: int
    posted_at: str
    posted_at_precise: str
    # In the event of a reported post, all of these will not be included in the
    # response, so they are defaulted to `None`.
    # This behaves similar to `omitempty` in Golang.
    expired_at: str | None = None
    author: User | None = None
    raw_text: str | None = None
    price: int | float | None = None
    is_archived: bool | None = None
    media: list[Media] | None = None
    preview: list[int | str] | None = None


class Posts(msgspec.Struct, rename={'has_more': 'hasMore', 'posts': 'list'}.get):

    """Useful attributes from the OnlyFans API. Inherits `msgspec.Struct`."""

    posts: list[Post]
    has_more: bool


class Chats(msgspec.Struct, rename={'has_more': 'hasMore', 'next_offset': 'nextOffset', 'chats': 'list'}.get):

    """Describes the relevant fields of OnlyFans's chats API response."""

    class Chat(msgspec.Struct, rename='camel'):

        with_user: User

    chats: list[Chat]
    has_more: bool
    next_offset: int


class Message(msgspec.Struct, rename='camel'):

    class Media(msgspec.Struct, rename='camel'):

        class Info(msgspec.Struct):

            class Source(msgspec.Struct):

                width: int
                height: int

            source: Source

        id: int
        can_view: bool
        type: str
        src: str | None
        duration: int
        info: Info

    text: str | None
    price: int | float
    media: list[Media]
    previews: list[int]
    from_user: User
    id: int
    created_at: str


class Messages(msgspec.Struct, rename={'has_more': 'hasMore', 'messages': 'list'}.get):

    messages: list[Message]
    has_more: bool


class Story(msgspec.Struct, rename='camel'):

    '''
    Describes the relevant fields of OnlyFans's story struct.
    '''

    class Media(msgspec.Struct, rename='camel'):

        class Source(msgspec.Struct):

            source: str | None
            width: int
            height: int
            duration: int

        id: int
        type: str
        can_view: bool
        source: Source

    class Question(msgspec.Struct):

        class Entity(msgspec.Struct):

            text: str

        entity: Entity

    id: int
    user_id: int
    created_at: str
    media: list[Media]
    question: Question | None


class HighlightCategory(msgspec.Struct, rename='camel'):

    '''
    Describes the relevant fields of OnlyFans's highlight struct.
    '''

    id: int
    user_id: int
    title: str
    cover: str
    created_at: str


class Highlight(msgspec.Struct, rename='camel'):

    '''
    Describes the relevant fields of OnlyFans's highlights API response.
    '''

    id: int
    user_id: int
    title: str
    cover: str
    created_at: str
    stories: list[Story]


class NormalizedMedia(msgspec.Struct):
    '''Custom normalized media'''
    user_id: int
    source_type: str
    source_id: int
    id: int
    file_type: str
    created_at: str
    value: str
    text: str
    width: int
    height: int
    duration: int
    url: str
    expired_at: str | None = None
    highlight_category: str | None = None


def normalize_post_media(post: Post, skip_temporary: bool = False) -> list[NormalizedMedia]:
    nm: list[NormalizedMedia] = []
    if not post.media or (post.expired_at and skip_temporary):
        return nm
    else:
        for media in post.media:
            if not media.can_view:
                continue
            nm.append(
                NormalizedMedia(
                    user_id=post.author.id,
                    source_type='posts',
                    source_id=post.id,
                    id=media.id,
                    file_type=media.type,
                    created_at=post.posted_at,
                    value='paid' if post.price else 'free',
                    text=post.raw_text,
                    width=media.source.width,
                    height=media.source.height,
                    duration=media.source.duration,
                    url=media.source.source,
                ),
            )
    return nm

def normalize_archived_post_media(post: Post, skip_temporary: bool = False) -> list[NormalizedMedia]:
    nm: list[NormalizedMedia] = []
    if not post.media or (post.expired_at and skip_temporary):
        return nm
    else:
        for media in post.media:
            if not media.can_view:
                continue
            nm.append(
                NormalizedMedia(
                    user_id=post.author.id,
                    source_type='archived',
                    source_id=post.id,
                    id=media.id,
                    file_type=media.type,
                    created_at=post.posted_at,
                    value='paid' if post.price and media.id not in post.preview else 'free',
                    text=post.raw_text,
                    width=media.source.width,
                    height=media.source.height,
                    duration=media.source.duration,
                    url=media.source.source
                ),
            )
    return nm

def normalize_message_media(message: Message) -> list[NormalizedMedia]:
    nm: list[NormalizedMedia] = []
    for media in message.media:
        if not media.can_view:
            continue
        nm.append(
            NormalizedMedia(
                user_id=message.from_user.id,
                source_type='messages',
                source_id=message.id,
                id=media.id,
                file_type=media.type,
                created_at=message.created_at,
                value='paid' if message.price and media.id not in message.previews else 'free',
                text=message.text,
                width=media.info.source.width,
                height=media.info.source.height,
                duration=media.duration,
                url=media.src,
            ),
        )
    return nm

def normalize_story_media(story: Story, *, highlight_category: str | None = None) -> list[NormalizedMedia]:
    nm: list[NormalizedMedia] = []
    for media in story.media:
        if not media.can_view:
            continue
        nm.append(
            NormalizedMedia(
                user_id=story.user_id,
                source_type='stories',
                source_id=story.id,
                highlight_category=highlight_category,
                id=media.id,
                file_type=media.type,
                created_at=story.created_at,
                text=f'{highlight_category}.{story.question.entity.text}' if story.question else highlight_category,
                width=media.source.width,
                height=media.source.height,
                duration=media.source.duration,
                url=media.source.source,
            ),
        )
    return nm
