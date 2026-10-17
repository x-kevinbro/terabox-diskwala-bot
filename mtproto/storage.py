"""Durable archive ledger. All SQL parameters are bound; no tokens in errors."""
from __future__ import annotations

import hashlib
import re
import secrets
import time
from urllib.parse import parse_qsl, urlencode, urlsplit

import aiohttp
from telegraph import normalize_share
from resolvers import pick_provider


class StorageError(Exception):
    pass


class ReviewRequired(StorageError):
    """An upload may have succeeded; never blindly upload it again."""


def link_identity(raw: str) -> tuple[str, str]:
    url = normalize_share(raw)
    if not url:
        raise StorageError('Unsupported share URL')
    parsed = urlsplit(url)
    pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
             if not k.lower().startswith('utm_') and k.lower() not in {'fbclid', 'gclid'}]
    provider = pick_provider(url)
    path = re.fullmatch(r'/s/([A-Za-z0-9_-]+)/?', parsed.path)
    # Only collapse known share forms. Preserve other URLs conservatively.
    if provider == 'terabox' and path:
        code = path.group(1)
        identity = 'terabox:share:' + code
    elif provider == 'terabox' and parsed.path in {'/sharing/link', '/share/link', '/'} and dict(pairs).get('surl'):
        identity = 'terabox:share:1' + dict(pairs)['surl']
        pairs = [(k, v) for k, v in pairs if k != 'surl']
    else:
        identity = f'{provider}:{parsed.hostname}:{parsed.path}'
    identity += '?' + urlencode(sorted(pairs))
    return hashlib.sha256(identity.encode()).hexdigest(), url


def turso_endpoint(raw: str) -> str:
    p = urlsplit(raw)
    if (p.scheme not in {'libsql', 'turso', 'https'} or not p.hostname
            or not p.hostname.endswith('.turso.io') or p.username or p.password
            or p.port not in {None, 443} or p.path not in {'', '/'} or p.query or p.fragment):
        raise StorageError('Use the database URL from Turso Cloud (*.turso.io), without paths or credentials')
    return f'https://{p.hostname}/v2/pipeline'


def encode_value(value):
    if value is None:
        return {'type': 'null'}
    if isinstance(value, int):
        return {'type': 'integer', 'value': str(value)}
    return {'type': 'text', 'value': str(value)}


class TursoSQL:
    def __init__(self, session, url: str, token: str):
        self.session, self.endpoint, self._token = session, turso_endpoint(url), token

    async def execute(self, sql, args=()):
        payload = {'requests': [
            {'type': 'execute', 'stmt': {'sql': sql, 'args': [encode_value(v) for v in args], 'want_rows': True}},
            {'type': 'close'},
        ]}
        try:
            async with self.session.post(self.endpoint, json=payload, allow_redirects=False,
                    headers={'Authorization': f'Bearer {self._token}'},
                    timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    raise StorageError(f'Database unavailable (HTTP {response.status})')
                data = await response.json()
            result = data['results'][0]
            if result.get('type') != 'ok':
                raise StorageError('Database query failed')
            table = result['response']['result']
            names = [c['name'] for c in table['cols']]
            rows = []
            for row in table['rows']:
                values = [None if v['type'] == 'null' else int(v['value']) if v['type'] == 'integer' else v.get('value') for v in row]
                rows.append(dict(zip(names, values)))
            return rows
        except StorageError:
            raise
        except (aiohttp.ClientError, TimeoutError, ValueError, KeyError, TypeError):
            raise StorageError('Database request failed; no automatic upload retry') from None


SCHEMA = [
    '''CREATE TABLE IF NOT EXISTS archive_links (
        id TEXT PRIMARY KEY, url TEXT NOT NULL, token TEXT NOT NULL UNIQUE,
        source_page TEXT NOT NULL DEFAULT '', chat_id INTEGER NOT NULL,
        state TEXT NOT NULL DEFAULT 'queued', owner TEXT, lease_until INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, error TEXT NOT NULL DEFAULT '')''',
    '''CREATE TABLE IF NOT EXISTS archive_assets (
        hash TEXT PRIMARY KEY, state TEXT NOT NULL, owner TEXT NOT NULL,
        channel_id INTEGER, message_id INTEGER, file_id TEXT,
        name TEXT NOT NULL, size INTEGER NOT NULL, created_at INTEGER NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS archive_files (
        link_id TEXT NOT NULL, slot TEXT NOT NULL, asset_hash TEXT NOT NULL,
        PRIMARY KEY(link_id, slot))''',
    'CREATE INDEX IF NOT EXISTS archive_queue ON archive_links(state, created_at)',
]


class Ledger:
    def __init__(self, db):
        self.db = db

    async def initialize(self):
        for sql in SCHEMA:
            await self.db.execute(sql)

    async def enqueue(self, urls, chat_id, source_page=''):
        identities = dict(link_identity(url) for url in urls)
        if not identities or len(identities) > 50:
            raise StorageError('Send between 1 and 50 supported links')
        now = int(time.time())
        args = []
        for key, url in identities.items():
            args.extend((key, url, secrets.token_urlsafe(24), source_page, chat_id, now, now))
        placeholders = ','.join('(?,?,?,?,?,?,?)' for _ in identities)
        # One atomic insert stores the complete submitted batch before any worker can see it.
        inserted = await self.db.execute(f'''INSERT INTO archive_links
            (id,url,token,source_page,chat_id,created_at,updated_at) VALUES {placeholders}
            ON CONFLICT(id) DO NOTHING RETURNING id''', args)
        new = {r['id'] for r in inserted}
        marks = ','.join('?' for _ in identities)
        # Failed/partial/cancelled jobs can retry; stored assets are always reused.
        retried = await self.db.execute(f'''UPDATE archive_links SET state='queued',chat_id=?,
            owner=NULL,lease_until=0,updated_at=?,error=''
            WHERE id IN ({marks}) AND state IN ('failed','partial','cancelled') RETURNING id''',
            (chat_id, now, *identities))
        retry_ids = {r['id'] for r in retried}
        rows = await self.db.execute(f'SELECT * FROM archive_links WHERE id IN ({marks})', tuple(identities))
        by_id = {r['id']: r for r in rows}
        return [(by_id[key], 'new' if key in new else 'retry' if key in retry_ids else 'duplicate') for key in identities]

    async def expire(self):
        # Never re-upload an interrupted job automatically: an upload acknowledgement
        # could have been lost. An operator must reconcile review jobs/assets.
        await self.db.execute("""UPDATE archive_links SET state='review',error='Expired processing lease',
            owner=NULL WHERE state='processing' AND lease_until < ?""", (int(time.time()),))

    async def claim(self, owner):
        now = int(time.time())
        rows = await self.db.execute('''UPDATE archive_links SET state='processing',owner=?,lease_until=?,updated_at=?
            WHERE id=(SELECT id FROM archive_links WHERE state='queued' ORDER BY created_at,id LIMIT 1)
            AND state='queued' AND NOT EXISTS
            (SELECT 1 FROM archive_links WHERE state='processing') RETURNING *''', (owner, now+180, now))
        return rows[0] if rows else None

    async def heartbeat(self, key, owner):
        rows = await self.db.execute("UPDATE archive_links SET lease_until=? WHERE id=? AND owner=? AND state='processing' RETURNING id",
                                     (int(time.time())+180, key, owner))
        if not rows:
            raise ReviewRequired('Processing lease was lost')

    async def finish(self, key, owner, state, error=''):
        rows = await self.db.execute('''UPDATE archive_links SET state=?,error=?,owner=NULL,lease_until=0,updated_at=?
            WHERE id=? AND owner=? AND state='processing' RETURNING id''',
            (state, error, int(time.time()), key, owner))
        if not rows:
            raise ReviewRequired('Processing lease was lost')

    async def cancel_queued(self, chat_id):
        return await self.db.execute("UPDATE archive_links SET state='cancelled' WHERE chat_id=? AND state='queued' RETURNING id", (chat_id,))

    async def known_file(self, key, slot):
        rows = await self.db.execute('''SELECT a.* FROM archive_assets a JOIN archive_files f
            ON f.asset_hash=a.hash WHERE f.link_id=? AND f.slot=?''', (key, slot))
        return rows[0] if rows else None

    async def reserve_asset(self, digest, key, slot, name, size):
        rows = await self.db.execute('''INSERT INTO archive_assets(hash,state,owner,name,size,created_at)
            VALUES(?,'uploading',?,?,?,?) ON CONFLICT(hash) DO NOTHING RETURNING *''',
            (digest, key, name, size, int(time.time())))
        await self.db.execute('''INSERT INTO archive_files(link_id,slot,asset_hash) VALUES(?,?,?)
            ON CONFLICT(link_id,slot) DO UPDATE SET asset_hash=excluded.asset_hash''', (key, slot, digest))
        if rows:
            return rows[0], True
        existing = await self.db.execute('SELECT * FROM archive_assets WHERE hash=?', (digest,))
        return existing[0], False

    async def save_asset(self, digest, key, channel_id, message_id, file_id):
        rows = await self.db.execute('''UPDATE archive_assets SET state='stored',channel_id=?,message_id=?,file_id=?
            WHERE hash=? AND owner=? AND state='uploading' RETURNING *''', (channel_id, message_id, file_id, digest, key))
        if not rows:
            raise ReviewRequired('Upload confirmation could not be saved')
        return rows[0]

    async def retrieval(self, token):
        if not re.fullmatch(r'[A-Za-z0-9_-]{32}', token):
            return None, []
        links = await self.db.execute('SELECT id,state FROM archive_links WHERE token=?', (token,))
        if not links:
            return None, []
        rows = await self.db.execute('''SELECT DISTINCT a.channel_id,a.message_id FROM archive_assets a
            JOIN archive_files f ON f.asset_hash=a.hash WHERE f.link_id=? AND a.state='stored'
            ORDER BY a.message_id''', (links[0]['id'],))
        return links[0], rows
