import os
import re
import json
import time
import uuid
import random
import threading
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, request, jsonify
from werkzeug.exceptions import HTTPException
from pymongo import MongoClient
from pymongo.errors import (
    DuplicateKeyError, OperationFailure, ConfigurationError, PyMongoError, ServerSelectionTimeoutError
)

app = Flask(__name__)

RANKS = ['A', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K']
RANK_NAMES = {
    'A': 'Ace', '2': 'Two', '3': 'Three', '4': 'Four',
    '5': 'Five', '6': 'Six', '7': 'Seven', '8': 'Eight',
    '9': 'Nine', '10': 'Ten', 'J': 'Jack', 'Q': 'Queen', 'K': 'King'
}
SUITS = ['♠', '♥', '♦', '♣']
MAX_PLAYERS = 3
ROOM_TTL_SECONDS = 6 * 60 * 60
LOCAL_STORE_PATH = '/tmp/bullshit_3p_rooms.json' if os.name != 'nt' else 'bullshit_3p_rooms.json'

# MongoDB Atlas connection string, set as an environment variable in Vercel.
MONGODB_URI = os.environ.get('MONGODB_URI') or os.environ.get('MONGO_URI') or ''
MONGODB_DB = os.environ.get('MONGODB_DB') or 'bullshit3p'
USE_MONGO = bool(MONGODB_URI)
IS_SERVERLESS = bool(os.environ.get('VERCEL') or os.environ.get('AWS_LAMBDA_FUNCTION_NAME'))

NO_CHANGE = object()  # returned by a mutation to say "nothing to save"


class ApiError(Exception):
    def __init__(self, message, status=400, retryable=False):
        super().__init__(message)
        self.message = message
        self.status = status
        self.retryable = retryable


# ---------------------------------------------------------
# Storage Layer (MongoDB Atlas on Vercel / Local JSON fallback)
#
# One document per room: {_id: code, version, expires_at, data: <room as JSON text>}.
# Every write is a compare-and-set on `version`, so two players acting at the same
# moment (or a chat message landing during a play) can no longer overwrite each other.
# ---------------------------------------------------------
def ensure_storage():
    if IS_SERVERLESS and not USE_MONGO:
        raise ApiError(
            'Game storage is not configured on the server. Add MONGODB_URI in your '
            'Vercel project settings (Environment Variables), then redeploy.',
            500
        )


_mongo_client = None
_mongo_lock = threading.Lock()


def _rooms_collection():
    """Create the client once and reuse it, so serverless calls don't open new connections."""
    global _mongo_client
    if _mongo_client is None:
        with _mongo_lock:
            if _mongo_client is None:
                client = MongoClient(
                    MONGODB_URI,
                    serverSelectionTimeoutMS=4000,
                    connectTimeoutMS=4000,
                    socketTimeoutMS=8000,
                    maxPoolSize=5,
                    appname='bullshit-3p'
                )
                coll = client[MONGODB_DB]['rooms']
                try:
                    # Atlas deletes a room automatically once expires_at has passed
                    coll.create_index('expires_at', expireAfterSeconds=0)
                except PyMongoError:
                    pass  # not fatal; rooms are also ignored once expired when read
                _mongo_client = client
    return _mongo_client[MONGODB_DB]['rooms']


AUTH_ERROR_CODES = (13, 18, 8000)  # Unauthorized, AuthenticationFailed, Atlas auth error


def _mongo_call(op):
    """Run a MongoDB operation, retrying brief network hiccups and translating errors."""
    for attempt in range(2):
        try:
            return op(_rooms_collection())
        except DuplicateKeyError:
            raise  # a normal outcome when a new room code is already taken
        except ConfigurationError:
            raise ApiError('MONGODB_URI looks invalid. Copy the connection string from Atlas again.', 500)
        except OperationFailure as e:
            if e.code in AUTH_ERROR_CODES:
                raise ApiError(
                    'MongoDB rejected the login. Check the username and password in MONGODB_URI.', 500
                )
            err = ApiError('The database had a problem. Please retry.', 503, retryable=True)
        except ServerSelectionTimeoutError:
            err = ApiError(
                'Cannot reach MongoDB Atlas. In Atlas, open Network Access and allow 0.0.0.0/0.',
                503, retryable=True
            )
        except PyMongoError:
            err = ApiError('Connection to the game server is unstable. Retrying...', 503, retryable=True)
        if attempt == 0:
            time.sleep(0.2)
        else:
            raise err


_local_lock = threading.RLock()


def _local_read_all():
    if not os.path.exists(LOCAL_STORE_PATH):
        return {}
    try:
        with open(LOCAL_STORE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _local_write_all(data):
    tmp_path = LOCAL_STORE_PATH + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f)
    os.replace(tmp_path, LOCAL_STORE_PATH)


def _utcnow():
    return datetime.now(timezone.utc)


def get_room(code):
    ensure_storage()
    code = (code or '').upper().strip()
    if USE_MONGO:
        doc = _mongo_call(lambda c: c.find_one({'_id': code}))
        if not doc:
            return None
        expires_at = doc.get('expires_at')
        if expires_at is not None:
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at <= _utcnow():
                return None  # expired; Atlas will delete it shortly
        try:
            room = json.loads(doc['data'])
        except (KeyError, ValueError, TypeError):
            return None
        room['version'] = doc.get('version', 0)
        return room
    with _local_lock:
        return _local_read_all().get(code)


def save_room_cas(room, expected_version):
    """Write the room only if nobody changed it since we read it. Returns True on success."""
    ensure_storage()
    code = room['code'].upper().strip()
    room['version'] = expected_version + 1
    room['updated_at'] = time.time()

    if USE_MONGO:
        doc = {
            '_id': code,
            'version': room['version'],
            'expires_at': _utcnow() + timedelta(seconds=ROOM_TTL_SECONDS),
            'data': json.dumps(room)
        }
        if expected_version == 0:
            # brand-new room: only succeeds if this code is not taken yet
            try:
                _mongo_call(lambda c: c.insert_one(doc))
                return True
            except DuplicateKeyError:
                return False
        result = _mongo_call(lambda c: c.replace_one({'_id': code, 'version': expected_version}, doc))
        return result.matched_count == 1

    with _local_lock:
        data = _local_read_all()
        now = time.time()
        for stale in [c for c, r in data.items() if now - r.get('updated_at', now) > ROOM_TTL_SECONDS]:
            del data[stale]
        current = data.get(code)
        current_version = current.get('version', 0) if current else 0
        if current_version != expected_version:
            return False
        data[code] = room
        _local_write_all(data)
        return True


def mutate_room(code, fn, request_id=None):
    """Read -> apply fn(room) -> compare-and-set save, retrying if another request got in first.

    request_id makes retries safe: if the browser re-sends a request after a timeout
    and the first copy actually went through, the second copy is ignored.
    """
    code = (code or '').upper().strip()
    for attempt in range(8):
        room = get_room(code)
        if room is None:
            raise ApiError('Room expired or not found.', 404)

        seen = room.setdefault('seen', [])
        if request_id and request_id in seen:
            return room

        outcome = fn(room)
        if outcome is NO_CHANGE:
            return room

        if request_id:
            seen.append(request_id)
            del seen[:-40]

        if save_room_cas(room, room.get('version', 0)):
            return room
        time.sleep(random.uniform(0.02, 0.08) * (attempt + 1))

    raise ApiError('The table is busy. Please try again.', 503, retryable=True)


# ---------------------------------------------------------
# Card & Game Helpers
# ---------------------------------------------------------
def create_deck():
    deck = [{'rank': r, 'suit': s, 'id': f'{r}{s}'} for s in SUITS for r in RANKS]
    random.shuffle(deck)
    return deck


def sort_hand(hand):
    order = {r: i for i, r in enumerate(RANKS)}
    return sorted(hand, key=lambda c: (order[c['rank']], c['suit']))


def plural_rank(rank, count):
    name = RANK_NAMES[rank]
    if count == 1:
        return f'1 {name}'
    if rank == '6':
        return f'{count} Sixes'
    return f'{count} {name}s'


def clean_code(code):
    return (code or '').upper().strip()[:8]


def clean_name(name, default):
    name = re.sub(r'[<>&"\'`]', '', (name or '')).strip()[:16]
    return name or default


def add_chat(room, sender, text):
    room['chat_seq'] = room.get('chat_seq', 0) + 1
    room.setdefault('chat', []).append({'n': room['chat_seq'], 'sender': sender, 'text': text})
    del room['chat'][:-50]


def require_player(room, player_id):
    player = next((p for p in room['players'] if p['id'] == player_id), None)
    if not player:
        raise ApiError('You are not seated at this table.', 403)
    return player


def deal_game(room):
    deck = create_deck()
    for p in room['players']:
        p['hand'] = []
        p['bullets'] = 3

    # Distribute all 52 cards across the 3 players
    for idx, card in enumerate(deck):
        room['players'][idx % MAX_PLAYERS]['hand'].append(card)

    for p in room['players']:
        p['hand'] = sort_hand(p['hand'])

    room['status'] = 'playing'
    room['pile'] = []
    room['turn_index'] = 0
    room['dealer_index'] = MAX_PLAYERS - 1
    room['current_rank_index'] = 0  # Starts on Ace ('A')
    room['last_play'] = None
    room['last_reveal'] = None
    room['pending_winner_id'] = None
    room['rankings'] = []
    room['banner_text'] = f"{room['players'][0]['name']} starts! Declare Aces."


def finalize_winner(room, winner_id):
    room['status'] = 'finished'
    room['pending_winner_id'] = None
    sorted_players = sorted(
        room['players'],
        key=lambda p: (0 if p['id'] == winner_id else 1, len(p['hand']))
    )
    room['rankings'] = [
        {
            'place': idx + 1,
            'id': p['id'],
            'name': p['name'],
            'cards_left': len(p['hand'])
        }
        for idx, p in enumerate(sorted_players)
    ]
    winner_name = sorted_players[0]['name']
    room['banner_text'] = f"🏆 {winner_name} emptied their hand and won!"


def sanitize_room_for_player(room, player_id):
    players_view = []
    my_hand = []
    for idx, p in enumerate(room['players']):
        is_me = (p['id'] == player_id)
        if is_me:
            my_hand = p['hand']
        players_view.append({
            'id': p['id'],
            'name': p['name'],
            'seat': idx,
            'is_host': idx == 0,
            'is_dealer': idx == room.get('dealer_index', MAX_PLAYERS - 1),
            'card_count': len(p['hand']),
            'bullets': p['bullets'],
            'is_me': is_me
        })

    last_play_public = None
    if room.get('last_play'):
        lp = room['last_play']
        last_play_public = {
            'player_id': lp['player_id'],
            'player_name': lp['player_name'],
            'count': len(lp['cards']),
            'declared_rank': lp['declared_rank'],
            'label': plural_rank(lp['declared_rank'], len(lp['cards']))
        }

    current_rank = RANKS[room.get('current_rank_index', 0)]
    return {
        'version': room.get('version', 0),
        'code': room['code'],
        'status': room['status'],
        'ammo_enabled': room['ammo_enabled'],
        'players': players_view,
        'my_hand': my_hand,
        'pile_count': len(room.get('pile', [])),
        'turn_index': room.get('turn_index', 0),
        'current_rank': current_rank,
        'current_rank_name': RANK_NAMES[current_rank],
        'last_play': last_play_public,
        'last_reveal': room.get('last_reveal'),
        'pending_winner_id': room.get('pending_winner_id'),
        'rankings': room.get('rankings', []),
        'banner_text': room.get('banner_text', ''),
        'chat_seq': room.get('chat_seq', 0),
        'chat': room.get('chat', [])[-25:]
    }


def state_response(room, player_id):
    return jsonify({'state': sanitize_room_for_player(room, player_id)})


def get_body():
    body = request.get_json(force=True, silent=True)
    return body if isinstance(body, dict) else {}


# ---------------------------------------------------------
# Error handling: always answer with JSON so the browser never
# chokes on an HTML error page ("Unexpected token <").
# ---------------------------------------------------------
@app.errorhandler(ApiError)
def handle_api_error(e):
    return jsonify({'error': e.message, 'retryable': e.retryable}), e.status


@app.errorhandler(HTTPException)
def handle_http_error(e):
    if request.path.startswith('/api/'):
        return jsonify({'error': e.description, 'retryable': False}), e.code
    return e


@app.errorhandler(Exception)
def handle_unexpected_error(e):
    app.logger.exception('Unhandled error')
    if request.path.startswith('/api/'):
        return jsonify({'error': 'Something went wrong on the server. Please retry.', 'retryable': True}), 500
    raise e


@app.after_request
def no_cache_for_api(resp):
    if request.path.startswith('/api/'):
        resp.headers['Cache-Control'] = 'no-store, max-age=0'
    return resp


# ---------------------------------------------------------
# Routes
# ---------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/health')
def health():
    if IS_SERVERLESS and not USE_MONGO:
        return jsonify({
            'ok': False,
            'storage': 'none',
            'message': 'Server storage is not set up: add MONGODB_URI in Vercel (Environment Variables), '
                       'then redeploy. Rooms cannot be shared until then.'
        })
    if USE_MONGO:
        try:
            _mongo_call(lambda c: c.database.command('ping'))
        except ApiError as e:
            return jsonify({'ok': False, 'storage': 'mongodb', 'message': e.message})
        return jsonify({'ok': True, 'storage': 'mongodb'})
    return jsonify({'ok': True, 'storage': 'local'})


@app.route('/api/create', methods=['POST'])
def create_room():
    body = get_body()
    name = clean_name(body.get('name'), 'somexmas05')
    ammo_enabled = bool(body.get('ammo_enabled', True))

    for _ in range(8):
        code = ''.join(random.choices('ABCDEFGHJKLMNPQRSTUVWXYZ23456789', k=5))
        player_id = str(uuid.uuid4())[:8]
        room = {
            'code': code,
            'version': 0,
            'status': 'waiting',
            'ammo_enabled': ammo_enabled,
            'players': [{
                'id': player_id,
                'name': name,
                'hand': [],
                'bullets': 3
            }],
            'pile': [],
            'turn_index': 0,
            'dealer_index': 2,
            'current_rank_index': 0,
            'last_play': None,
            'last_reveal': None,
            'pending_winner_id': None,
            'rankings': [],
            'banner_text': 'Waiting for other players... (1/3)',
            'chat': [],
            'chat_seq': 0,
            'seen': []
        }
        add_chat(room, 'System', f'{name} created table {code}. Share the code with 2 friends!')
        # expected_version=0 means "only create if this code is not taken yet"
        if save_room_cas(room, 0):
            return jsonify({'player_id': player_id, 'state': sanitize_room_for_player(room, player_id)})

    raise ApiError('Could not create a room right now. Please try again.', 503, retryable=True)


@app.route('/api/join', methods=['POST'])
def join_room():
    body = get_body()
    code = clean_code(body.get('code'))
    name = clean_name(body.get('name'), 'Player')
    known_id = body.get('player_id')
    request_id = body.get('request_id')
    result = {}

    def fn(room):
        # 1) Reconnect using the id this browser was given earlier
        if known_id:
            for p in room['players']:
                if p['id'] == known_id:
                    result['player_id'] = p['id']
                    return NO_CHANGE

        # 2) Table full: the only valid join is a reconnect by name
        if len(room['players']) >= MAX_PLAYERS:
            for p in room['players']:
                if p['name'].lower() == name.lower():
                    result['player_id'] = p['id']
                    return NO_CHANGE
            raise ApiError('This 3-player table is already full!')

        # 3) New seat. If the name is already taken, make it unique
        #    (two friends leaving the default name must not share one seat).
        taken = {p['name'].lower() for p in room['players']}
        final_name, n = name, 2
        while final_name.lower() in taken:
            suffix = str(n)
            final_name = name[:16 - len(suffix)] + suffix
            n += 1

        player_id = str(uuid.uuid4())[:8]
        room['players'].append({'id': player_id, 'name': final_name, 'hand': [], 'bullets': 3})
        result['player_id'] = player_id
        if request_id:
            room.setdefault('join_ids', {})[str(request_id)] = player_id

        count = len(room['players'])
        add_chat(room, 'System', f'{final_name} joined the table ({count}/3).')
        if count < MAX_PLAYERS:
            room['banner_text'] = f'Waiting for other players... ({count}/3)'
        else:
            deal_game(room)
            add_chat(room, 'System', 'All 3 players connected! Cards dealt — Aces first.')

    room = mutate_room(code, fn, request_id)
    if 'player_id' not in result:
        # The browser re-sent a join that already went through: hand back the same seat.
        previous = room.get('join_ids', {}).get(str(request_id))
        if not previous:
            raise ApiError('Could not join the table. Please try again.', 503, retryable=True)
        result['player_id'] = previous
    return jsonify({'player_id': result['player_id'], 'state': sanitize_room_for_player(room, result['player_id'])})


@app.route('/api/state/<code>')
def room_state(code):
    player_id = request.args.get('player_id', '')
    since = request.args.get('since', type=int)
    room = get_room(clean_code(code))
    if not room:
        raise ApiError('Room expired or not found.', 404)
    version = room.get('version', 0)
    if since is not None and since == version:
        return jsonify({'unchanged': True, 'version': version})
    return state_response(room, player_id)


@app.route('/api/play', methods=['POST'])
def play_cards():
    body = get_body()
    code = clean_code(body.get('code'))
    player_id = body.get('player_id')
    card_ids = body.get('card_ids') or []

    if not isinstance(card_ids, list) or not card_ids or len(card_ids) > 4:
        raise ApiError('Select between 1 and 4 cards.')

    def fn(room):
        if room['status'] != 'playing':
            raise ApiError('Game is not active.')

        turn_idx = room['turn_index']
        active_player = room['players'][turn_idx]
        if active_player['id'] != player_id:
            raise ApiError('Not your turn!')

        # If a previous player had 0 cards and nobody called BS before the next play, they win now
        if room.get('pending_winner_id') and room['pending_winner_id'] != player_id:
            finalize_winner(room, room['pending_winner_id'])
            return

        # Extract selected cards from player's hand
        selected_cards = []
        remaining_hand = []
        card_id_set = set(card_ids)
        for c in active_player['hand']:
            if c['id'] in card_id_set:
                selected_cards.append(c)
                card_id_set.remove(c['id'])
            else:
                remaining_hand.append(c)

        if len(selected_cards) != len(card_ids):
            raise ApiError('Invalid card selection.')

        declared_rank = RANKS[room['current_rank_index']]
        active_player['hand'] = remaining_hand
        room['pile'].extend(selected_cards)
        room['last_reveal'] = None
        room['last_play'] = {
            'player_id': active_player['id'],
            'player_name': active_player['name'],
            'cards': selected_cards,
            'declared_rank': declared_rank
        }

        # Check if player emptied their hand (must survive BS call!)
        if len(remaining_hand) == 0:
            room['pending_winner_id'] = active_player['id']
            room['banner_text'] = (
                f"{active_player['name']} played their last cards as "
                f"{plural_rank(declared_rank, len(selected_cards))}! Call Bullsh!t or let them win?"
            )
        else:
            room['pending_winner_id'] = None
            room['banner_text'] = (
                f"{active_player['name']} declared {plural_rank(declared_rank, len(selected_cards))}."
            )

        # Advance turn & rank clockwise among 3 players
        room['turn_index'] = (turn_idx + 1) % MAX_PLAYERS
        room['current_rank_index'] = (room['current_rank_index'] + 1) % len(RANKS)

    room = mutate_room(code, fn, body.get('request_id'))
    return state_response(room, player_id)


@app.route('/api/call_bs', methods=['POST'])
def call_bullshit():
    body = get_body()
    code = clean_code(body.get('code'))
    caller_id = body.get('player_id')

    def fn(room):
        if room['status'] != 'playing':
            raise ApiError('Game is not active.')

        last_play = room.get('last_play')
        if not last_play:
            raise ApiError('Too late — that play was already challenged.')

        if last_play['player_id'] == caller_id:
            raise ApiError('You cannot call Bullsh!t on yourself!')

        caller_idx = next((i for i, p in enumerate(room['players']) if p['id'] == caller_id), None)
        accused_idx = next((i for i, p in enumerate(room['players']) if p['id'] == last_play['player_id']), None)
        if caller_idx is None or accused_idx is None:
            raise ApiError('Player not found.')

        caller = room['players'][caller_idx]
        accused = room['players'][accused_idx]

        if room['ammo_enabled'] and caller['bullets'] <= 0:
            raise ApiError('Out of bullets! You cannot call Bullsh!t anymore.')

        # Check if accused lied
        declared_rank = last_play['declared_rank']
        was_truth = all(c['rank'] == declared_rank for c in last_play['cards'])
        lied = not was_truth

        # Spend 1 bullet first
        if room['ammo_enabled']:
            caller['bullets'] -= 1

        pile_cards = list(room['pile'])
        room['pile'] = []
        room['last_play'] = None

        if lied:
            # Caller caught the lie! Reload 1 bullet, liar takes entire pile, caller takes turn on SAME rank
            if room['ammo_enabled']:
                caller['bullets'] = min(3, caller['bullets'] + 1)
            accused['hand'] = sort_hand(accused['hand'] + pile_cards)
            room['pending_winner_id'] = None
            room['turn_index'] = caller_idx
            room['current_rank_index'] = RANKS.index(declared_rank)
            result_msg = (
                f"💥 BULLSH!T! {accused['name']} lied and took {len(pile_cards)} cards! "
                f"{caller['name']} reloaded & plays {RANK_NAMES[declared_rank]}s."
            )
        else:
            # Accused told the truth! Caller loses bullet permanently and takes entire pile
            caller['hand'] = sort_hand(caller['hand'] + pile_cards)
            result_msg = (
                f"❌ WRONG CALL! {accused['name']} told the truth! "
                f"{caller['name']} lost a bullet & took {len(pile_cards)} cards."
            )

        room['last_reveal'] = {
            'id': str(uuid.uuid4())[:6],
            'caller_name': caller['name'],
            'accused_name': accused['name'],
            'declared_rank': declared_rank,
            'cards': last_play['cards'],
            'lied': lied,
            'message': result_msg
        }

        # If the accused told the truth with their last cards, they win immediately
        if not lied and len(accused['hand']) == 0:
            finalize_winner(room, accused['id'])
            return

        room['banner_text'] = result_msg
        add_chat(room, 'Table', result_msg)

    room = mutate_room(code, fn, body.get('request_id'))
    return state_response(room, caller_id)


@app.route('/api/pass_win', methods=['POST'])
def pass_win():
    body = get_body()
    code = clean_code(body.get('code'))
    player_id = body.get('player_id')

    def fn(room):
        require_player(room, player_id)
        if room['status'] == 'finished':
            return NO_CHANGE  # another player already confirmed it
        if not room.get('pending_winner_id'):
            raise ApiError('No pending win to confirm.')
        if room['pending_winner_id'] == player_id:
            raise ApiError('Waiting for opponents to challenge or pass.')
        finalize_winner(room, room['pending_winner_id'])

    room = mutate_room(code, fn, body.get('request_id'))
    return state_response(room, player_id)


@app.route('/api/chat', methods=['POST'])
def send_chat():
    body = get_body()
    code = clean_code(body.get('code'))
    player_id = body.get('player_id')
    text = (body.get('text') or '').strip()[:140]
    if not text:
        raise ApiError('Invalid message.')

    def fn(room):
        player = require_player(room, player_id)
        add_chat(room, player['name'], text)

    room = mutate_room(code, fn, body.get('request_id'))
    return state_response(room, player_id)


@app.route('/api/restart', methods=['POST'])
def restart_game():
    body = get_body()
    code = clean_code(body.get('code'))
    player_id = body.get('player_id')

    def fn(room):
        require_player(room, player_id)
        if len(room['players']) < MAX_PLAYERS:
            raise ApiError('Need 3 players seated to start.')
        if room['status'] != 'finished':
            return NO_CHANGE  # someone else already pressed PLAY AGAIN
        deal_game(room)
        add_chat(room, 'System', 'New round dealt! Aces first.')

    room = mutate_room(code, fn, body.get('request_id'))
    return state_response(room, player_id)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
