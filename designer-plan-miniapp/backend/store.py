import csv
import hashlib
import hmac
import io
import json
import os
import secrets
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
DB_PATH = DATA_DIR / 'designer_plan.sqlite3'

DEFAULT_DESIGNER_SHARE = 0.15

DEFAULT_WORK: Dict[str, Any] = {
    'work_id': 'POJIANZHE-001',
    'name': '破茧者',
    'subtitle': '中国原创积木作品 · 首发款',
    'sale_mode': 'preorder',
    'crowdfunding_goal_amount': 50000,
    'crowdfunding_deadline': '',
    'crowdfunding_status': 'active',
    'cover_image': 'https://picsum.photos/seed/pojianzhe-cover/1200/720',
    'gallery_images': [
        'https://picsum.photos/seed/pojianzhe-g1/1200/900',
        'https://picsum.photos/seed/pojianzhe-g2/1200/900',
        'https://picsum.photos/seed/pojianzhe-g3/1200/900',
    ],
    'story': '以鹤为骨、梅为脉、山石为势，通过可拼搭结构表达“破茧而立”的成长意象。作品强调桌面雕塑感与观赏面完整度。',
    'specs': [
        {'label': '零件数', 'value': '约 980 pcs'},
        {'label': '成品尺寸', 'value': '约 26 x 26 x 33 cm'},
        {'label': '拼搭难度', 'value': '中高阶'},
        {'label': '拼搭时长', 'value': '6-8 小时'},
        {'label': '发货节奏', 'value': '预售后 15 天内'},
    ],
    'highlights': [
        '环形构图+鹤翼展开，主视角冲击力强',
        '梅枝与山体形成前后层次，适合静态陈列',
        '结构模块化，便于后续IP化扩展',
    ],
    'sku_list': [
        {
            'id': 'standard',
            'name': '标准版',
            'price': 499,
            'deposit': 99,
            'stock': 120,
            'perks': ['基础彩盒', '电子说明书', '售后补件支持'],
        },
        {
            'id': 'collector',
            'name': '收藏版',
            'price': 699,
            'deposit': 149,
            'stock': 60,
            'perks': ['限定编号卡', '设计师签名卡', '独立包装套封'],
        },
    ],
}

DEFAULT_ADMIN_SETTINGS: Dict[str, Any] = {
    'general': {
        'site_name': '酷玩潮原创设计师计划',
        'site_subtitle': 'MOC 孵化与发售平台',
        'contact_email': '',
        'contact_wechat': '',
        'announcement': '',
    },
    'api': {
        'api_base_url': 'http://127.0.0.1:8002',
        'media_base_url': 'http://127.0.0.1:8002/static',
        'wechat_login_enabled': True,
        'payment_mode': 'mock',
        'request_timeout_ms': 8000,
    },
}

ADMIN_PERMISSION_KEYS: List[str] = [
    'overview',
    'project',
    'order',
    'user',
    'submission',
    'designer',
    'feedback',
    'log',
    'setting',
]

DEFAULT_ADMIN_ROLE_DEFINITIONS: List[Dict[str, Any]] = [
    {
        'role_key': 'superadmin',
        'role_name': '超级管理员',
        'permissions': ADMIN_PERMISSION_KEYS,
        'is_system': 1,
    },
    {
        'role_key': 'operator',
        'role_name': '运营',
        'permissions': ['overview', 'project', 'order', 'user', 'designer', 'feedback', 'log', 'setting'],
        'is_system': 1,
    },
    {
        'role_key': 'finance',
        'role_name': '财务',
        'permissions': ['overview', 'designer', 'log'],
        'is_system': 1,
    },
    {
        'role_key': 'reviewer',
        'role_name': '审核',
        'permissions': ['overview', 'submission', 'feedback', 'log'],
        'is_system': 1,
    },
]

ORDER_STATUS_TEXT = {
    'pending_deposit': '待支付定金',
    'deposit_paid': '已付定金，待付尾款',
    'crowdfunding_pending': '待支付众筹支持',
    'crowdfunding_paid': '众筹支持成功',
    'crowdfunding_refunding': '众筹失败退款中',
    'crowdfunding_refunded': '众筹失败已退款',
    'crowdfunding_refund_failed': '众筹退款异常',
}

SALE_MODE_TEXT = {
    'preorder': '预售',
    'crowdfunding': '众筹',
}

SUBMISSION_STATUS_TEXT = {
    'pending': '待审核',
    'approved': '审核通过',
    'rejected': '需修改',
}

DESIGNER_STATUS_TEXT = {
    'active': '已开通',
    'paused': '暂停',
}

SETTLEMENT_STATUS_TEXT = {
    'pending': '未结算',
    'settled': '已结算',
}

FEEDBACK_STATUS_TEXT = {
    'pending': '待处理',
    'processing': '处理中',
    'resolved': '已解决',
    'rejected': '已驳回',
}

FEEDBACK_PRIORITY_TEXT = {
    'low': '低',
    'normal': '普通',
    'high': '高',
    'urgent': '紧急',
}

CROWDFUNDING_STATUS_TEXT = {
    'active': '众筹进行中',
    'producing': '众筹达标，生产中',
    'failed': '众筹失败，退款中',
}


def now_iso() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class Store:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.lock = threading.Lock()
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> List[str]:
        rows = conn.execute(f'PRAGMA table_info({table})').fetchall()
        return [str(r['name']) for r in rows]

    def _normalize_admin_permissions(self, permissions: Any) -> List[str]:
        source = permissions if isinstance(permissions, list) else []
        allowed = set(ADMIN_PERMISSION_KEYS)
        result: List[str] = []
        seen = set()
        for item in source:
            key = str(item or '').strip().lower()
            if not key or key in seen or key not in allowed:
                continue
            seen.add(key)
            result.append(key)
        return result

    def _hash_admin_password(self, password: str) -> str:
        safe_pwd = str(password or '')
        if len(safe_pwd) < 6:
            raise ValueError('管理员密码至少 6 位')
        iterations = 120000
        salt = secrets.token_hex(16)
        digest = hashlib.pbkdf2_hmac('sha256', safe_pwd.encode('utf-8'), salt.encode('utf-8'), iterations).hex()
        return f'pbkdf2_sha256${iterations}${salt}${digest}'

    def _verify_admin_password(self, password: str, password_hash: str) -> bool:
        safe_hash = str(password_hash or '').strip()
        safe_pwd = str(password or '')
        parts = safe_hash.split('$')
        if len(parts) != 4 or parts[0] != 'pbkdf2_sha256':
            return False
        try:
            iterations = int(parts[1])
        except Exception:
            return False
        salt = parts[2]
        expected = parts[3]
        candidate = hashlib.pbkdf2_hmac('sha256', safe_pwd.encode('utf-8'), salt.encode('utf-8'), iterations).hex()
        return hmac.compare_digest(expected, candidate)

    def _serialize_admin_role_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        try:
            parsed = json.loads(row['permissions_json'] or '[]')
            perms = self._normalize_admin_permissions(parsed)
        except Exception:
            perms = []
        return {
            'role_key': row['role_key'],
            'role_name': row['role_name'] or row['role_key'],
            'permissions': perms,
            'is_system': bool(int(row['is_system'] or 0)),
            'created_at': row['created_at'] or '',
            'updated_at': row['updated_at'] or '',
        }

    def _serialize_admin_user_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        try:
            parsed = json.loads(row['permissions_json'] or '[]')
            perms = self._normalize_admin_permissions(parsed)
        except Exception:
            perms = []
        status = str(row['status'] or 'active')
        return {
            'admin_id': int(row['id']),
            'username': row['username'] or '',
            'display_name': row['display_name'] or row['username'] or '',
            'status': status,
            'status_text': '启用' if status == 'active' else '停用',
            'role_key': row['role_key'] or '',
            'role_name': row['role_name'] or row['role_key'] or '',
            'permissions': perms,
            'last_login_at': row['last_login_at'] or '',
            'created_at': row['created_at'] or '',
            'updated_at': row['updated_at'] or '',
        }

    def _seed_admin_roles(self, conn: sqlite3.Connection) -> None:
        ts = now_iso()
        for item in DEFAULT_ADMIN_ROLE_DEFINITIONS:
            role_key = str(item.get('role_key') or '').strip().lower()
            if not role_key:
                continue
            role_name = str(item.get('role_name') or role_key).strip() or role_key
            perms = self._normalize_admin_permissions(item.get('permissions') or [])
            is_system = 1 if int(item.get('is_system') or 0) else 0
            conn.execute(
                '''
                INSERT INTO admin_roles(role_key, role_name, permissions_json, is_system, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(role_key) DO UPDATE SET
                  role_name=CASE WHEN admin_roles.is_system=1 THEN excluded.role_name ELSE admin_roles.role_name END,
                  permissions_json=CASE WHEN admin_roles.is_system=1 THEN excluded.permissions_json ELSE admin_roles.permissions_json END,
                  is_system=CASE WHEN admin_roles.is_system=1 THEN 1 ELSE admin_roles.is_system END,
                  updated_at=CASE WHEN admin_roles.is_system=1 THEN excluded.updated_at ELSE admin_roles.updated_at END
                ''',
                (role_key, role_name, json.dumps(perms, ensure_ascii=False), is_system, ts, ts),
            )

    def _seed_default_admin_user(self, conn: sqlite3.Connection) -> None:
        count_row = conn.execute('SELECT COUNT(1) AS c FROM admin_users').fetchone()
        if int(count_row['c'] or 0) > 0:
            return
        username = (os.getenv('ADMIN_USERNAME', 'admin').strip() or 'admin')[:32]
        password = os.getenv('ADMIN_PASSWORD', 'admin123456').strip() or 'admin123456'
        display_name = (os.getenv('ADMIN_DISPLAY_NAME', '系统管理员').strip() or '系统管理员')[:40]
        if len(password) < 6:
            password = 'admin123456'
        ts = now_iso()
        conn.execute(
            '''
            INSERT INTO admin_users(username, password_hash, role_key, display_name, status, last_login_at, created_at, updated_at)
            VALUES(?, ?, 'superadmin', ?, 'active', '', ?, ?)
            ''',
            (username, self._hash_admin_password(password), display_name, ts, ts),
        )

    def get_admin_identity_by_session(self, session_token: str) -> Optional[Dict[str, Any]]:
        token = str(session_token or '').strip()
        if not token:
            return None
        now = now_iso()
        with self.lock, self._conn() as conn:
            conn.execute('DELETE FROM admin_sessions WHERE expires_at<=?', (now,))
            row = conn.execute(
                '''
                SELECT
                  u.*,
                  r.role_name,
                  r.permissions_json,
                  s.session_token,
                  s.expires_at
                FROM admin_sessions s
                JOIN admin_users u ON u.id = s.admin_user_id
                LEFT JOIN admin_roles r ON r.role_key = u.role_key
                WHERE s.session_token=?
                  AND s.expires_at>?
                  AND u.status='active'
                LIMIT 1
                ''',
                (token, now),
            ).fetchone()
            if not row:
                conn.commit()
                return None
            conn.execute('UPDATE admin_sessions SET updated_at=? WHERE session_token=?', (now, token))
            conn.commit()
        base = self._serialize_admin_user_row(row)
        base['session_token'] = token
        base['session_expires_at'] = row['expires_at'] or ''
        return base

    def admin_logout(self, session_token: str) -> None:
        token = str(session_token or '').strip()
        if not token:
            return
        with self.lock, self._conn() as conn:
            conn.execute('DELETE FROM admin_sessions WHERE session_token=?', (token,))
            conn.commit()

    def admin_login(self, username: str, password: str, session_hours: int = 12) -> Dict[str, Any]:
        safe_username = str(username or '').strip()
        safe_password = str(password or '')
        if not safe_username or not safe_password:
            raise ValueError('请输入用户名和密码')
        ttl_hours = max(1, min(int(session_hours or 12), 168))
        now = datetime.now()
        now_text = now.strftime('%Y-%m-%d %H:%M:%S')
        expires = (now + timedelta(hours=ttl_hours)).strftime('%Y-%m-%d %H:%M:%S')

        with self.lock, self._conn() as conn:
            conn.execute('DELETE FROM admin_sessions WHERE expires_at<=?', (now_text,))
            row = conn.execute(
                '''
                SELECT
                  u.*,
                  r.role_name,
                  r.permissions_json
                FROM admin_users u
                LEFT JOIN admin_roles r ON r.role_key = u.role_key
                WHERE u.username=?
                LIMIT 1
                ''',
                (safe_username,),
            ).fetchone()
            if not row or not self._verify_admin_password(safe_password, row['password_hash'] or ''):
                conn.commit()
                raise ValueError('用户名或密码错误')
            if str(row['status'] or 'active') != 'active':
                conn.commit()
                raise ValueError('账号已停用，请联系管理员')
            if not str(row['role_key'] or '').strip():
                conn.commit()
                raise ValueError('账号角色无效，请联系管理员')
            session_token = secrets.token_hex(24)
            conn.execute(
                '''
                INSERT INTO admin_sessions(session_token, admin_user_id, expires_at, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?)
                ''',
                (session_token, int(row['id']), expires, now_text, now_text),
            )
            conn.execute(
                'UPDATE admin_users SET last_login_at=?, updated_at=? WHERE id=?',
                (now_text, now_text, int(row['id'])),
            )
            conn.commit()
        admin = self.get_admin_identity_by_session(session_token)
        if not admin:
            raise ValueError('登录失败，请稍后重试')
        return {'session_token': session_token, 'expires_at': expires, 'admin': admin}

    def list_admin_roles(self) -> List[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT *
                FROM admin_roles
                ORDER BY is_system DESC, role_key ASC
                '''
            ).fetchall()
        return [self._serialize_admin_role_row(row) for row in rows]

    def create_admin_role(self, role_key: str, role_name: str, permissions: List[str]) -> Dict[str, Any]:
        key = str(role_key or '').strip().lower()
        if not key:
            raise ValueError('岗位标识不能为空')
        if len(key) > 32:
            raise ValueError('岗位标识不能超过 32 字符')
        if any(not (ch.isalnum() or ch in {'_', '-'}) for ch in key):
            raise ValueError('岗位标识仅支持字母、数字、_、-')
        name = str(role_name or '').strip()
        if not name:
            raise ValueError('岗位名称不能为空')
        perms = self._normalize_admin_permissions(permissions or [])
        if not perms:
            raise ValueError('请至少选择 1 项权限')
        ts = now_iso()
        with self.lock, self._conn() as conn:
            exists = conn.execute('SELECT role_key FROM admin_roles WHERE role_key=? LIMIT 1', (key,)).fetchone()
            if exists:
                raise ValueError('岗位标识已存在')
            conn.execute(
                '''
                INSERT INTO admin_roles(role_key, role_name, permissions_json, is_system, created_at, updated_at)
                VALUES(?, ?, ?, 0, ?, ?)
                ''',
                (key, name[:40], json.dumps(perms, ensure_ascii=False), ts, ts),
            )
            row = conn.execute('SELECT * FROM admin_roles WHERE role_key=? LIMIT 1', (key,)).fetchone()
            conn.commit()
        return self._serialize_admin_role_row(row)

    def update_admin_role(self, role_key: str, role_name: Optional[str] = None, permissions: Optional[List[str]] = None) -> Dict[str, Any]:
        key = str(role_key or '').strip().lower()
        if not key:
            raise ValueError('岗位标识不能为空')
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM admin_roles WHERE role_key=? LIMIT 1', (key,)).fetchone()
            if not row:
                raise ValueError('岗位不存在')
            next_name = str(row['role_name'] or key)
            if role_name is not None:
                next_name = str(role_name or '').strip()
            if not next_name:
                raise ValueError('岗位名称不能为空')
            try:
                current_raw = json.loads(row['permissions_json'] or '[]')
            except Exception:
                current_raw = []
            current_perms = self._normalize_admin_permissions(current_raw)
            next_perms = current_perms if permissions is None else self._normalize_admin_permissions(permissions)
            if key == 'superadmin':
                next_perms = list(ADMIN_PERMISSION_KEYS)
            if not next_perms:
                raise ValueError('请至少选择 1 项权限')
            conn.execute(
                '''
                UPDATE admin_roles
                SET role_name=?, permissions_json=?, updated_at=?
                WHERE role_key=?
                ''',
                (next_name[:40], json.dumps(next_perms, ensure_ascii=False), now_iso(), key),
            )
            refreshed = conn.execute('SELECT * FROM admin_roles WHERE role_key=? LIMIT 1', (key,)).fetchone()
            conn.commit()
        return self._serialize_admin_role_row(refreshed)

    def list_admin_users(self, limit: int = 500) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 500), 2000))
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT
                  u.*,
                  COALESCE(r.role_name, u.role_key) AS role_name,
                  COALESCE(r.permissions_json, '[]') AS permissions_json
                FROM admin_users u
                LEFT JOIN admin_roles r ON r.role_key = u.role_key
                ORDER BY u.updated_at DESC, u.id DESC
                LIMIT ?
                ''',
                (safe_limit,),
            ).fetchall()
        return [self._serialize_admin_user_row(row) for row in rows]

    def create_admin_user(
        self,
        username: str,
        password: str,
        role_key: str,
        display_name: str = '',
        status: str = 'active',
    ) -> Dict[str, Any]:
        safe_username = str(username or '').strip()
        if len(safe_username) < 3:
            raise ValueError('用户名至少 3 位')
        if len(safe_username) > 32:
            raise ValueError('用户名不能超过 32 位')
        if any(not (ch.isalnum() or ch in {'_', '-', '.'}) for ch in safe_username):
            raise ValueError('用户名仅支持字母、数字、_、-、.')
        safe_role_key = str(role_key or '').strip().lower()
        if not safe_role_key:
            raise ValueError('岗位不能为空')
        safe_display_name = str(display_name or '').strip()[:40]
        safe_status = str(status or 'active').strip().lower()
        if safe_status not in {'active', 'disabled'}:
            safe_status = 'active'
        password_hash = self._hash_admin_password(password)
        ts = now_iso()
        with self.lock, self._conn() as conn:
            role = conn.execute('SELECT role_key FROM admin_roles WHERE role_key=? LIMIT 1', (safe_role_key,)).fetchone()
            if not role:
                raise ValueError('岗位不存在')
            exists = conn.execute('SELECT id FROM admin_users WHERE username=? LIMIT 1', (safe_username,)).fetchone()
            if exists:
                raise ValueError('用户名已存在')
            conn.execute(
                '''
                INSERT INTO admin_users(username, password_hash, role_key, display_name, status, last_login_at, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, '', ?, ?)
                ''',
                (safe_username, password_hash, safe_role_key, safe_display_name or safe_username, safe_status, ts, ts),
            )
            row = conn.execute(
                '''
                SELECT
                  u.*,
                  COALESCE(r.role_name, u.role_key) AS role_name,
                  COALESCE(r.permissions_json, '[]') AS permissions_json
                FROM admin_users u
                LEFT JOIN admin_roles r ON r.role_key = u.role_key
                WHERE u.username=?
                LIMIT 1
                ''',
                (safe_username,),
            ).fetchone()
            conn.commit()
        return self._serialize_admin_user_row(row)

    def update_admin_user(
        self,
        admin_id: int,
        display_name: Optional[str] = None,
        role_key: Optional[str] = None,
        status: Optional[str] = None,
        password: Optional[str] = None,
    ) -> Dict[str, Any]:
        safe_id = int(admin_id or 0)
        if safe_id <= 0:
            raise ValueError('管理员ID不合法')
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM admin_users WHERE id=? LIMIT 1', (safe_id,)).fetchone()
            if not row:
                raise ValueError('管理员不存在')
            next_display_name = str(row['display_name'] or row['username'] or '')
            if display_name is not None:
                next_display_name = str(display_name or '').strip()[:40] or str(row['username'] or '')

            next_role_key = str(row['role_key'] or '').strip().lower()
            if role_key is not None:
                next_role_key = str(role_key or '').strip().lower()
            if not next_role_key:
                raise ValueError('岗位不能为空')
            role = conn.execute('SELECT role_key FROM admin_roles WHERE role_key=? LIMIT 1', (next_role_key,)).fetchone()
            if not role:
                raise ValueError('岗位不存在')

            next_status = str(row['status'] or 'active').strip().lower()
            if status is not None:
                next_status = str(status or '').strip().lower()
            if next_status not in {'active', 'disabled'}:
                raise ValueError('状态仅支持 active/disabled')

            next_password_hash = str(row['password_hash'] or '')
            if password is not None and str(password).strip():
                next_password_hash = self._hash_admin_password(str(password))

            conn.execute(
                '''
                UPDATE admin_users
                SET password_hash=?, role_key=?, display_name=?, status=?, updated_at=?
                WHERE id=?
                ''',
                (next_password_hash, next_role_key, next_display_name, next_status, now_iso(), safe_id),
            )
            if next_status != 'active':
                conn.execute('DELETE FROM admin_sessions WHERE admin_user_id=?', (safe_id,))
            refreshed = conn.execute(
                '''
                SELECT
                  u.*,
                  COALESCE(r.role_name, u.role_key) AS role_name,
                  COALESCE(r.permissions_json, '[]') AS permissions_json
                FROM admin_users u
                LEFT JOIN admin_roles r ON r.role_key = u.role_key
                WHERE u.id=?
                LIMIT 1
                ''',
                (safe_id,),
            ).fetchone()
            conn.commit()
        return self._serialize_admin_user_row(refreshed)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        work_cols = self._table_columns(conn, 'works')
        if 'sale_mode' not in work_cols:
            conn.execute("ALTER TABLE works ADD COLUMN sale_mode TEXT NOT NULL DEFAULT 'preorder'")
        if 'crowdfunding_goal_amount' not in work_cols:
            conn.execute("ALTER TABLE works ADD COLUMN crowdfunding_goal_amount INTEGER NOT NULL DEFAULT 0")
        if 'crowdfunding_deadline' not in work_cols:
            conn.execute("ALTER TABLE works ADD COLUMN crowdfunding_deadline TEXT NOT NULL DEFAULT ''")
        if 'crowdfunding_status' not in work_cols:
            conn.execute("ALTER TABLE works ADD COLUMN crowdfunding_status TEXT NOT NULL DEFAULT 'active'")
        if 'cover_image' not in work_cols:
            conn.execute("ALTER TABLE works ADD COLUMN cover_image TEXT NOT NULL DEFAULT ''")
        if 'gallery_json' not in work_cols:
            conn.execute("ALTER TABLE works ADD COLUMN gallery_json TEXT NOT NULL DEFAULT '[]'")
        conn.execute(
            "UPDATE works SET cover_image=? WHERE COALESCE(cover_image, '')=''",
            (str(DEFAULT_WORK.get('cover_image') or ''),),
        )
        conn.execute(
            "UPDATE works SET gallery_json=? WHERE COALESCE(gallery_json, '')=''",
            (json.dumps(DEFAULT_WORK.get('gallery_images') or [], ensure_ascii=False),),
        )
        conn.execute(
            '''
            UPDATE works
            SET crowdfunding_status =
              CASE
                WHEN sale_mode='crowdfunding' THEN COALESCE(NULLIF(crowdfunding_status, ''), 'active')
                ELSE 'active'
              END
            '''
        )

        order_cols = self._table_columns(conn, 'orders')
        if 'sale_mode' not in order_cols:
            conn.execute("ALTER TABLE orders ADD COLUMN sale_mode TEXT NOT NULL DEFAULT 'preorder'")
        if 'paid_amount' not in order_cols:
            conn.execute("ALTER TABLE orders ADD COLUMN paid_amount INTEGER NOT NULL DEFAULT 0")
        if 'refund_status' not in order_cols:
            conn.execute("ALTER TABLE orders ADD COLUMN refund_status TEXT NOT NULL DEFAULT 'none'")
        if 'refunded_at' not in order_cols:
            conn.execute("ALTER TABLE orders ADD COLUMN refunded_at TEXT NOT NULL DEFAULT ''")
        if 'refund_amount' not in order_cols:
            conn.execute("ALTER TABLE orders ADD COLUMN refund_amount INTEGER NOT NULL DEFAULT 0")
        if 'refund_reason' not in order_cols:
            conn.execute("ALTER TABLE orders ADD COLUMN refund_reason TEXT NOT NULL DEFAULT ''")
        if 'admin_note' not in order_cols:
            conn.execute("ALTER TABLE orders ADD COLUMN admin_note TEXT NOT NULL DEFAULT ''")
        conn.execute(
            '''
            UPDATE orders
            SET paid_amount =
              CASE
                WHEN COALESCE(paid_amount, 0) > 0 THEN paid_amount
                WHEN COALESCE(sale_mode, 'preorder') = 'crowdfunding' THEN total_amount
                ELSE deposit_amount
              END
            '''
        )
        conn.execute(
            '''
            UPDATE orders
            SET refund_status='refunded',
                refund_amount=CASE WHEN refund_amount > 0 THEN refund_amount ELSE paid_amount END
            WHERE pay_status='refunded'
              AND COALESCE(refund_status, 'none')='none'
            '''
        )

        # 兼容旧版 designer_updates（无 work_id）
        update_cols = self._table_columns(conn, 'designer_updates')
        if 'work_id' not in update_cols:
            conn.execute("ALTER TABLE designer_updates ADD COLUMN work_id TEXT NOT NULL DEFAULT ''")

        # 兼容旧版 designers（可能缺 bio/default_share_ratio/avatar_url）
        designer_cols = self._table_columns(conn, 'designers')
        if 'bio' not in designer_cols:
            conn.execute("ALTER TABLE designers ADD COLUMN bio TEXT NOT NULL DEFAULT ''")
        if 'default_share_ratio' not in designer_cols:
            conn.execute(f'ALTER TABLE designers ADD COLUMN default_share_ratio REAL NOT NULL DEFAULT {DEFAULT_DESIGNER_SHARE}')
        if 'avatar_url' not in designer_cols:
            conn.execute("ALTER TABLE designers ADD COLUMN avatar_url TEXT NOT NULL DEFAULT ''")

        feedback_cols = self._table_columns(conn, 'user_feedbacks')
        if feedback_cols and 'priority' not in feedback_cols:
            conn.execute("ALTER TABLE user_feedbacks ADD COLUMN priority TEXT NOT NULL DEFAULT 'normal'")
        if feedback_cols and 'images_json' not in feedback_cols:
            conn.execute("ALTER TABLE user_feedbacks ADD COLUMN images_json TEXT NOT NULL DEFAULT '[]'")
        if feedback_cols and 'reply_operator' not in feedback_cols:
            conn.execute("ALTER TABLE user_feedbacks ADD COLUMN reply_operator TEXT NOT NULL DEFAULT ''")
        if feedback_cols and 'replied_at' not in feedback_cols:
            conn.execute("ALTER TABLE user_feedbacks ADD COLUMN replied_at TEXT NOT NULL DEFAULT ''")

        template_cols = self._table_columns(conn, 'feedback_reply_templates')
        if template_cols and 'is_active' not in template_cols:
            conn.execute("ALTER TABLE feedback_reply_templates ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")

        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS project_comments (
              comment_id TEXT PRIMARY KEY,
              work_id TEXT NOT NULL,
              user_id INTEGER NOT NULL,
              content TEXT NOT NULL,
              designer_reply TEXT NOT NULL DEFAULT '',
              reply_designer_id INTEGER NOT NULL DEFAULT 0,
              reply_at TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_project_comments_work_created
              ON project_comments(work_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_project_comments_user_created
              ON project_comments(user_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_project_comments_reply_designer
              ON project_comments(reply_designer_id, reply_at DESC);
            '''
        )
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS app_settings (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              setting_key TEXT NOT NULL UNIQUE,
              setting_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            '''
        )
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS admin_roles (
              role_key TEXT PRIMARY KEY,
              role_name TEXT NOT NULL,
              permissions_json TEXT NOT NULL DEFAULT '[]',
              is_system INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS admin_users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT NOT NULL UNIQUE,
              password_hash TEXT NOT NULL,
              role_key TEXT NOT NULL,
              display_name TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'active',
              last_login_at TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS admin_sessions (
              session_token TEXT PRIMARY KEY,
              admin_user_id INTEGER NOT NULL,
              expires_at TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            '''
        )

    def _init_db(self) -> None:
        with self.lock, self._conn() as conn:
            conn.executescript(
                '''
                CREATE TABLE IF NOT EXISTS users (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  openid TEXT NOT NULL UNIQUE,
                  nickname TEXT NOT NULL DEFAULT '',
                  session_token TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS works (
                  work_id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  subtitle TEXT NOT NULL,
                  sale_mode TEXT NOT NULL DEFAULT 'preorder',
                  crowdfunding_goal_amount INTEGER NOT NULL DEFAULT 0,
                  crowdfunding_deadline TEXT NOT NULL DEFAULT '',
                  crowdfunding_status TEXT NOT NULL DEFAULT 'active',
                  cover_image TEXT NOT NULL DEFAULT '',
                  gallery_json TEXT NOT NULL DEFAULT '[]',
                  story TEXT NOT NULL,
                  specs_json TEXT NOT NULL,
                  highlights_json TEXT NOT NULL,
                  sku_json TEXT NOT NULL,
                  is_current INTEGER NOT NULL DEFAULT 0,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS reservations (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  work_id TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  UNIQUE(user_id, work_id)
                );

                CREATE TABLE IF NOT EXISTS orders (
                  order_id TEXT PRIMARY KEY,
                  user_id INTEGER NOT NULL,
                  work_id TEXT NOT NULL,
                  work_name TEXT NOT NULL,
                  sku_id TEXT NOT NULL,
                  sku_name TEXT NOT NULL,
                  quantity INTEGER NOT NULL,
                  unit_price INTEGER NOT NULL,
                  deposit_price INTEGER NOT NULL,
                  total_amount INTEGER NOT NULL,
                  deposit_amount INTEGER NOT NULL,
                  paid_amount INTEGER NOT NULL DEFAULT 0,
                  sale_mode TEXT NOT NULL DEFAULT 'preorder',
                  pay_status TEXT NOT NULL,
                  order_status TEXT NOT NULL,
                  refund_status TEXT NOT NULL DEFAULT 'none',
                  refunded_at TEXT NOT NULL DEFAULT '',
                  refund_amount INTEGER NOT NULL DEFAULT 0,
                  refund_reason TEXT NOT NULL DEFAULT '',
                  admin_note TEXT NOT NULL DEFAULT '',
                  payment_channel TEXT NOT NULL DEFAULT '',
                  transaction_id TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  paid_at TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS submissions (
                  submission_id TEXT PRIMARY KEY,
                  user_id INTEGER NOT NULL,
                  designer_name TEXT NOT NULL,
                  contact TEXT NOT NULL,
                  work_name TEXT NOT NULL,
                  category TEXT NOT NULL,
                  intro TEXT NOT NULL,
                  estimated_pieces INTEGER NOT NULL,
                  image_urls_json TEXT NOT NULL,
                  status TEXT NOT NULL,
                  review_note TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS payment_logs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  order_id TEXT NOT NULL,
                  mode TEXT NOT NULL,
                  payload_json TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS admin_action_logs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  actor TEXT NOT NULL DEFAULT '',
                  action_type TEXT NOT NULL,
                  target_type TEXT NOT NULL,
                  target_id TEXT NOT NULL,
                  related_user_id INTEGER NOT NULL DEFAULT 0,
                  detail_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS designers (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL UNIQUE,
                  display_name TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT 'active',
                  default_share_ratio REAL NOT NULL DEFAULT 0.15,
                  bio TEXT NOT NULL DEFAULT '',
                  avatar_url TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS designer_work_links (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  designer_id INTEGER NOT NULL,
                  work_id TEXT NOT NULL,
                  share_ratio REAL NOT NULL DEFAULT 0.15,
                  is_active INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(designer_id, work_id)
                );

                CREATE TABLE IF NOT EXISTS designer_updates (
                  update_id TEXT PRIMARY KEY,
                  designer_id INTEGER NOT NULL,
                  work_id TEXT NOT NULL,
                  title TEXT NOT NULL,
                  content TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS project_comments (
                  comment_id TEXT PRIMARY KEY,
                  work_id TEXT NOT NULL,
                  user_id INTEGER NOT NULL,
                  content TEXT NOT NULL,
                  designer_reply TEXT NOT NULL DEFAULT '',
                  reply_designer_id INTEGER NOT NULL DEFAULT 0,
                  reply_at TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS designer_commission_records (
                  record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                  designer_id INTEGER NOT NULL,
                  order_id TEXT NOT NULL,
                  work_id TEXT NOT NULL,
                  share_ratio REAL NOT NULL,
                  commission_amount REAL NOT NULL,
                  settlement_status TEXT NOT NULL DEFAULT 'pending',
                  settlement_note TEXT NOT NULL DEFAULT '',
                  settled_at TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(designer_id, order_id)
                );

                CREATE TABLE IF NOT EXISTS user_feedbacks (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  category TEXT NOT NULL DEFAULT 'general',
                  priority TEXT NOT NULL DEFAULT 'normal',
                  content TEXT NOT NULL,
                  images_json TEXT NOT NULL DEFAULT '[]',
                  contact TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT 'pending',
                  admin_reply TEXT NOT NULL DEFAULT '',
                  reply_operator TEXT NOT NULL DEFAULT '',
                  replied_at TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS feedback_reply_templates (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  code TEXT NOT NULL UNIQUE,
                  title TEXT NOT NULL,
                  content TEXT NOT NULL,
                  is_active INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS app_settings (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  setting_key TEXT NOT NULL UNIQUE,
                  setting_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS admin_roles (
                  role_key TEXT PRIMARY KEY,
                  role_name TEXT NOT NULL,
                  permissions_json TEXT NOT NULL DEFAULT '[]',
                  is_system INTEGER NOT NULL DEFAULT 0,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS admin_users (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  username TEXT NOT NULL UNIQUE,
                  password_hash TEXT NOT NULL,
                  role_key TEXT NOT NULL,
                  display_name TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT 'active',
                  last_login_at TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS admin_sessions (
                  session_token TEXT PRIMARY KEY,
                  admin_user_id INTEGER NOT NULL,
                  expires_at TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                '''
            )
            self._migrate_schema(conn)
            conn.executescript(
                '''
                CREATE INDEX IF NOT EXISTS idx_orders_user_created ON orders(user_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_submissions_user_created ON submissions(user_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_designer_links_designer ON designer_work_links(designer_id, is_active);
                CREATE INDEX IF NOT EXISTS idx_designer_updates_designer ON designer_updates(designer_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_designer_updates_work ON designer_updates(work_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_project_comments_work_created ON project_comments(work_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_project_comments_user_created ON project_comments(user_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_project_comments_reply_designer ON project_comments(reply_designer_id, reply_at DESC);
                CREATE INDEX IF NOT EXISTS idx_commission_designer ON designer_commission_records(designer_id, settlement_status, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_commission_settlement ON designer_commission_records(settlement_status, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_admin_action_user ON admin_action_logs(related_user_id, id DESC);
                CREATE INDEX IF NOT EXISTS idx_admin_action_target ON admin_action_logs(target_type, target_id, id DESC);
                CREATE INDEX IF NOT EXISTS idx_admin_action_actor_time ON admin_action_logs(actor, created_at DESC, id DESC);
                CREATE INDEX IF NOT EXISTS idx_admin_action_time ON admin_action_logs(created_at DESC, id DESC);
                CREATE INDEX IF NOT EXISTS idx_feedback_user_created ON user_feedbacks(user_id, id DESC);
                CREATE INDEX IF NOT EXISTS idx_feedback_status_id ON user_feedbacks(status, id DESC);
                CREATE INDEX IF NOT EXISTS idx_feedback_priority_id ON user_feedbacks(priority, id DESC);
                CREATE INDEX IF NOT EXISTS idx_feedback_tpl_active ON feedback_reply_templates(is_active, id DESC);
                CREATE INDEX IF NOT EXISTS idx_admin_users_role ON admin_users(role_key, status, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_admin_sessions_user ON admin_sessions(admin_user_id, expires_at DESC);
                '''
            )
            self._seed_admin_roles(conn)
            self._seed_default_admin_user(conn)
            conn.commit()

        self.seed_default_work()

    def seed_default_work(self) -> None:
        with self.lock, self._conn() as conn:
            existing = conn.execute('SELECT work_id FROM works WHERE is_current=1 LIMIT 1').fetchone()
            if existing:
                return
            payload = DEFAULT_WORK
            conn.execute(
                '''
                INSERT OR REPLACE INTO works(
                  work_id, name, subtitle, sale_mode, crowdfunding_goal_amount, crowdfunding_deadline,
                  crowdfunding_status, cover_image, gallery_json, story, specs_json, highlights_json, sku_json, is_current, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                ''',
                (
                    payload['work_id'],
                    payload['name'],
                    payload['subtitle'],
                    payload.get('sale_mode') or 'preorder',
                    int(payload.get('crowdfunding_goal_amount') or 0),
                    str(payload.get('crowdfunding_deadline') or ''),
                    str(payload.get('crowdfunding_status') or 'active'),
                    str(payload.get('cover_image') or ''),
                    json.dumps(payload.get('gallery_images') or [], ensure_ascii=False),
                    payload['story'],
                    json.dumps(payload['specs'], ensure_ascii=False),
                    json.dumps(payload['highlights'], ensure_ascii=False),
                    json.dumps(payload['sku_list'], ensure_ascii=False),
                    now_iso(),
                ),
            )
            conn.commit()

    def _normalize_sale_mode(self, raw: Any) -> str:
        mode = str(raw or 'preorder').strip().lower()
        return mode if mode in {'preorder', 'crowdfunding'} else 'preorder'

    def _normalize_crowdfunding_status(self, raw: Any) -> str:
        status = str(raw or 'active').strip().lower()
        return status if status in {'active', 'producing', 'failed'} else 'active'

    def _normalize_admin_settings_payload(
        self,
        payload: Dict[str, Any],
        fallback: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        base = fallback or DEFAULT_ADMIN_SETTINGS
        base_general = base.get('general') or {}
        base_api = base.get('api') or {}
        incoming_general = payload.get('general') if isinstance(payload.get('general'), dict) else {}
        incoming_api = payload.get('api') if isinstance(payload.get('api'), dict) else {}

        site_name = str(incoming_general.get('site_name', base_general.get('site_name', ''))).strip()
        site_subtitle = str(incoming_general.get('site_subtitle', base_general.get('site_subtitle', ''))).strip()
        contact_email = str(incoming_general.get('contact_email', base_general.get('contact_email', ''))).strip()
        contact_wechat = str(incoming_general.get('contact_wechat', base_general.get('contact_wechat', ''))).strip()
        announcement = str(incoming_general.get('announcement', base_general.get('announcement', ''))).strip()
        if not site_name:
            raise ValueError('通用设置：站点名称不能为空')

        api_base_url = str(incoming_api.get('api_base_url', base_api.get('api_base_url', ''))).strip()
        media_base_url = str(incoming_api.get('media_base_url', base_api.get('media_base_url', ''))).strip()
        payment_mode = str(incoming_api.get('payment_mode', base_api.get('payment_mode', 'mock'))).strip().lower()
        if payment_mode not in {'mock', 'wechat'}:
            payment_mode = 'mock'
        timeout_raw = incoming_api.get('request_timeout_ms', base_api.get('request_timeout_ms', 8000))
        try:
            request_timeout_ms = int(timeout_raw or 8000)
        except Exception:
            request_timeout_ms = 8000
        request_timeout_ms = max(1000, min(request_timeout_ms, 60000))
        wechat_login_enabled = bool(incoming_api.get('wechat_login_enabled', base_api.get('wechat_login_enabled', True)))

        return {
            'general': {
                'site_name': site_name[:80],
                'site_subtitle': site_subtitle[:120],
                'contact_email': contact_email[:120],
                'contact_wechat': contact_wechat[:120],
                'announcement': announcement[:500],
            },
            'api': {
                'api_base_url': api_base_url[:200],
                'media_base_url': media_base_url[:200],
                'wechat_login_enabled': wechat_login_enabled,
                'payment_mode': payment_mode,
                'request_timeout_ms': request_timeout_ms,
            },
        }

    def get_admin_settings(self) -> Dict[str, Any]:
        with self.lock, self._conn() as conn:
            row = conn.execute(
                "SELECT setting_json, updated_at FROM app_settings WHERE setting_key='global' LIMIT 1"
            ).fetchone()
            if not row:
                normalized = self._normalize_admin_settings_payload(DEFAULT_ADMIN_SETTINGS)
                ts = now_iso()
                conn.execute(
                    '''
                    INSERT INTO app_settings(setting_key, setting_json, created_at, updated_at)
                    VALUES('global', ?, ?, ?)
                    ''',
                    (json.dumps(normalized, ensure_ascii=False), ts, ts),
                )
                conn.commit()
                return {'settings': normalized, 'updated_at': ts}

            try:
                parsed = json.loads(row['setting_json'] or '{}')
                if not isinstance(parsed, dict):
                    parsed = {}
            except Exception:
                parsed = {}
            normalized = self._normalize_admin_settings_payload(parsed, fallback=DEFAULT_ADMIN_SETTINGS)
            return {'settings': normalized, 'updated_at': row['updated_at'] or ''}

    def update_admin_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get_admin_settings()
        merged_input = {
            'general': {**(current.get('settings', {}).get('general') or {})},
            'api': {**(current.get('settings', {}).get('api') or {})},
        }
        if isinstance(payload.get('general'), dict):
            merged_input['general'].update(payload.get('general') or {})
        if isinstance(payload.get('api'), dict):
            merged_input['api'].update(payload.get('api') or {})
        normalized = self._normalize_admin_settings_payload(merged_input, fallback=current.get('settings') or DEFAULT_ADMIN_SETTINGS)
        ts = now_iso()
        with self.lock, self._conn() as conn:
            conn.execute(
                '''
                INSERT INTO app_settings(setting_key, setting_json, created_at, updated_at)
                VALUES('global', ?, ?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET
                  setting_json=excluded.setting_json,
                  updated_at=excluded.updated_at
                ''',
                (json.dumps(normalized, ensure_ascii=False), ts, ts),
            )
            conn.commit()
        return {'settings': normalized, 'updated_at': ts}

    def _parse_deadline_dt(self, raw: Any) -> Optional[datetime]:
        value = str(raw or '').strip()
        if not value:
            return None
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def _evaluate_crowdfunding_campaign(self, conn: sqlite3.Connection, work_row: sqlite3.Row) -> sqlite3.Row:
        sale_mode = self._normalize_sale_mode(work_row['sale_mode'])
        if sale_mode != 'crowdfunding':
            return work_row

        work_id = str(work_row['work_id'])
        status = self._normalize_crowdfunding_status(work_row['crowdfunding_status'])
        goal = int(work_row['crowdfunding_goal_amount'] or 0)
        deadline_dt = self._parse_deadline_dt(work_row['crowdfunding_deadline'])
        now_dt = datetime.now()

        metrics = self._compute_work_metrics(
            conn=conn,
            work_id=work_id,
            sale_mode='crowdfunding',
            crowdfunding_goal_amount=goal,
        )
        funded_amount = int(metrics.get('funded_amount') or 0)

        next_status = status
        should_refund = False

        if status == 'active':
            if goal > 0 and funded_amount >= goal:
                next_status = 'producing'
            elif deadline_dt is not None and now_dt >= deadline_dt and funded_amount < goal:
                next_status = 'failed'
                should_refund = True

        if next_status != status:
            conn.execute(
                'UPDATE works SET crowdfunding_status=?, updated_at=? WHERE work_id=?',
                (next_status, now_iso(), work_id),
            )

        if should_refund:
            self._auto_refund_crowdfunding_orders(
                conn=conn,
                work_id=work_id,
                reason='众筹截止未达目标，系统自动退款',
            )

        if next_status != status or should_refund:
            refreshed = conn.execute('SELECT * FROM works WHERE work_id=? LIMIT 1', (work_id,)).fetchone()
            return refreshed if refreshed else work_row
        return work_row

    def _auto_refund_crowdfunding_orders(self, conn: sqlite3.Connection, work_id: str, reason: str) -> None:
        pay_mode = (os.getenv('PAY_MODE', 'mock').strip().lower() or 'mock')
        is_wechat_mode = pay_mode in {'wechat', 'real'}
        rows = conn.execute(
            '''
            SELECT order_id, paid_amount
            FROM orders
            WHERE work_id=?
              AND sale_mode='crowdfunding'
              AND pay_status='paid'
              AND COALESCE(refund_status, 'none')='none'
            ''',
            (work_id,),
        ).fetchall()

        if not rows:
            return

        ts = now_iso()
        order_ids = [str(row['order_id']) for row in rows]
        placeholders = ','.join('?' for _ in order_ids)

        if is_wechat_mode:
            conn.execute(
                f'''
                UPDATE orders
                SET order_status='crowdfunding_refunding',
                    refund_status='pending_submit',
                    refund_amount=paid_amount,
                    refund_reason=?
                WHERE order_id IN ({placeholders})
                ''',
                tuple([reason] + order_ids),
            )
        else:
            conn.execute(
                f'''
                UPDATE orders
                SET pay_status='refunded',
                    order_status='crowdfunding_refunded',
                    refund_status='refunded',
                    refunded_at=?,
                    refund_amount=paid_amount,
                    refund_reason=?
                WHERE order_id IN ({placeholders})
                ''',
                tuple([ts, reason] + order_ids),
            )

        conn.execute(
            f'DELETE FROM designer_commission_records WHERE order_id IN ({placeholders})',
            tuple(order_ids),
        )

        for oid in order_ids:
            conn.execute(
                'INSERT INTO payment_logs(order_id, mode, payload_json, created_at) VALUES(?, ?, ?, ?)',
                (
                    oid,
                    'refund_pending_submit' if is_wechat_mode else 'auto_refund',
                    json.dumps({'reason': reason}, ensure_ascii=False),
                    ts,
                ),
            )

    def _compute_work_metrics(
        self,
        conn: sqlite3.Connection,
        work_id: str,
        sale_mode: str,
        crowdfunding_goal_amount: int,
    ) -> Dict[str, Any]:
        if sale_mode == 'crowdfunding':
            row = conn.execute(
                '''
                SELECT
                  COUNT(1) AS supporters_count,
                  COALESCE(SUM(paid_amount), 0) AS funded_amount
                FROM orders
                WHERE work_id=?
                  AND sale_mode='crowdfunding'
                  AND pay_status='paid'
                ''',
                (work_id,),
            ).fetchone()
            supporters_count = int(row['supporters_count'] or 0)
            funded_amount = int(row['funded_amount'] or 0)
            goal = int(crowdfunding_goal_amount or 0)
            progress_percent = 0.0
            if goal > 0:
                progress_percent = round(min(100.0, funded_amount * 100.0 / goal), 2)
            return {
                'supporters_count': supporters_count,
                'funded_amount': funded_amount,
                'goal_amount': goal,
                'progress_percent': progress_percent,
            }

        row = conn.execute(
            '''
            SELECT
              COUNT(1) AS paid_orders,
              COALESCE(SUM(paid_amount), 0) AS paid_amount
            FROM orders
            WHERE work_id=?
              AND sale_mode='preorder'
              AND pay_status='paid'
            ''',
            (work_id,),
        ).fetchone()
        return {
            'paid_orders': int(row['paid_orders'] or 0),
            'paid_amount': int(row['paid_amount'] or 0),
        }

    def _list_work_designers(self, conn: sqlite3.Connection, work_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 20), 100))
        rows = conn.execute(
            '''
            SELECT
              d.id AS designer_id,
              d.user_id,
              d.display_name,
              d.bio,
              d.avatar_url,
              d.status,
              l.share_ratio,
              l.updated_at,
              COALESCE(u.nickname, '') AS nickname
            FROM designer_work_links l
            JOIN designers d ON d.id = l.designer_id
            LEFT JOIN users u ON u.id = d.user_id
            WHERE l.work_id=? AND l.is_active=1
            ORDER BY l.updated_at DESC
            LIMIT ?
            ''',
            (str(work_id or '').strip(), safe_limit),
        ).fetchall()
        return [
            {
                'designer_id': int(row['designer_id']),
                'user_id': int(row['user_id']),
                'display_name': row['display_name'] or row['nickname'] or f"设计师{int(row['designer_id'])}",
                'bio': row['bio'] or '',
                'avatar_url': row['avatar_url'] or '',
                'status': row['status'] or 'active',
                'share_ratio': float(row['share_ratio'] or DEFAULT_DESIGNER_SHARE),
                'share_percent': round(float(row['share_ratio'] or DEFAULT_DESIGNER_SHARE) * 100, 2),
                'updated_at': row['updated_at'] or '',
            }
            for row in rows
        ]

    def _serialize_work_row(self, row: sqlite3.Row, metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        keys = set(row.keys())
        sale_mode = self._normalize_sale_mode(row['sale_mode'])
        crowdfunding_goal_amount = int(row['crowdfunding_goal_amount'] or 0)
        crowdfunding_deadline = str(row['crowdfunding_deadline'] or '')
        crowdfunding_status = self._normalize_crowdfunding_status(row['crowdfunding_status'])
        cover_image = str(row['cover_image'] or '') if 'cover_image' in keys else ''
        gallery_raw = row['gallery_json'] if 'gallery_json' in keys else '[]'
        try:
            gallery_images = json.loads(gallery_raw or '[]')
            if not isinstance(gallery_images, list):
                gallery_images = []
        except Exception:
            gallery_images = []
        gallery_images = [str(x).strip() for x in gallery_images if str(x).strip()][:12]
        metrics = metrics or {}
        ret = {
            'work_id': row['work_id'],
            'name': row['name'],
            'subtitle': row['subtitle'],
            'sale_mode': sale_mode,
            'sale_mode_text': SALE_MODE_TEXT.get(sale_mode, sale_mode),
            'crowdfunding_goal_amount': crowdfunding_goal_amount,
            'crowdfunding_deadline': crowdfunding_deadline,
            'crowdfunding_status': crowdfunding_status,
            'crowdfunding_status_text': CROWDFUNDING_STATUS_TEXT.get(crowdfunding_status, crowdfunding_status),
            'cover_image': cover_image,
            'gallery_images': gallery_images,
            'story': row['story'],
            'specs': json.loads(row['specs_json'] or '[]'),
            'highlights': json.loads(row['highlights_json'] or '[]'),
            'sku_list': json.loads(row['sku_json'] or '[]'),
            'funding': metrics if sale_mode == 'crowdfunding' else None,
            'preorder_stats': metrics if sale_mode == 'preorder' else None,
            'updated_at': row['updated_at'],
        }
        if 'is_current' in keys:
            ret['is_current'] = bool(int(row['is_current'] or 0))
        return ret

    def get_current_work(self) -> Dict[str, Any]:
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM works WHERE is_current=1 ORDER BY updated_at DESC LIMIT 1').fetchone()
            if not row:
                self.seed_default_work()
                row = conn.execute('SELECT * FROM works WHERE is_current=1 ORDER BY updated_at DESC LIMIT 1').fetchone()
            row = self._evaluate_crowdfunding_campaign(conn=conn, work_row=row)
            conn.commit()
            mode = self._normalize_sale_mode(row['sale_mode'])
            metrics = self._compute_work_metrics(
                conn=conn,
                work_id=row['work_id'],
                sale_mode=mode,
                crowdfunding_goal_amount=int(row['crowdfunding_goal_amount'] or 0),
            )
            ret = self._serialize_work_row(row, metrics=metrics)
            ret['designers'] = self._list_work_designers(conn=conn, work_id=str(row['work_id']), limit=20)
            return ret

    def get_work_by_id(self, work_id: str) -> Optional[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM works WHERE work_id=? LIMIT 1', (work_id,)).fetchone()
            if not row:
                return None
            row = self._evaluate_crowdfunding_campaign(conn=conn, work_row=row)
            conn.commit()
            mode = self._normalize_sale_mode(row['sale_mode'])
            metrics = self._compute_work_metrics(
                conn=conn,
                work_id=row['work_id'],
                sale_mode=mode,
                crowdfunding_goal_amount=int(row['crowdfunding_goal_amount'] or 0),
            )
            ret = self._serialize_work_row(row, metrics=metrics)
            ret['designers'] = self._list_work_designers(conn=conn, work_id=str(row['work_id']), limit=20)
            return ret

    def update_current_work(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get_current_work()
        sale_mode = self._normalize_sale_mode(payload.get('sale_mode') or current.get('sale_mode'))
        goal_amount_raw = payload.get('crowdfunding_goal_amount', current.get('crowdfunding_goal_amount', 0))
        try:
            goal_amount = int(goal_amount_raw or 0)
        except Exception:
            goal_amount = 0
        if goal_amount < 0:
            goal_amount = 0
        if sale_mode == 'crowdfunding' and goal_amount <= 0:
            raise ValueError('众筹模式下目标金额必须大于0')

        if 'crowdfunding_deadline' in payload:
            crowdfunding_deadline = str(payload.get('crowdfunding_deadline') or '')
        else:
            crowdfunding_deadline = str(current.get('crowdfunding_deadline') or '')

        merged = {
            'work_id': current['work_id'],
            'name': payload.get('name') or current['name'],
            'subtitle': payload.get('subtitle') or current['subtitle'],
            'sale_mode': sale_mode,
            'crowdfunding_goal_amount': goal_amount,
            'crowdfunding_deadline': crowdfunding_deadline,
            'crowdfunding_status': 'active' if sale_mode == 'crowdfunding' else 'active',
            'cover_image': str(payload.get('cover_image') if 'cover_image' in payload else current.get('cover_image') or '').strip(),
            'gallery_images': payload.get('gallery_images')
            if 'gallery_images' in payload
            else payload.get('gallery')
            if 'gallery' in payload
            else current.get('gallery_images', []),
            'story': payload.get('story') or current['story'],
            'specs': payload.get('specs') or current['specs'],
            'highlights': payload.get('highlights') or current['highlights'],
            'sku_list': payload.get('sku_list') or current['sku_list'],
        }
        if not isinstance(merged['gallery_images'], list):
            merged['gallery_images'] = []
        merged['gallery_images'] = [str(x).strip() for x in merged['gallery_images'] if str(x).strip()][:12]

        with self.lock, self._conn() as conn:
            conn.execute('UPDATE works SET is_current=0')
            conn.execute(
                '''
                INSERT OR REPLACE INTO works(
                  work_id, name, subtitle, sale_mode, crowdfunding_goal_amount, crowdfunding_deadline,
                  crowdfunding_status, cover_image, gallery_json, story, specs_json, highlights_json, sku_json, is_current, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                ''',
                (
                    merged['work_id'],
                    merged['name'],
                    merged['subtitle'],
                    merged['sale_mode'],
                    int(merged['crowdfunding_goal_amount']),
                    merged['crowdfunding_deadline'],
                    merged['crowdfunding_status'],
                    merged['cover_image'],
                    json.dumps(merged['gallery_images'], ensure_ascii=False),
                    merged['story'],
                    json.dumps(merged['specs'], ensure_ascii=False),
                    json.dumps(merged['highlights'], ensure_ascii=False),
                    json.dumps(merged['sku_list'], ensure_ascii=False),
                    now_iso(),
                ),
            )
            conn.commit()
        return self.get_current_work()

    def _validate_work_id(self, raw: Any) -> str:
        work_id = str(raw or '').strip()
        if not work_id:
            raise ValueError('项目ID不能为空')
        if len(work_id) > 64:
            raise ValueError('项目ID长度不能超过 64')
        for ch in work_id:
            if not (ch.isalnum() or ch in {'-', '_'}):
                raise ValueError('项目ID仅支持字母、数字、-、_')
        return work_id

    def _normalize_project_payload(
        self,
        payload: Dict[str, Any],
        fallback: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        base = fallback or {}
        sale_mode = self._normalize_sale_mode(payload.get('sale_mode') if 'sale_mode' in payload else base.get('sale_mode'))
        goal_raw = payload.get('crowdfunding_goal_amount') if 'crowdfunding_goal_amount' in payload else base.get('crowdfunding_goal_amount', 0)
        try:
            goal = int(goal_raw or 0)
        except Exception:
            goal = 0
        if goal < 0:
            goal = 0
        if sale_mode == 'crowdfunding' and goal <= 0:
            raise ValueError('众筹模式下目标金额必须大于0')

        name = str(payload.get('name') if 'name' in payload else base.get('name', '')).strip()
        if not name:
            raise ValueError('项目名称不能为空')
        subtitle = str(payload.get('subtitle') if 'subtitle' in payload else base.get('subtitle', '')).strip()
        story = str(payload.get('story') if 'story' in payload else base.get('story', '')).strip()
        if not story:
            story = str(DEFAULT_WORK.get('story') or '')

        specs = payload.get('specs') if 'specs' in payload else base.get('specs', DEFAULT_WORK.get('specs') or [])
        highlights = payload.get('highlights') if 'highlights' in payload else base.get('highlights', DEFAULT_WORK.get('highlights') or [])
        sku_list = payload.get('sku_list') if 'sku_list' in payload else base.get('sku_list', DEFAULT_WORK.get('sku_list') or [])
        if not isinstance(specs, list):
            raise ValueError('specs 必须是数组')
        if not isinstance(highlights, list):
            raise ValueError('highlights 必须是数组')
        if not isinstance(sku_list, list):
            raise ValueError('sku_list 必须是数组')

        crowdfunding_deadline = str(
            payload.get('crowdfunding_deadline')
            if 'crowdfunding_deadline' in payload
            else base.get('crowdfunding_deadline', '')
        ).strip()
        cover_image = str(
            payload.get('cover_image')
            if 'cover_image' in payload
            else payload.get('image_cover')
            if 'image_cover' in payload
            else base.get('cover_image', '')
        ).strip()
        gallery_raw = (
            payload.get('gallery_images')
            if 'gallery_images' in payload
            else payload.get('gallery')
            if 'gallery' in payload
            else base.get('gallery_images', [])
        )
        if isinstance(gallery_raw, str):
            gallery_images = [x.strip() for x in gallery_raw.split('\n') if x.strip()]
        elif isinstance(gallery_raw, list):
            gallery_images = [str(x).strip() for x in gallery_raw if str(x).strip()]
        else:
            gallery_images = []
        gallery_images = gallery_images[:12]

        return {
            'name': name,
            'subtitle': subtitle,
            'sale_mode': sale_mode,
            'crowdfunding_goal_amount': goal,
            'crowdfunding_deadline': crowdfunding_deadline,
            'crowdfunding_status': 'active',
            'cover_image': cover_image,
            'gallery_images': gallery_images,
            'story': story,
            'specs': specs,
            'highlights': highlights,
            'sku_list': sku_list,
        }

    def admin_list_projects(
        self,
        keyword: str = '',
        sale_mode: str = '',
        is_current: int = -1,
        limit: int = 200,
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit or 200), 1000))
        mode = self._normalize_sale_mode(sale_mode) if sale_mode else ''
        keyword = (keyword or '').strip()
        current_filter = int(is_current) if int(is_current or -1) in {-1, 0, 1} else -1
        conditions: List[str] = []
        params: List[Any] = []
        if mode:
            conditions.append('sale_mode=?')
            params.append(mode)
        if keyword:
            like_kw = f'%{keyword}%'
            conditions.append('(work_id LIKE ? OR name LIKE ? OR subtitle LIKE ?)')
            params.extend([like_kw, like_kw, like_kw])
        if current_filter in {0, 1}:
            conditions.append('is_current=?')
            params.append(current_filter)
        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ''

        with self.lock, self._conn() as conn:
            rows = conn.execute(
                f'''
                SELECT *
                FROM works
                {where_sql}
                ORDER BY is_current DESC, updated_at DESC
                LIMIT ?
                ''',
                tuple(params + [safe_limit]),
            ).fetchall()

            items: List[Dict[str, Any]] = []
            for row in rows:
                row = self._evaluate_crowdfunding_campaign(conn=conn, work_row=row)
                mode_val = self._normalize_sale_mode(row['sale_mode'])
                metrics = self._compute_work_metrics(
                    conn=conn,
                    work_id=row['work_id'],
                    sale_mode=mode_val,
                    crowdfunding_goal_amount=int(row['crowdfunding_goal_amount'] or 0),
                )
                item = self._serialize_work_row(row, metrics=metrics)
                designer_rows = conn.execute(
                    '''
                    SELECT
                      l.share_ratio,
                      d.id AS designer_id,
                      d.display_name,
                      d.status,
                      COALESCE(u.openid, '') AS openid,
                      COALESCE(u.nickname, '') AS nickname
                    FROM designer_work_links l
                    JOIN designers d ON d.id = l.designer_id
                    LEFT JOIN users u ON u.id = d.user_id
                    WHERE l.work_id=? AND l.is_active=1
                    ORDER BY l.updated_at DESC
                    LIMIT 20
                    ''',
                    (str(row['work_id']),),
                ).fetchall()
                item['designers'] = [
                    {
                        'designer_id': int(x['designer_id']),
                        'display_name': x['display_name'] or x['nickname'] or '',
                        'openid': x['openid'] or '',
                        'status': x['status'] or '',
                        'share_ratio': float(x['share_ratio'] or DEFAULT_DESIGNER_SHARE),
                        'share_percent': round(float(x['share_ratio'] or DEFAULT_DESIGNER_SHARE) * 100, 2),
                    }
                    for x in designer_rows
                ]
                items.append(item)

            summary_row = conn.execute(
                f'''
                SELECT
                  COUNT(1) AS total,
                  COALESCE(SUM(CASE WHEN sale_mode='preorder' THEN 1 ELSE 0 END), 0) AS preorder_count,
                  COALESCE(SUM(CASE WHEN sale_mode='crowdfunding' THEN 1 ELSE 0 END), 0) AS crowdfunding_count,
                  COALESCE(SUM(CASE WHEN is_current=1 THEN 1 ELSE 0 END), 0) AS current_count
                FROM works
                {where_sql}
                ''',
                tuple(params),
            ).fetchone()
            current_row = conn.execute(
                'SELECT work_id, name FROM works WHERE is_current=1 ORDER BY updated_at DESC LIMIT 1'
            ).fetchone()
            conn.commit()
        return {
            'items': items,
            'summary': {
                'total': int(summary_row['total'] or 0),
                'preorder_count': int(summary_row['preorder_count'] or 0),
                'crowdfunding_count': int(summary_row['crowdfunding_count'] or 0),
                'current_count': int(summary_row['current_count'] or 0),
                'current_work_id': (current_row['work_id'] if current_row else ''),
                'current_work_name': (current_row['name'] if current_row else ''),
            },
        }

    def admin_create_project(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        work_id = self._validate_work_id(payload.get('work_id'))
        merged = self._normalize_project_payload(payload=payload, fallback=DEFAULT_WORK)
        wants_current = bool(payload.get('is_current'))
        ts = now_iso()
        with self.lock, self._conn() as conn:
            exists = conn.execute('SELECT work_id FROM works WHERE work_id=? LIMIT 1', (work_id,)).fetchone()
            if exists:
                raise ValueError('项目ID已存在，请更换后重试')
            if wants_current:
                conn.execute('UPDATE works SET is_current=0')
            else:
                has_current = conn.execute('SELECT work_id FROM works WHERE is_current=1 LIMIT 1').fetchone()
                if not has_current:
                    wants_current = True
            conn.execute(
                '''
                INSERT INTO works(
                  work_id, name, subtitle, sale_mode, crowdfunding_goal_amount, crowdfunding_deadline,
                  crowdfunding_status, cover_image, gallery_json, story, specs_json, highlights_json, sku_json, is_current, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    work_id,
                    merged['name'],
                    merged['subtitle'],
                    merged['sale_mode'],
                    int(merged['crowdfunding_goal_amount']),
                    merged['crowdfunding_deadline'],
                    merged['crowdfunding_status'],
                    merged['cover_image'],
                    json.dumps(merged['gallery_images'], ensure_ascii=False),
                    merged['story'],
                    json.dumps(merged['specs'], ensure_ascii=False),
                    json.dumps(merged['highlights'], ensure_ascii=False),
                    json.dumps(merged['sku_list'], ensure_ascii=False),
                    1 if wants_current else 0,
                    ts,
                ),
            )
            conn.commit()
        item = self.get_work_by_id(work_id)
        if not item:
            raise ValueError('项目创建失败')
        return item

    def admin_update_project(self, work_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        safe_work_id = self._validate_work_id(work_id)
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM works WHERE work_id=? LIMIT 1', (safe_work_id,)).fetchone()
            if not row:
                raise ValueError('项目不存在')
            try:
                gallery_images = json.loads(row['gallery_json'] or '[]')
                if not isinstance(gallery_images, list):
                    gallery_images = []
            except Exception:
                gallery_images = []
            current = {
                'name': row['name'],
                'subtitle': row['subtitle'],
                'sale_mode': row['sale_mode'],
                'crowdfunding_goal_amount': int(row['crowdfunding_goal_amount'] or 0),
                'crowdfunding_deadline': row['crowdfunding_deadline'] or '',
                'cover_image': row['cover_image'] or '',
                'gallery_images': gallery_images,
                'story': row['story'] or '',
                'specs': json.loads(row['specs_json'] or '[]'),
                'highlights': json.loads(row['highlights_json'] or '[]'),
                'sku_list': json.loads(row['sku_json'] or '[]'),
            }
            merged = self._normalize_project_payload(payload=payload, fallback=current)
            desired_current = bool(int(row['is_current'] or 0))
            if 'is_current' in payload:
                desired_current = bool(payload.get('is_current'))
            if desired_current:
                conn.execute('UPDATE works SET is_current=0')
            conn.execute(
                '''
                UPDATE works
                SET name=?, subtitle=?, sale_mode=?, crowdfunding_goal_amount=?, crowdfunding_deadline=?,
                    crowdfunding_status=?, cover_image=?, gallery_json=?, story=?, specs_json=?, highlights_json=?, sku_json=?, is_current=?, updated_at=?
                WHERE work_id=?
                ''',
                (
                    merged['name'],
                    merged['subtitle'],
                    merged['sale_mode'],
                    int(merged['crowdfunding_goal_amount']),
                    merged['crowdfunding_deadline'],
                    merged['crowdfunding_status'],
                    merged['cover_image'],
                    json.dumps(merged['gallery_images'], ensure_ascii=False),
                    merged['story'],
                    json.dumps(merged['specs'], ensure_ascii=False),
                    json.dumps(merged['highlights'], ensure_ascii=False),
                    json.dumps(merged['sku_list'], ensure_ascii=False),
                    1 if desired_current else 0,
                    now_iso(),
                    safe_work_id,
                ),
            )
            if not desired_current:
                has_current = conn.execute('SELECT work_id FROM works WHERE is_current=1 LIMIT 1').fetchone()
                if not has_current:
                    conn.execute('UPDATE works SET is_current=1 WHERE work_id=?', (safe_work_id,))
            conn.commit()
        item = self.get_work_by_id(safe_work_id)
        if not item:
            raise ValueError('项目更新失败')
        return item

    def admin_set_current_project(self, work_id: str) -> Dict[str, Any]:
        safe_work_id = self._validate_work_id(work_id)
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT work_id FROM works WHERE work_id=? LIMIT 1', (safe_work_id,)).fetchone()
            if not row:
                raise ValueError('项目不存在')
            conn.execute('UPDATE works SET is_current=0')
            conn.execute('UPDATE works SET is_current=1, updated_at=? WHERE work_id=?', (now_iso(), safe_work_id))
            conn.commit()
        item = self.get_work_by_id(safe_work_id)
        if not item:
            raise ValueError('切换当前项目失败')
        return item

    def upsert_user_session(self, openid: str, nickname: str = '') -> Dict[str, Any]:
        token = uuid.uuid4().hex
        timestamp = now_iso()
        is_new_user = False

        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT id, nickname FROM users WHERE openid=? LIMIT 1', (openid,)).fetchone()
            if row:
                next_name = nickname.strip() or row['nickname'] or ''
                conn.execute(
                    'UPDATE users SET nickname=?, session_token=?, updated_at=? WHERE id=?',
                    (next_name, token, timestamp, row['id']),
                )
                user_id = int(row['id'])
                final_name = next_name
            else:
                conn.execute(
                    'INSERT INTO users(openid, nickname, session_token, created_at, updated_at) VALUES(?, ?, ?, ?, ?)',
                    (openid, nickname.strip(), token, timestamp, timestamp),
                )
                user_id = int(conn.execute('SELECT last_insert_rowid() AS id').fetchone()['id'])
                final_name = nickname.strip()
                is_new_user = True
            conn.commit()

        return {
            'user_id': user_id,
            'openid': openid,
            'nickname': final_name,
            'session_token': token,
            'is_new_user': is_new_user,
        }

    def get_user_by_openid(self, openid: str) -> Optional[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT id, openid, nickname FROM users WHERE openid=? LIMIT 1', (openid,)).fetchone()
            if not row:
                return None
            return {
                'user_id': int(row['id']),
                'openid': row['openid'],
                'nickname': row['nickname'],
            }

    def get_user_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        if not token:
            return None
        with self.lock, self._conn() as conn:
            row = conn.execute(
                'SELECT id, openid, nickname, session_token FROM users WHERE session_token=? LIMIT 1',
                (token,),
            ).fetchone()
            if not row:
                return None
            return {
                'user_id': int(row['id']),
                'openid': row['openid'],
                'nickname': row['nickname'],
                'session_token': row['session_token'],
            }

    def get_user_profile(self, user_id: int) -> Dict[str, Any]:
        with self.lock, self._conn() as conn:
            row = conn.execute(
                'SELECT id, openid, nickname, created_at, updated_at FROM users WHERE id=? LIMIT 1',
                (int(user_id),),
            ).fetchone()
            if not row:
                raise ValueError('用户不存在')
            return {
                'user_id': int(row['id']),
                'openid': row['openid'],
                'nickname': row['nickname'] or '',
                'created_at': row['created_at'],
                'updated_at': row['updated_at'],
                'registered': bool((row['nickname'] or '').strip()),
            }

    def update_user_profile(self, user_id: int, nickname: str = '') -> Dict[str, Any]:
        safe_name = (nickname or '').strip()
        if not safe_name:
            raise ValueError('昵称不能为空')
        if len(safe_name) > 32:
            raise ValueError('昵称不能超过 32 个字符')
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT id FROM users WHERE id=? LIMIT 1', (int(user_id),)).fetchone()
            if not row:
                raise ValueError('用户不存在')
            conn.execute(
                'UPDATE users SET nickname=?, updated_at=? WHERE id=?',
                (safe_name, now_iso(), int(user_id)),
            )
            conn.commit()
        return self.get_user_profile(user_id)

    def _serialize_feedback_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        keys = set(row.keys())
        status = str(row['status'] or 'pending').strip() if 'status' in keys else 'pending'
        priority = str(row['priority'] or 'normal').strip().lower() if 'priority' in keys else 'normal'
        if priority not in FEEDBACK_PRIORITY_TEXT:
            priority = 'normal'
        images_raw = row['images_json'] if 'images_json' in keys else '[]'
        try:
            image_urls = json.loads(images_raw or '[]')
            if not isinstance(image_urls, list):
                image_urls = []
        except Exception:
            image_urls = []
        image_urls = [str(x).strip() for x in image_urls if str(x).strip()][:9]
        return {
            'id': int(row['id']),
            'user_id': int(row['user_id']),
            'user_openid': row['user_openid'] if 'user_openid' in keys else '',
            'user_nickname': row['user_nickname'] if 'user_nickname' in keys else '',
            'category': row['category'] or 'general',
            'priority': priority,
            'priority_text': FEEDBACK_PRIORITY_TEXT.get(priority, priority),
            'content': row['content'] or '',
            'image_urls': image_urls,
            'contact': row['contact'] or '',
            'status': status,
            'status_text': FEEDBACK_STATUS_TEXT.get(status, status),
            'admin_reply': row['admin_reply'] or '',
            'reply_operator': row['reply_operator'] if 'reply_operator' in keys else '',
            'replied_at': row['replied_at'] if 'replied_at' in keys else '',
            'created_at': row['created_at'] or '',
            'updated_at': row['updated_at'] or '',
        }

    def create_feedback(
        self,
        user_id: int,
        category: str,
        content: str,
        contact: str = '',
        priority: str = 'normal',
        image_urls: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        safe_category = (category or 'general').strip().lower()
        if safe_category not in {'general', 'bug', 'feature', 'service'}:
            safe_category = 'general'
        safe_priority = (priority or 'normal').strip().lower()
        if safe_priority not in FEEDBACK_PRIORITY_TEXT:
            safe_priority = 'normal'
        safe_content = (content or '').strip()
        if not safe_content:
            raise ValueError('反馈内容不能为空')
        if len(safe_content) < 5:
            raise ValueError('反馈内容至少 5 个字符')
        if len(safe_content) > 1000:
            raise ValueError('反馈内容不能超过 1000 个字符')
        safe_contact = (contact or '').strip()[:120]
        cleaned_images = [str(x).strip() for x in (image_urls or []) if str(x).strip()][:9]
        ts = now_iso()
        with self.lock, self._conn() as conn:
            conn.execute(
                '''
                INSERT INTO user_feedbacks(
                  user_id, category, priority, content, images_json, contact, status, admin_reply, reply_operator, replied_at, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, 'pending', '', '', '', ?, ?)
                ''',
                (
                    int(user_id),
                    safe_category,
                    safe_priority,
                    safe_content,
                    json.dumps(cleaned_images, ensure_ascii=False),
                    safe_contact,
                    ts,
                    ts,
                ),
            )
            feedback_id = int(conn.execute('SELECT last_insert_rowid() AS id').fetchone()['id'])
            row = conn.execute(
                '''
                SELECT f.*, COALESCE(u.openid, '') AS user_openid, COALESCE(u.nickname, '') AS user_nickname
                FROM user_feedbacks f
                LEFT JOIN users u ON u.id = f.user_id
                WHERE f.id=?
                LIMIT 1
                ''',
                (feedback_id,),
            ).fetchone()
            conn.commit()
            return self._serialize_feedback_row(row)

    def list_feedback_by_user(self, user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 50), 200))
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT f.*, COALESCE(u.openid, '') AS user_openid, COALESCE(u.nickname, '') AS user_nickname
                FROM user_feedbacks f
                LEFT JOIN users u ON u.id = f.user_id
                WHERE f.user_id=?
                ORDER BY f.id DESC
                LIMIT ?
                ''',
                (int(user_id), safe_limit),
            ).fetchall()
        return [self._serialize_feedback_row(row) for row in rows]

    def admin_list_feedback(self, status: str = '', keyword: str = '', priority: str = '', limit: int = 200) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit or 200), 2000))
        status = (status or '').strip().lower()
        priority = (priority or '').strip().lower()
        keyword = (keyword or '').strip()
        conditions: List[str] = []
        params: List[Any] = []
        if status:
            conditions.append('f.status=?')
            params.append(status)
        if priority:
            conditions.append('f.priority=?')
            params.append(priority)
        if keyword:
            like_kw = f'%{keyword}%'
            conditions.append('(u.openid LIKE ? OR u.nickname LIKE ? OR f.content LIKE ? OR f.contact LIKE ?)')
            params.extend([like_kw, like_kw, like_kw, like_kw])
        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ''

        with self.lock, self._conn() as conn:
            rows = conn.execute(
                f'''
                SELECT f.*, COALESCE(u.openid, '') AS user_openid, COALESCE(u.nickname, '') AS user_nickname
                FROM user_feedbacks f
                LEFT JOIN users u ON u.id = f.user_id
                {where_sql}
                ORDER BY f.id DESC
                LIMIT ?
                ''',
                tuple(params + [safe_limit]),
            ).fetchall()
            items = [self._serialize_feedback_row(row) for row in rows]

            summary_row = conn.execute(
                f'''
                SELECT
                  COUNT(1) AS total,
                  COALESCE(SUM(CASE WHEN f.status='pending' THEN 1 ELSE 0 END), 0) AS pending_count,
                  COALESCE(SUM(CASE WHEN f.status='processing' THEN 1 ELSE 0 END), 0) AS processing_count,
                  COALESCE(SUM(CASE WHEN f.status='resolved' THEN 1 ELSE 0 END), 0) AS resolved_count,
                  COALESCE(SUM(CASE WHEN f.status='rejected' THEN 1 ELSE 0 END), 0) AS rejected_count
                FROM user_feedbacks f
                LEFT JOIN users u ON u.id = f.user_id
                {where_sql}
                ''',
                tuple(params),
            ).fetchone()
        return {
            'items': items,
            'summary': {
                'total': int(summary_row['total'] or 0),
                'pending_count': int(summary_row['pending_count'] or 0),
                'processing_count': int(summary_row['processing_count'] or 0),
                'resolved_count': int(summary_row['resolved_count'] or 0),
                'rejected_count': int(summary_row['rejected_count'] or 0),
            },
        }

    def admin_export_feedback_csv(self, status: str = '', keyword: str = '', priority: str = '', limit: int = 5000) -> str:
        safe_limit = max(1, min(int(limit or 5000), 10000))
        data = self.admin_list_feedback(
            status=(status or '').strip(),
            keyword=(keyword or '').strip(),
            priority=(priority or '').strip(),
            limit=safe_limit,
        )
        items = data.get('items') or []
        buff = io.StringIO()
        writer = csv.writer(buff)
        writer.writerow(
            [
                'id',
                'status',
                'status_text',
                'priority',
                'priority_text',
                'category',
                'user_id',
                'user_openid',
                'user_nickname',
                'content',
                'contact',
                'image_urls',
                'admin_reply',
                'reply_operator',
                'replied_at',
                'created_at',
                'updated_at',
            ]
        )
        for it in items:
            writer.writerow(
                [
                    it.get('id', ''),
                    it.get('status', ''),
                    it.get('status_text', ''),
                    it.get('priority', ''),
                    it.get('priority_text', ''),
                    it.get('category', ''),
                    it.get('user_id', ''),
                    it.get('user_openid', ''),
                    it.get('user_nickname', ''),
                    it.get('content', ''),
                    it.get('contact', ''),
                    ' | '.join([str(x).strip() for x in (it.get('image_urls') or []) if str(x).strip()]),
                    it.get('admin_reply', ''),
                    it.get('reply_operator', ''),
                    it.get('replied_at', ''),
                    it.get('created_at', ''),
                    it.get('updated_at', ''),
                ]
            )
        return buff.getvalue()

    def admin_reply_feedback(
        self,
        feedback_id: int,
        status: str = 'resolved',
        admin_reply: str = '',
        reply_operator: str = '',
    ) -> Dict[str, Any]:
        safe_status = (status or '').strip().lower()
        if safe_status not in {'pending', 'processing', 'resolved', 'rejected'}:
            raise ValueError('反馈状态非法')
        safe_reply = (admin_reply or '').strip()[:1000]
        if safe_status in {'resolved', 'rejected'} and not safe_reply:
            raise ValueError('已处理状态必须填写回复内容')
        safe_operator = (reply_operator or '').strip()[:64]
        ts = now_iso()
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT id FROM user_feedbacks WHERE id=? LIMIT 1', (int(feedback_id),)).fetchone()
            if not row:
                raise ValueError('反馈记录不存在')
            replied_at = ts if safe_status in {'resolved', 'rejected'} else ''
            conn.execute(
                '''
                UPDATE user_feedbacks
                SET status=?, admin_reply=?, reply_operator=?, replied_at=?, updated_at=?
                WHERE id=?
                ''',
                (safe_status, safe_reply, safe_operator, replied_at, ts, int(feedback_id)),
            )
            row = conn.execute(
                '''
                SELECT f.*, COALESCE(u.openid, '') AS user_openid, COALESCE(u.nickname, '') AS user_nickname
                FROM user_feedbacks f
                LEFT JOIN users u ON u.id = f.user_id
                WHERE f.id=?
                LIMIT 1
                ''',
                (int(feedback_id),),
            ).fetchone()
            conn.commit()
        return self._serialize_feedback_row(row)

    def list_feedback_templates(self, active_only: bool = False, limit: int = 200) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 200), 1000))
        with self.lock, self._conn() as conn:
            if active_only:
                rows = conn.execute(
                    '''
                    SELECT *
                    FROM feedback_reply_templates
                    WHERE is_active=1
                    ORDER BY updated_at DESC, id DESC
                    LIMIT ?
                    ''',
                    (safe_limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    '''
                    SELECT *
                    FROM feedback_reply_templates
                    ORDER BY updated_at DESC, id DESC
                    LIMIT ?
                    ''',
                    (safe_limit,),
                ).fetchall()
        return [
            {
                'id': int(row['id']),
                'code': row['code'] or '',
                'title': row['title'] or '',
                'content': row['content'] or '',
                'is_active': bool(int(row['is_active'] or 0)),
                'created_at': row['created_at'] or '',
                'updated_at': row['updated_at'] or '',
            }
            for row in rows
        ]

    def get_feedback_template_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        safe_code = (code or '').strip()
        if not safe_code:
            return None
        with self.lock, self._conn() as conn:
            row = conn.execute(
                '''
                SELECT *
                FROM feedback_reply_templates
                WHERE code=?
                LIMIT 1
                ''',
                (safe_code,),
            ).fetchone()
        if not row:
            return None
        return {
            'id': int(row['id']),
            'code': row['code'] or '',
            'title': row['title'] or '',
            'content': row['content'] or '',
            'is_active': bool(int(row['is_active'] or 0)),
            'created_at': row['created_at'] or '',
            'updated_at': row['updated_at'] or '',
        }

    def upsert_feedback_template(
        self,
        code: str,
        title: str,
        content: str,
        is_active: bool = True,
    ) -> Dict[str, Any]:
        safe_code = (code or '').strip().lower()
        safe_title = (title or '').strip()
        safe_content = (content or '').strip()
        if not safe_code:
            raise ValueError('模板编码不能为空')
        if len(safe_code) > 64:
            raise ValueError('模板编码不能超过 64 字符')
        if not safe_title:
            raise ValueError('模板标题不能为空')
        if len(safe_title) > 80:
            raise ValueError('模板标题不能超过 80 字符')
        if not safe_content:
            raise ValueError('模板内容不能为空')
        if len(safe_content) > 1000:
            raise ValueError('模板内容不能超过 1000 字符')
        ts = now_iso()
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT id FROM feedback_reply_templates WHERE code=? LIMIT 1', (safe_code,)).fetchone()
            if row:
                conn.execute(
                    '''
                    UPDATE feedback_reply_templates
                    SET title=?, content=?, is_active=?, updated_at=?
                    WHERE code=?
                    ''',
                    (safe_title, safe_content, 1 if is_active else 0, ts, safe_code),
                )
            else:
                conn.execute(
                    '''
                    INSERT INTO feedback_reply_templates(
                      code, title, content, is_active, created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?)
                    ''',
                    (safe_code, safe_title, safe_content, 1 if is_active else 0, ts, ts),
                )
            conn.commit()
        ret = self.get_feedback_template_by_code(safe_code)
        if not ret:
            raise ValueError('模板写入失败')
        return ret

    def reserve_work(self, user_id: int, work_id: str) -> Dict[str, Any]:
        created = False
        with self.lock, self._conn() as conn:
            exists = conn.execute('SELECT id FROM reservations WHERE user_id=? AND work_id=? LIMIT 1', (user_id, work_id)).fetchone()
            if not exists:
                conn.execute('INSERT INTO reservations(user_id, work_id, created_at) VALUES(?, ?, ?)', (user_id, work_id, now_iso()))
                conn.commit()
                created = True
        return {'reserved': True, 'created': created}

    def has_reservation(self, user_id: int, work_id: str) -> bool:
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT id FROM reservations WHERE user_id=? AND work_id=? LIMIT 1', (user_id, work_id)).fetchone()
            return bool(row)

    def _serialize_order_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        status = row['order_status']
        sale_mode = self._normalize_sale_mode(row['sale_mode'])
        keys = set(row.keys())
        return {
            'order_id': row['order_id'],
            'user_id': int(row['user_id']) if 'user_id' in keys else 0,
            'work_id': row['work_id'],
            'work_name': row['work_name'],
            'sku_id': row['sku_id'],
            'sku_name': row['sku_name'],
            'sale_mode': sale_mode,
            'quantity': int(row['quantity']),
            'unit_price': int(row['unit_price']),
            'deposit_price': int(row['deposit_price']),
            'total_amount': int(row['total_amount']),
            'deposit_amount': int(row['deposit_amount']),
            'paid_amount': int(row['paid_amount']),
            'pay_status': row['pay_status'],
            'order_status': status,
            'order_status_text': ORDER_STATUS_TEXT.get(status, status),
            'refund_status': row['refund_status'],
            'refunded_at': row['refunded_at'],
            'refund_amount': int(row['refund_amount'] or 0),
            'refund_reason': row['refund_reason'],
            'admin_note': row['admin_note'] if 'admin_note' in keys else '',
            'payment_channel': row['payment_channel'],
            'transaction_id': row['transaction_id'],
            'created_at': row['created_at'],
            'paid_at': row['paid_at'],
            'user_openid': row['user_openid'] if 'user_openid' in keys else '',
            'user_nickname': row['user_nickname'] if 'user_nickname' in keys else '',
        }

    def create_preorder(self, user_id: int, sku_id: str, quantity: int) -> Dict[str, Any]:
        work = self.get_current_work()
        sale_mode = self._normalize_sale_mode(work.get('sale_mode'))
        crowdfunding_status = self._normalize_crowdfunding_status(work.get('crowdfunding_status'))
        if sale_mode == 'crowdfunding':
            if crowdfunding_status == 'producing':
                raise ValueError('众筹已达标并进入生产阶段，当前不再接受众筹支持')
            if crowdfunding_status == 'failed':
                raise ValueError('众筹已结束且未达标，订单已自动退款，当前不可支持')

        sku = next((x for x in work['sku_list'] if x.get('id') == sku_id), None)
        if not sku:
            raise ValueError('版本不存在')
        if quantity < 1 or quantity > 20:
            raise ValueError('购买数量超出范围')

        unit_price = int(sku.get('price') or 0)
        deposit_price = int(sku.get('deposit') or 0)
        total_amount = unit_price * quantity
        deposit_amount = deposit_price * quantity
        if sale_mode == 'crowdfunding':
            paid_amount = total_amount
            order_status = 'crowdfunding_pending'
        else:
            paid_amount = deposit_amount
            order_status = 'pending_deposit'
        order_id = f"KWC{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6].upper()}"
        created_at = now_iso()

        with self.lock, self._conn() as conn:
            conn.execute(
                '''
                INSERT INTO orders(
                  order_id, user_id, work_id, work_name, sku_id, sku_name, quantity,
                  unit_price, deposit_price, total_amount, deposit_amount, paid_amount, sale_mode,
                  pay_status, order_status, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    order_id,
                    user_id,
                    work['work_id'],
                    work['name'],
                    sku_id,
                    str(sku.get('name') or ''),
                    quantity,
                    unit_price,
                    deposit_price,
                    total_amount,
                    deposit_amount,
                    paid_amount,
                    sale_mode,
                    'pending',
                    order_status,
                    created_at,
                ),
            )
            conn.commit()

            row = conn.execute('SELECT * FROM orders WHERE order_id=? LIMIT 1', (order_id,)).fetchone()
            return self._serialize_order_row(row)

    def _sync_commissions_for_order(self, conn: sqlite3.Connection, order_id: str, work_id: str) -> None:
        ts = now_iso()
        conn.execute(
            '''
            INSERT INTO designer_commission_records(
              designer_id, order_id, work_id, share_ratio, commission_amount,
              settlement_status, settlement_note, settled_at, created_at, updated_at
            )
            SELECT
              l.designer_id,
              o.order_id,
              o.work_id,
              l.share_ratio,
              ROUND(o.total_amount * l.share_ratio, 2),
              'pending', '', '', ?, ?
            FROM orders o
            JOIN designer_work_links l
              ON l.work_id = o.work_id
             AND l.is_active = 1
            WHERE o.order_id = ?
              AND o.work_id = ?
              AND o.pay_status = 'paid'
              AND COALESCE(o.refund_status, 'none') = 'none'
              AND NOT EXISTS(
                SELECT 1 FROM designer_commission_records c
                WHERE c.designer_id = l.designer_id AND c.order_id = o.order_id
              )
            ''',
            (ts, ts, order_id, work_id),
        )

    def _ensure_commission_records(self) -> None:
        with self.lock, self._conn() as conn:
            ts = now_iso()
            conn.execute(
                '''
                INSERT INTO designer_commission_records(
                  designer_id, order_id, work_id, share_ratio, commission_amount,
                  settlement_status, settlement_note, settled_at, created_at, updated_at
                )
                SELECT
                  l.designer_id,
                  o.order_id,
                  o.work_id,
                  l.share_ratio,
                  ROUND(o.total_amount * l.share_ratio, 2),
                  'pending', '', '', ?, ?
                FROM orders o
                JOIN designer_work_links l
                  ON l.work_id = o.work_id
                 AND l.is_active = 1
                WHERE o.pay_status = 'paid'
                  AND COALESCE(o.refund_status, 'none') = 'none'
                  AND NOT EXISTS(
                    SELECT 1 FROM designer_commission_records c
                    WHERE c.designer_id = l.designer_id AND c.order_id = o.order_id
                  )
                ''',
                (ts, ts),
            )
            conn.commit()

    def mark_order_paid(self, order_id: str, user_id: int, payment_channel: str, transaction_id: str = '') -> Dict[str, Any]:
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM orders WHERE order_id=? AND user_id=? LIMIT 1', (order_id, user_id)).fetchone()
            if not row:
                raise ValueError('订单不存在')

            if row['pay_status'] != 'paid':
                paid_at = now_iso()
                sale_mode = self._normalize_sale_mode(row['sale_mode'])
                order_status = 'crowdfunding_paid' if sale_mode == 'crowdfunding' else 'deposit_paid'
                conn.execute(
                    '''
                    UPDATE orders
                    SET pay_status='paid',
                        order_status=?,
                        payment_channel=?,
                        transaction_id=?,
                        paid_at=?
                    WHERE order_id=?
                    ''',
                    (order_status, payment_channel, transaction_id, paid_at, order_id),
                )
                self._sync_commissions_for_order(conn, order_id=order_id, work_id=row['work_id'])
                if sale_mode == 'crowdfunding':
                    work_row = conn.execute('SELECT * FROM works WHERE work_id=? LIMIT 1', (row['work_id'],)).fetchone()
                    if work_row:
                        self._evaluate_crowdfunding_campaign(conn=conn, work_row=work_row)
                conn.commit()

            refreshed = conn.execute('SELECT * FROM orders WHERE order_id=? LIMIT 1', (order_id,)).fetchone()
            return self._serialize_order_row(refreshed)

    def log_payment(self, order_id: str, mode: str, payload: Dict[str, Any]) -> None:
        with self.lock, self._conn() as conn:
            conn.execute(
                'INSERT INTO payment_logs(order_id, mode, payload_json, created_at) VALUES(?, ?, ?, ?)',
                (order_id, mode, json.dumps(payload, ensure_ascii=False), now_iso()),
            )
            conn.commit()

    def log_admin_action(
        self,
        actor: str,
        action_type: str,
        target_type: str,
        target_id: str,
        related_user_id: int = 0,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.lock, self._conn() as conn:
            conn.execute(
                '''
                INSERT INTO admin_action_logs(
                  actor, action_type, target_type, target_id, related_user_id, detail_json, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    (actor or '').strip()[:64],
                    (action_type or '').strip()[:64],
                    (target_type or '').strip()[:32],
                    (target_id or '').strip()[:128],
                    max(0, int(related_user_id or 0)),
                    json.dumps(detail or {}, ensure_ascii=False),
                    now_iso(),
                ),
            )
            conn.commit()

    def _serialize_admin_action_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        keys = set(row.keys())
        detail_raw = row['detail_json'] if 'detail_json' in keys else '{}'
        try:
            detail = json.loads(detail_raw or '{}')
            if not isinstance(detail, dict):
                detail = {'value': detail}
        except Exception:
            detail = {'raw': str(detail_raw or '')}
        return {
            'id': int(row['id']) if 'id' in keys else 0,
            'actor': row['actor'] if 'actor' in keys else '',
            'action_type': row['action_type'] if 'action_type' in keys else '',
            'target_type': row['target_type'] if 'target_type' in keys else '',
            'target_id': row['target_id'] if 'target_id' in keys else '',
            'related_user_id': int(row['related_user_id'] or 0) if 'related_user_id' in keys else 0,
            'detail': detail,
            'created_at': row['created_at'] if 'created_at' in keys else '',
        }

    def get_order_by_id(self, order_id: str) -> Optional[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM orders WHERE order_id=? LIMIT 1', (order_id,)).fetchone()
            if not row:
                return None
            return self._serialize_order_row(row)

    def set_order_admin_note(self, order_id: str, note: str) -> Dict[str, Any]:
        note = (note or '').strip()[:500]
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM orders WHERE order_id=? LIMIT 1', (order_id,)).fetchone()
            if not row:
                raise ValueError('订单不存在')
            conn.execute('UPDATE orders SET admin_note=? WHERE order_id=?', (note, order_id))
            conn.commit()
            refreshed = conn.execute('SELECT * FROM orders WHERE order_id=? LIMIT 1', (order_id,)).fetchone()
            return self._serialize_order_row(refreshed)

    def list_pending_crowdfunding_refunds(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT *
                FROM orders
                WHERE sale_mode='crowdfunding'
                  AND pay_status='paid'
                  AND refund_status='pending_submit'
                ORDER BY created_at ASC
                LIMIT ?
                ''',
                (max(1, min(limit, 500)),),
            ).fetchall()
            return [self._serialize_order_row(row) for row in rows]

    def mark_order_refund_submitted(self, order_id: str, out_refund_no: str, reason: str = '') -> Dict[str, Any]:
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM orders WHERE order_id=? LIMIT 1', (order_id,)).fetchone()
            if not row:
                raise ValueError('订单不存在')
            if str(row['sale_mode'] or '').strip() != 'crowdfunding':
                raise ValueError('仅众筹订单支持退款提交流程')

            next_reason = (reason or '').strip() or str(row['refund_reason'] or '')
            conn.execute(
                '''
                UPDATE orders
                SET order_status='crowdfunding_refunding',
                    refund_status='processing',
                    refund_reason=?,
                    refund_amount=CASE WHEN refund_amount > 0 THEN refund_amount ELSE paid_amount END
                WHERE order_id=?
                ''',
                (next_reason, order_id),
            )
            conn.execute(
                'INSERT INTO payment_logs(order_id, mode, payload_json, created_at) VALUES(?, ?, ?, ?)',
                (
                    order_id,
                    'refund_submitted',
                    json.dumps({'out_refund_no': out_refund_no}, ensure_ascii=False),
                    now_iso(),
                ),
            )
            conn.commit()

            refreshed = conn.execute('SELECT * FROM orders WHERE order_id=? LIMIT 1', (order_id,)).fetchone()
            return self._serialize_order_row(refreshed)

    def mark_order_refund_by_notify(
        self,
        order_id: str,
        wechat_refund_status: str,
        refund_amount: int = 0,
        refunded_at: str = '',
        reason: str = '',
        out_refund_no: str = '',
        refund_id: str = '',
    ) -> Dict[str, Any]:
        status = str(wechat_refund_status or '').strip().upper()
        if status not in {'SUCCESS', 'PROCESSING', 'CLOSED', 'ABNORMAL'}:
            raise ValueError('微信退款状态不支持')

        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM orders WHERE order_id=? LIMIT 1', (order_id,)).fetchone()
            if not row:
                raise ValueError('订单不存在')

            if status == 'SUCCESS':
                next_refunded_at = (refunded_at or '').strip() or now_iso()
                next_reason = (reason or '').strip() or str(row['refund_reason'] or '')
                next_amount = int(refund_amount or 0)
                if next_amount <= 0:
                    next_amount = int(row['refund_amount'] or 0) or int(row['paid_amount'] or 0)
                conn.execute(
                    '''
                    UPDATE orders
                    SET pay_status='refunded',
                        order_status='crowdfunding_refunded',
                        refund_status='refunded',
                        refunded_at=?,
                        refund_amount=?,
                        refund_reason=?
                    WHERE order_id=?
                    ''',
                    (next_refunded_at, next_amount, next_reason, order_id),
                )
                conn.execute('DELETE FROM designer_commission_records WHERE order_id=?', (order_id,))
            elif status == 'PROCESSING':
                next_amount = int(refund_amount or 0)
                next_reason = (reason or '').strip()
                conn.execute(
                    '''
                    UPDATE orders
                    SET order_status='crowdfunding_refunding',
                        refund_status='processing',
                        refund_amount=CASE WHEN ? > 0 THEN ? ELSE refund_amount END,
                        refund_reason=CASE WHEN ? <> '' THEN ? ELSE refund_reason END
                    WHERE order_id=?
                    ''',
                    (next_amount, next_amount, next_reason, next_reason, order_id),
                )
            else:
                next_reason = (reason or '').strip() or '微信退款失败，请人工处理'
                conn.execute(
                    '''
                    UPDATE orders
                    SET order_status='crowdfunding_refund_failed',
                        refund_status='failed',
                        refund_reason=?
                    WHERE order_id=?
                    ''',
                    (next_reason, order_id),
                )

            conn.execute(
                'INSERT INTO payment_logs(order_id, mode, payload_json, created_at) VALUES(?, ?, ?, ?)',
                (
                    order_id,
                    'refund_notify',
                    json.dumps(
                        {
                            'status': status,
                            'refund_amount': int(refund_amount or 0),
                            'refunded_at': refunded_at,
                            'reason': reason,
                            'out_refund_no': out_refund_no,
                            'refund_id': refund_id,
                        },
                        ensure_ascii=False,
                    ),
                    now_iso(),
                ),
            )
            conn.commit()

            refreshed = conn.execute('SELECT * FROM orders WHERE order_id=? LIMIT 1', (order_id,)).fetchone()
            return self._serialize_order_row(refreshed)

    def list_orders_by_user(self, user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                'SELECT * FROM orders WHERE user_id=? ORDER BY created_at DESC, order_id DESC LIMIT ?',
                (user_id, max(1, min(limit, 200))),
            ).fetchall()
            return [self._serialize_order_row(row) for row in rows]

    def list_orders_by_user_filtered(
        self,
        user_id: int,
        limit: int = 20,
        offset: int = 0,
        status: str = '',
        sale_mode: str = '',
        period_days: int = 0,
    ) -> Dict[str, Any]:
        status = (status or '').strip()
        mode = self._normalize_sale_mode(sale_mode) if sale_mode else ''
        safe_limit = max(1, min(int(limit or 20), 100))
        safe_offset = max(0, int(offset or 0))
        safe_period_days = max(0, min(int(period_days or 0), 3650))

        conditions = ['user_id=?']
        params: List[Any] = [user_id]

        if mode:
            conditions.append('sale_mode=?')
            params.append(mode)

        if status == 'pending':
            conditions.append("pay_status='pending'")
        elif status == 'paid':
            conditions.append("pay_status='paid' AND COALESCE(refund_status, 'none')='none'")
        elif status == 'refunding':
            conditions.append("COALESCE(refund_status, 'none') IN ('pending_submit', 'processing')")
        elif status == 'refunded':
            conditions.append("(pay_status='refunded' OR COALESCE(refund_status, 'none')='refunded')")

        if safe_period_days > 0:
            since = (datetime.now() - timedelta(days=safe_period_days)).strftime('%Y-%m-%d %H:%M:%S')
            conditions.append('created_at >= ?')
            params.append(since)

        where_sql = f"WHERE {' AND '.join(conditions)}"
        with self.lock, self._conn() as conn:
            total_row = conn.execute(f'SELECT COUNT(1) AS c FROM orders {where_sql}', tuple(params)).fetchone()
            total = int(total_row['c'] or 0)
            rows = conn.execute(
                f'''
                SELECT *
                FROM orders
                {where_sql}
                ORDER BY created_at DESC, order_id DESC
                LIMIT ? OFFSET ?
                ''',
                tuple(params + [safe_limit, safe_offset]),
            ).fetchall()
            items = [self._serialize_order_row(row) for row in rows]
            return {
                'items': items,
                'total': total,
                'limit': safe_limit,
                'offset': safe_offset,
                'has_more': (safe_offset + len(items)) < total,
            }

    def _build_designer_qualification(self, submissions: List[Dict[str, Any]]) -> Dict[str, Any]:
        safe_submissions = submissions or []
        total_count = len(safe_submissions)
        approved_count = sum(1 for item in safe_submissions if str(item.get('status') or '') == 'approved')
        pending_count = sum(1 for item in safe_submissions if str(item.get('status') or '') == 'pending')
        rejected_count = sum(1 for item in safe_submissions if str(item.get('status') or '') == 'rejected')
        has_applied = total_count > 0
        can_enroll = approved_count > 0
        latest = safe_submissions[0] if safe_submissions else {}
        latest_status = str(latest.get('status') or '')
        latest_status_text = str(latest.get('status_text') or latest_status)
        message = '已满足开通设计师条件'
        if not has_applied:
            message = '请先提交设计师投稿申请'
        elif not can_enroll:
            message = '你的投稿尚未审核通过，请等待审核结果'
        return {
            'has_applied': has_applied,
            'can_enroll': can_enroll,
            'total_submissions': total_count,
            'approved_submissions': approved_count,
            'pending_submissions': pending_count,
            'rejected_submissions': rejected_count,
            'latest_status': latest_status,
            'latest_status_text': latest_status_text,
            'message': message,
        }

    def get_designer_qualification_by_user(self, user_id: int, limit: int = 200) -> Dict[str, Any]:
        submissions = self.list_submissions_by_user(user_id, limit=max(1, min(limit, 1000)))
        return self._build_designer_qualification(submissions)

    def create_submission(self, user_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        submission_id = f"SUB{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6].upper()}"
        timestamp = now_iso()
        with self.lock, self._conn() as conn:
            conn.execute(
                '''
                INSERT INTO submissions(
                  submission_id, user_id, designer_name, contact, work_name, category,
                  intro, estimated_pieces, image_urls_json, status, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    submission_id,
                    user_id,
                    str(payload.get('designer_name') or ''),
                    str(payload.get('contact') or ''),
                    str(payload.get('work_name') or ''),
                    str(payload.get('category') or ''),
                    str(payload.get('intro') or ''),
                    int(payload.get('estimated_pieces') or 0),
                    json.dumps(payload.get('image_urls') or [], ensure_ascii=False),
                    'pending',
                    timestamp,
                    timestamp,
                ),
            )
            conn.commit()
            row = conn.execute('SELECT * FROM submissions WHERE submission_id=? LIMIT 1', (submission_id,)).fetchone()
            return self._serialize_submission_row(row)

    def _serialize_submission_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        status = row['status']
        return {
            'submission_id': row['submission_id'],
            'user_id': int(row['user_id']),
            'designer_name': row['designer_name'],
            'contact': row['contact'],
            'work_name': row['work_name'],
            'category': row['category'],
            'intro': row['intro'],
            'estimated_pieces': int(row['estimated_pieces']),
            'image_urls': json.loads(row['image_urls_json'] or '[]'),
            'status': status,
            'status_text': SUBMISSION_STATUS_TEXT.get(status, status),
            'review_note': row['review_note'],
            'created_at': row['created_at'],
            'updated_at': row['updated_at'],
        }

    def list_submissions_by_user(self, user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            rows = conn.execute('SELECT * FROM submissions WHERE user_id=? ORDER BY created_at DESC LIMIT ?', (user_id, max(1, min(limit, 200)))).fetchall()
            return [self._serialize_submission_row(row) for row in rows]

    def _serialize_designer_row(self, row: sqlite3.Row, openid: str = '', nickname: str = '') -> Dict[str, Any]:
        status = str(row['status'] or 'active')
        share_ratio = float(row['default_share_ratio'] or DEFAULT_DESIGNER_SHARE)
        return {
            'designer_id': int(row['id']),
            'user_id': int(row['user_id']),
            'openid': openid,
            'nickname': nickname,
            'display_name': row['display_name'] or nickname or '',
            'status': status,
            'status_text': DESIGNER_STATUS_TEXT.get(status, status),
            'default_share_ratio': share_ratio,
            'default_share_percent': round(share_ratio * 100, 2),
            'bio': row['bio'] or '',
            'avatar_url': row['avatar_url'] or '',
            'created_at': row['created_at'],
            'updated_at': row['updated_at'],
        }

    def get_designer_profile_by_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM designers WHERE user_id=? LIMIT 1', (user_id,)).fetchone()
            if not row:
                return None
            user_row = conn.execute('SELECT openid, nickname FROM users WHERE id=? LIMIT 1', (user_id,)).fetchone()
            openid = user_row['openid'] if user_row else ''
            nickname = user_row['nickname'] if user_row else ''
            return self._serialize_designer_row(row, openid=openid, nickname=nickname)

    def get_designer_public_profile(self, designer_id: int) -> Optional[Dict[str, Any]]:
        safe_id = int(designer_id or 0)
        if safe_id <= 0:
            return None
        with self.lock, self._conn() as conn:
            row = conn.execute(
                '''
                SELECT d.*, COALESCE(u.nickname, '') AS nickname
                FROM designers d
                LEFT JOIN users u ON u.id = d.user_id
                WHERE d.id=?
                LIMIT 1
                ''',
                (safe_id,),
            ).fetchone()
            if not row:
                return None
            profile = self._serialize_designer_row(row, openid='', nickname=row['nickname'] or '')
            assignment_rows = conn.execute(
                '''
                SELECT
                  l.work_id,
                  l.share_ratio,
                  l.updated_at,
                  COALESCE(w.name, l.work_id) AS work_name
                FROM designer_work_links l
                LEFT JOIN works w ON w.work_id = l.work_id
                WHERE l.designer_id=? AND l.is_active=1
                ORDER BY l.updated_at DESC
                LIMIT 100
                ''',
                (safe_id,),
            ).fetchall()
            assignments = [
                {
                    'work_id': item['work_id'],
                    'work_name': item['work_name'],
                    'share_ratio': float(item['share_ratio'] or DEFAULT_DESIGNER_SHARE),
                    'share_percent': round(float(item['share_ratio'] or DEFAULT_DESIGNER_SHARE) * 100, 2),
                    'updated_at': item['updated_at'] or '',
                }
                for item in assignment_rows
            ]
        return {'profile': profile, 'assignments': assignments}

    def update_designer_profile(
        self,
        designer_id: int,
        display_name: Optional[str] = None,
        bio: Optional[str] = None,
        avatar_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        safe_id = int(designer_id or 0)
        if safe_id <= 0:
            raise ValueError('设计师ID不合法')
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM designers WHERE id=? LIMIT 1', (safe_id,)).fetchone()
            if not row:
                raise ValueError('设计师不存在')
            current_name = str(row['display_name'] or '').strip()
            next_name = current_name if display_name is None else str(display_name or '').strip()
            if not next_name:
                raise ValueError('设计师名称不能为空')
            if len(next_name) > 40:
                raise ValueError('设计师名称不能超过 40 字')
            next_bio = str(row['bio'] or '') if bio is None else str(bio or '').strip()
            if len(next_bio) > 1000:
                raise ValueError('设计师介绍不能超过 1000 字')
            next_avatar = str(row['avatar_url'] or '') if avatar_url is None else str(avatar_url or '').strip()
            if len(next_avatar) > 500:
                raise ValueError('头像地址长度不能超过 500')
            conn.execute(
                '''
                UPDATE designers
                SET display_name=?, bio=?, avatar_url=?, updated_at=?
                WHERE id=?
                ''',
                (next_name, next_bio, next_avatar, now_iso(), safe_id),
            )
            user_row = conn.execute(
                '''
                SELECT d.*, COALESCE(u.openid, '') AS openid, COALESCE(u.nickname, '') AS nickname
                FROM designers d
                LEFT JOIN users u ON u.id = d.user_id
                WHERE d.id=?
                LIMIT 1
                ''',
                (safe_id,),
            ).fetchone()
            conn.commit()
        return self._serialize_designer_row(user_row, openid=user_row['openid'], nickname=user_row['nickname'])

    def enroll_designer(self, user_id: int, display_name: str = '', bio: str = '') -> Dict[str, Any]:
        qualification = self.get_designer_qualification_by_user(user_id=user_id, limit=200)
        if not qualification.get('has_applied'):
            raise ValueError('请先提交设计师投稿申请，审核通过后再开通')
        if not qualification.get('can_enroll'):
            raise ValueError('你的投稿尚未审核通过，暂时不能开通设计师入口')

        timestamp = now_iso()
        display_name = (display_name or '').strip()
        bio = (bio or '').strip()

        with self.lock, self._conn() as conn:
            user_row = conn.execute('SELECT nickname FROM users WHERE id=? LIMIT 1', (user_id,)).fetchone()
            nickname = user_row['nickname'] if user_row else ''
            approved_submission = conn.execute(
                '''
                SELECT designer_name
                FROM submissions
                WHERE user_id=? AND status='approved'
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                ''',
                (user_id,),
            ).fetchone()
            approved_name = (approved_submission['designer_name'] if approved_submission else '') or ''
            row = conn.execute('SELECT * FROM designers WHERE user_id=? LIMIT 1', (user_id,)).fetchone()

            if row:
                next_display = display_name or row['display_name'] or approved_name or nickname
                next_bio = bio or row['bio']
                conn.execute(
                    '''
                    UPDATE designers
                    SET display_name=?, bio=?, status='active', updated_at=?
                    WHERE user_id=?
                    ''',
                    (next_display, next_bio, timestamp, user_id),
                )
            else:
                next_display = display_name or approved_name or nickname or f'设计师{user_id}'
                conn.execute(
                    '''
                    INSERT INTO designers(
                      user_id, display_name, status, default_share_ratio, bio, created_at, updated_at
                    ) VALUES(?, ?, 'active', ?, ?, ?, ?)
                    ''',
                    (user_id, next_display, DEFAULT_DESIGNER_SHARE, bio, timestamp, timestamp),
                )

            conn.commit()

        return self.get_designer_profile_by_user(user_id) or {}

    def _list_designer_assignments(self, designer_id: int) -> List[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT l.*, COALESCE(w.name, l.work_id) AS work_name
                FROM designer_work_links l
                LEFT JOIN works w ON w.work_id = l.work_id
                WHERE l.designer_id=? AND l.is_active=1
                ORDER BY l.updated_at DESC
                ''',
                (designer_id,),
            ).fetchall()
            return [
                {
                    'work_id': row['work_id'],
                    'work_name': row['work_name'],
                    'share_ratio': float(row['share_ratio'] or DEFAULT_DESIGNER_SHARE),
                    'share_percent': round(float(row['share_ratio'] or DEFAULT_DESIGNER_SHARE) * 100, 2),
                    'updated_at': row['updated_at'],
                }
                for row in rows
            ]

    def bind_designer_work(self, openid: str, work_id: str, share_ratio: float = DEFAULT_DESIGNER_SHARE) -> Dict[str, Any]:
        openid = (openid or '').strip()
        work_id = (work_id or '').strip()
        if not openid:
            raise ValueError('openid 不能为空')
        if not work_id:
            raise ValueError('work_id 不能为空')
        if share_ratio <= 0 or share_ratio > 1:
            raise ValueError('分成比例必须在 0-1 之间')

        work = self.get_work_by_id(work_id)
        if not work:
            raise ValueError('作品不存在')

        user = self.get_user_by_openid(openid)
        if not user:
            raise ValueError('未找到该 openid 对应用户，请让设计师先登录一次小程序')

        profile = self.enroll_designer(user_id=user['user_id'])
        designer_id = int(profile['designer_id'])
        timestamp = now_iso()

        with self.lock, self._conn() as conn:
            exists = conn.execute(
                'SELECT id FROM designer_work_links WHERE designer_id=? AND work_id=? LIMIT 1',
                (designer_id, work_id),
            ).fetchone()
            if exists:
                conn.execute(
                    '''
                    UPDATE designer_work_links
                    SET share_ratio=?, is_active=1, updated_at=?
                    WHERE designer_id=? AND work_id=?
                    ''',
                    (float(share_ratio), timestamp, designer_id, work_id),
                )
            else:
                conn.execute(
                    '''
                    INSERT INTO designer_work_links(
                      designer_id, work_id, share_ratio, is_active, created_at, updated_at
                    ) VALUES(?, ?, ?, 1, ?, ?)
                    ''',
                    (designer_id, work_id, float(share_ratio), timestamp, timestamp),
                )

            # 分成比例变更时，未结算记录同步新比例
            conn.execute(
                '''
                UPDATE designer_commission_records
                SET share_ratio=?,
                    commission_amount=ROUND((
                      SELECT o.total_amount FROM orders o WHERE o.order_id = designer_commission_records.order_id
                    ) * ?, 2),
                    updated_at=?
                WHERE designer_id=?
                  AND work_id=?
                  AND settlement_status='pending'
                ''',
                (float(share_ratio), float(share_ratio), timestamp, designer_id, work_id),
            )

            # 补齐历史已支付订单佣金记录
            conn.execute(
                '''
                INSERT INTO designer_commission_records(
                  designer_id, order_id, work_id, share_ratio, commission_amount,
                  settlement_status, settlement_note, settled_at, created_at, updated_at
                )
                SELECT
                  ?, o.order_id, o.work_id, ?, ROUND(o.total_amount * ?, 2),
                  'pending', '', '', ?, ?
                FROM orders o
                WHERE o.work_id=?
                  AND o.pay_status='paid'
                  AND COALESCE(o.refund_status, 'none')='none'
                  AND NOT EXISTS(
                    SELECT 1 FROM designer_commission_records c
                    WHERE c.designer_id=? AND c.order_id=o.order_id
                  )
                ''',
                (
                    designer_id,
                    float(share_ratio),
                    float(share_ratio),
                    timestamp,
                    timestamp,
                    work_id,
                    designer_id,
                ),
            )

            conn.commit()

        return {
            'designer': self.get_designer_profile_by_user(user['user_id']),
            'assignments': self._list_designer_assignments(designer_id),
        }

    def list_designers(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT d.*, u.openid, u.nickname
                FROM designers d
                JOIN users u ON u.id = d.user_id
                ORDER BY d.updated_at DESC
                LIMIT ?
                ''',
                (max(1, min(limit, 1000)),),
            ).fetchall()

            result: List[Dict[str, Any]] = []
            for row in rows:
                base = self._serialize_designer_row(row, openid=row['openid'], nickname=row['nickname'])
                base['assignments'] = self._list_designer_assignments(int(row['id']))
                result.append(base)
            return result

    def list_work_updates_public(self, work_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT
                  u.update_id,
                  u.work_id,
                  u.title,
                  u.content,
                  u.created_at,
                  d.display_name,
                  us.nickname,
                  COALESCE(w.name, u.work_id) AS work_name
                FROM designer_updates u
                JOIN designers d ON d.id = u.designer_id
                LEFT JOIN users us ON us.id = d.user_id
                LEFT JOIN works w ON w.work_id = u.work_id
                WHERE u.work_id=?
                ORDER BY u.created_at DESC
                LIMIT ?
                ''',
                (work_id, max(1, min(limit, 100))),
            ).fetchall()

            return [
                {
                    'update_id': row['update_id'],
                    'work_id': row['work_id'],
                    'work_name': row['work_name'],
                    'title': row['title'],
                    'content': row['content'],
                    'designer_name': row['display_name'] or row['nickname'] or '设计师',
                    'created_at': row['created_at'],
                }
                for row in rows
            ]

    def _serialize_project_comment_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        keys = set(row.keys())
        user_id = int(row['user_id']) if 'user_id' in keys else 0
        user_nickname = (row['user_nickname'] if 'user_nickname' in keys else '') or ''
        reply_nickname = (row['reply_designer_nickname'] if 'reply_designer_nickname' in keys else '') or ''
        reply_display = (row['reply_designer_name'] if 'reply_designer_name' in keys else '') or ''
        return {
            'comment_id': row['comment_id'],
            'work_id': row['work_id'],
            'work_name': row['work_name'] if 'work_name' in keys else '',
            'user_id': user_id,
            'user_nickname': user_nickname or f'玩家{user_id}',
            'content': row['content'] or '',
            'designer_reply': row['designer_reply'] or '',
            'reply_designer_id': int(row['reply_designer_id'] or 0),
            'reply_designer_name': reply_display or reply_nickname or '',
            'reply_at': row['reply_at'] or '',
            'created_at': row['created_at'] or '',
            'updated_at': row['updated_at'] or '',
        }

    def list_project_comments_public(self, work_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        safe_work_id = (work_id or '').strip()
        if not safe_work_id:
            return []
        safe_limit = max(1, min(int(limit or 50), 200))
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT
                  c.*,
                  COALESCE(w.name, c.work_id) AS work_name,
                  COALESCE(u.nickname, '') AS user_nickname,
                  COALESCE(rd.display_name, '') AS reply_designer_name,
                  COALESCE(ru.nickname, '') AS reply_designer_nickname
                FROM project_comments c
                LEFT JOIN works w ON w.work_id = c.work_id
                LEFT JOIN users u ON u.id = c.user_id
                LEFT JOIN designers rd ON rd.id = c.reply_designer_id
                LEFT JOIN users ru ON ru.id = rd.user_id
                WHERE c.work_id=?
                ORDER BY c.created_at DESC, c.comment_id DESC
                LIMIT ?
                ''',
                (safe_work_id, safe_limit),
            ).fetchall()
        return [self._serialize_project_comment_row(row) for row in rows]

    def create_project_comment(self, user_id: int, work_id: str, content: str) -> Dict[str, Any]:
        safe_work_id = (work_id or '').strip()
        if not safe_work_id:
            raise ValueError('作品ID不能为空')
        safe_content = (content or '').strip()
        if not safe_content:
            raise ValueError('评论内容不能为空')
        if len(safe_content) > 300:
            raise ValueError('评论内容不能超过 300 字')

        comment_id = f"CMT{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:8].upper()}"
        ts = now_iso()
        with self.lock, self._conn() as conn:
            work_row = conn.execute('SELECT work_id FROM works WHERE work_id=? LIMIT 1', (safe_work_id,)).fetchone()
            if not work_row:
                raise ValueError('作品不存在')
            conn.execute(
                '''
                INSERT INTO project_comments(
                  comment_id, work_id, user_id, content, designer_reply, reply_designer_id, reply_at, created_at, updated_at
                ) VALUES(?, ?, ?, ?, '', 0, '', ?, ?)
                ''',
                (comment_id, safe_work_id, int(user_id), safe_content, ts, ts),
            )
            row = conn.execute(
                '''
                SELECT
                  c.*,
                  COALESCE(w.name, c.work_id) AS work_name,
                  COALESCE(u.nickname, '') AS user_nickname,
                  '' AS reply_designer_name,
                  '' AS reply_designer_nickname
                FROM project_comments c
                LEFT JOIN works w ON w.work_id = c.work_id
                LEFT JOIN users u ON u.id = c.user_id
                WHERE c.comment_id=?
                LIMIT 1
                ''',
                (comment_id,),
            ).fetchone()
            conn.commit()
        return self._serialize_project_comment_row(row)

    def list_designer_comments(self, designer_id: int, work_id: str = '', limit: int = 100) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 100), 300))
        safe_work_id = (work_id or '').strip()
        with self.lock, self._conn() as conn:
            if safe_work_id:
                link = conn.execute(
                    '''
                    SELECT id
                    FROM designer_work_links
                    WHERE designer_id=? AND work_id=? AND is_active=1
                    LIMIT 1
                    ''',
                    (int(designer_id), safe_work_id),
                ).fetchone()
                if not link:
                    raise ValueError('该作品未绑定到你的设计师账号')
                rows = conn.execute(
                    '''
                    SELECT
                      c.*,
                      COALESCE(w.name, c.work_id) AS work_name,
                      COALESCE(u.nickname, '') AS user_nickname,
                      COALESCE(rd.display_name, '') AS reply_designer_name,
                      COALESCE(ru.nickname, '') AS reply_designer_nickname
                    FROM project_comments c
                    LEFT JOIN works w ON w.work_id = c.work_id
                    LEFT JOIN users u ON u.id = c.user_id
                    LEFT JOIN designers rd ON rd.id = c.reply_designer_id
                    LEFT JOIN users ru ON ru.id = rd.user_id
                    WHERE c.work_id=?
                    ORDER BY c.created_at DESC, c.comment_id DESC
                    LIMIT ?
                    ''',
                    (safe_work_id, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    '''
                    SELECT
                      c.*,
                      COALESCE(w.name, c.work_id) AS work_name,
                      COALESCE(u.nickname, '') AS user_nickname,
                      COALESCE(rd.display_name, '') AS reply_designer_name,
                      COALESCE(ru.nickname, '') AS reply_designer_nickname
                    FROM project_comments c
                    LEFT JOIN works w ON w.work_id = c.work_id
                    LEFT JOIN users u ON u.id = c.user_id
                    LEFT JOIN designers rd ON rd.id = c.reply_designer_id
                    LEFT JOIN users ru ON ru.id = rd.user_id
                    WHERE EXISTS(
                      SELECT 1 FROM designer_work_links l
                      WHERE l.designer_id=? AND l.work_id=c.work_id AND l.is_active=1
                    )
                    ORDER BY c.created_at DESC, c.comment_id DESC
                    LIMIT ?
                    ''',
                    (int(designer_id), safe_limit),
                ).fetchall()
        return [self._serialize_project_comment_row(row) for row in rows]

    def reply_project_comment(self, designer_id: int, comment_id: str, reply_content: str) -> Dict[str, Any]:
        safe_comment_id = (comment_id or '').strip()
        if not safe_comment_id:
            raise ValueError('评论ID不能为空')
        safe_reply = (reply_content or '').strip()
        if not safe_reply:
            raise ValueError('回复内容不能为空')
        if len(safe_reply) > 500:
            raise ValueError('回复内容不能超过 500 字')

        ts = now_iso()
        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM project_comments WHERE comment_id=? LIMIT 1', (safe_comment_id,)).fetchone()
            if not row:
                raise ValueError('评论不存在')
            work_id = str(row['work_id'] or '')
            link = conn.execute(
                '''
                SELECT id
                FROM designer_work_links
                WHERE designer_id=? AND work_id=? AND is_active=1
                LIMIT 1
                ''',
                (int(designer_id), work_id),
            ).fetchone()
            if not link:
                raise ValueError('该评论所属作品未绑定到你的设计师账号')

            conn.execute(
                '''
                UPDATE project_comments
                SET designer_reply=?, reply_designer_id=?, reply_at=?, updated_at=?
                WHERE comment_id=?
                ''',
                (safe_reply, int(designer_id), ts, ts, safe_comment_id),
            )
            refreshed = conn.execute(
                '''
                SELECT
                  c.*,
                  COALESCE(w.name, c.work_id) AS work_name,
                  COALESCE(u.nickname, '') AS user_nickname,
                  COALESCE(rd.display_name, '') AS reply_designer_name,
                  COALESCE(ru.nickname, '') AS reply_designer_nickname
                FROM project_comments c
                LEFT JOIN works w ON w.work_id = c.work_id
                LEFT JOIN users u ON u.id = c.user_id
                LEFT JOIN designers rd ON rd.id = c.reply_designer_id
                LEFT JOIN users ru ON ru.id = rd.user_id
                WHERE c.comment_id=?
                LIMIT 1
                ''',
                (safe_comment_id,),
            ).fetchone()
            conn.commit()
        return self._serialize_project_comment_row(refreshed)

    def list_designer_projects(self, designer_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 100), 200))
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT w.*
                FROM works w
                JOIN designer_work_links l ON l.work_id = w.work_id
                WHERE l.designer_id=? AND l.is_active=1
                ORDER BY w.is_current DESC, w.updated_at DESC
                LIMIT ?
                ''',
                (int(designer_id), safe_limit),
            ).fetchall()
            items: List[Dict[str, Any]] = []
            for row in rows:
                mode = self._normalize_sale_mode(row['sale_mode'])
                metrics = self._compute_work_metrics(
                    conn=conn,
                    work_id=str(row['work_id']),
                    sale_mode=mode,
                    crowdfunding_goal_amount=int(row['crowdfunding_goal_amount'] or 0),
                )
                items.append(self._serialize_work_row(row, metrics=metrics))
            return items

    def designer_update_project(self, designer_id: int, work_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        safe_work_id = (work_id or '').strip()
        if not safe_work_id:
            raise ValueError('项目ID不能为空')
        with self.lock, self._conn() as conn:
            link = conn.execute(
                '''
                SELECT id
                FROM designer_work_links
                WHERE designer_id=? AND work_id=? AND is_active=1
                LIMIT 1
                ''',
                (int(designer_id), safe_work_id),
            ).fetchone()
            if not link:
                raise ValueError('该项目未绑定到你的设计师账号')

            row = conn.execute('SELECT * FROM works WHERE work_id=? LIMIT 1', (safe_work_id,)).fetchone()
            if not row:
                raise ValueError('项目不存在')

            current_name = str(row['name'] or '')
            current_subtitle = str(row['subtitle'] or '')
            current_story = str(row['story'] or '')
            current_cover = str(row['cover_image'] or '')
            try:
                current_gallery = json.loads(row['gallery_json'] or '[]')
                if not isinstance(current_gallery, list):
                    current_gallery = []
            except Exception:
                current_gallery = []
            try:
                current_highlights = json.loads(row['highlights_json'] or '[]')
                if not isinstance(current_highlights, list):
                    current_highlights = []
            except Exception:
                current_highlights = []
            try:
                current_specs = json.loads(row['specs_json'] or '[]')
                if not isinstance(current_specs, list):
                    current_specs = []
            except Exception:
                current_specs = []

            next_name = str(payload.get('name') if 'name' in payload else current_name).strip() or current_name
            next_subtitle = str(payload.get('subtitle') if 'subtitle' in payload else current_subtitle).strip()
            next_story = str(payload.get('story') if 'story' in payload else current_story).strip() or current_story
            next_cover = str(payload.get('cover_image') if 'cover_image' in payload else current_cover).strip()
            gallery_raw = payload.get('gallery_images') if 'gallery_images' in payload else current_gallery
            highlights_raw = payload.get('highlights') if 'highlights' in payload else current_highlights
            specs_raw = payload.get('specs') if 'specs' in payload else current_specs

            if isinstance(gallery_raw, str):
                next_gallery = [x.strip() for x in gallery_raw.split('\n') if x.strip()]
            elif isinstance(gallery_raw, list):
                next_gallery = [str(x).strip() for x in gallery_raw if str(x).strip()]
            else:
                raise ValueError('gallery_images 必须是数组')
            next_gallery = next_gallery[:12]

            if not isinstance(highlights_raw, list):
                raise ValueError('highlights 必须是数组')
            next_highlights = [str(x).strip() for x in highlights_raw if str(x).strip()][:12]

            if not isinstance(specs_raw, list):
                raise ValueError('specs 必须是数组')
            next_specs = specs_raw

            conn.execute(
                '''
                UPDATE works
                SET name=?, subtitle=?, story=?, cover_image=?, gallery_json=?, highlights_json=?, specs_json=?, updated_at=?
                WHERE work_id=?
                ''',
                (
                    next_name,
                    next_subtitle,
                    next_story,
                    next_cover,
                    json.dumps(next_gallery, ensure_ascii=False),
                    json.dumps(next_highlights, ensure_ascii=False),
                    json.dumps(next_specs, ensure_ascii=False),
                    now_iso(),
                    safe_work_id,
                ),
            )
            refreshed = conn.execute('SELECT * FROM works WHERE work_id=? LIMIT 1', (safe_work_id,)).fetchone()
            mode = self._normalize_sale_mode(refreshed['sale_mode'])
            metrics = self._compute_work_metrics(
                conn=conn,
                work_id=safe_work_id,
                sale_mode=mode,
                crowdfunding_goal_amount=int(refreshed['crowdfunding_goal_amount'] or 0),
            )
            conn.commit()
        return self._serialize_work_row(refreshed, metrics=metrics)

    def create_designer_update(self, designer_id: int, work_id: str, title: str, content: str) -> Dict[str, Any]:
        work_id = (work_id or '').strip()
        title = (title or '').strip()
        content = (content or '').strip()

        if not work_id:
            raise ValueError('请先选择作品')
        if not title or not content:
            raise ValueError('标题和内容不能为空')
        if len(title) > 80:
            raise ValueError('标题不能超过 80 字')
        if len(content) > 1000:
            raise ValueError('内容不能超过 1000 字')

        with self.lock, self._conn() as conn:
            link = conn.execute(
                'SELECT id FROM designer_work_links WHERE designer_id=? AND work_id=? AND is_active=1 LIMIT 1',
                (designer_id, work_id),
            ).fetchone()
            if not link:
                raise ValueError('该作品未绑定到你的设计师账号')

            update_id = f"UPD{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6].upper()}"
            conn.execute(
                'INSERT INTO designer_updates(update_id, designer_id, work_id, title, content, created_at) VALUES(?, ?, ?, ?, ?, ?)',
                (update_id, designer_id, work_id, title, content, now_iso()),
            )
            conn.commit()

            row = conn.execute(
                '''
                SELECT u.*, COALESCE(w.name, u.work_id) AS work_name
                FROM designer_updates u
                LEFT JOIN works w ON w.work_id=u.work_id
                WHERE u.update_id=?
                LIMIT 1
                ''',
                (update_id,),
            ).fetchone()

            return {
                'update_id': row['update_id'],
                'work_id': row['work_id'],
                'work_name': row['work_name'],
                'title': row['title'],
                'content': row['content'],
                'created_at': row['created_at'],
            }

    def list_designer_updates(self, designer_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT u.*, COALESCE(w.name, u.work_id) AS work_name
                FROM designer_updates u
                LEFT JOIN works w ON w.work_id = u.work_id
                WHERE u.designer_id=?
                  AND EXISTS (
                    SELECT 1 FROM designer_work_links l
                    WHERE l.designer_id=u.designer_id AND l.work_id=u.work_id AND l.is_active=1
                  )
                ORDER BY u.created_at DESC
                LIMIT ?
                ''',
                (designer_id, max(1, min(limit, 200))),
            ).fetchall()

            return [
                {
                    'update_id': row['update_id'],
                    'work_id': row['work_id'],
                    'work_name': row['work_name'],
                    'title': row['title'],
                    'content': row['content'],
                    'created_at': row['created_at'],
                }
                for row in rows
            ]

    def _serialize_commission_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        status = row['settlement_status']
        keys = set(row.keys())
        return {
            'record_id': int(row['record_id']),
            'designer_id': int(row['designer_id']),
            'designer_user_id': int(row['designer_user_id']) if 'designer_user_id' in keys else 0,
            'display_name': row['display_name'],
            'openid': row['openid'],
            'order_id': row['order_id'],
            'work_id': row['work_id'],
            'work_name': row['work_name'],
            'sku_name': row['sku_name'],
            'quantity': int(row['quantity'] or 0),
            'total_amount': int(row['total_amount'] or 0),
            'share_ratio': float(row['share_ratio'] or DEFAULT_DESIGNER_SHARE),
            'share_percent': round(float(row['share_ratio'] or DEFAULT_DESIGNER_SHARE) * 100, 2),
            'commission_amount': round(float(row['commission_amount'] or 0), 2),
            'settlement_status': status,
            'settlement_status_text': SETTLEMENT_STATUS_TEXT.get(status, status),
            'settlement_note': row['settlement_note'] or '',
            'settled_at': row['settled_at'] or '',
            'paid_at': row['paid_at'] or '',
            'created_at': row['created_at'],
            'updated_at': row['updated_at'],
        }

    def list_designer_orders(self, designer_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        self._ensure_commission_records()
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT
                  c.record_id,
                  c.designer_id,
                  c.order_id,
                  c.work_id,
                  c.share_ratio,
                  c.commission_amount,
                  c.settlement_status,
                  c.settlement_note,
                  c.settled_at,
                  c.created_at,
                  c.updated_at,
                  o.work_name,
                  o.sku_name,
                  o.quantity,
                  o.total_amount,
                  o.paid_at,
                  d.display_name,
                  u.openid,
                  d.user_id AS designer_user_id
                FROM designer_commission_records c
                JOIN orders o ON o.order_id = c.order_id
                JOIN designers d ON d.id = c.designer_id
                LEFT JOIN users u ON u.id = d.user_id
                WHERE c.designer_id=?
                ORDER BY o.paid_at DESC, o.created_at DESC
                LIMIT ?
                ''',
                (designer_id, max(1, min(limit, 300))),
            ).fetchall()

            return [self._serialize_commission_row(row) for row in rows]

    def _designer_sales_summary(self, designer_id: int) -> Dict[str, Any]:
        self._ensure_commission_records()
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT
                  c.work_id,
                  COALESCE(w.name, c.work_id) AS work_name,
                  c.share_ratio,
                  COUNT(c.record_id) AS paid_orders,
                  COALESCE(SUM(o.quantity), 0) AS paid_units,
                  COALESCE(SUM(o.total_amount), 0) AS total_sales_amount,
                  COALESCE(SUM(c.commission_amount), 0) AS total_commission_amount,
                  COALESCE(SUM(CASE WHEN c.settlement_status='pending' THEN c.commission_amount ELSE 0 END), 0) AS pending_commission_amount,
                  COALESCE(SUM(CASE WHEN c.settlement_status='settled' THEN c.commission_amount ELSE 0 END), 0) AS settled_commission_amount,
                  COALESCE(SUM(o.deposit_amount), 0) AS total_deposit_amount
                FROM designer_commission_records c
                JOIN orders o ON o.order_id = c.order_id
                LEFT JOIN works w ON w.work_id = c.work_id
                WHERE c.designer_id=?
                GROUP BY c.work_id, work_name, c.share_ratio
                ORDER BY MAX(c.updated_at) DESC
                ''',
                (designer_id,),
            ).fetchall()

        by_work: List[Dict[str, Any]] = []
        paid_orders_count = 0
        paid_units = 0
        total_sales_amount = 0
        total_deposit_amount = 0
        estimated_commission_amount = 0.0
        pending_commission_amount = 0.0
        settled_commission_amount = 0.0

        for row in rows:
            item = {
                'work_id': row['work_id'],
                'work_name': row['work_name'],
                'share_ratio': float(row['share_ratio'] or DEFAULT_DESIGNER_SHARE),
                'share_percent': round(float(row['share_ratio'] or DEFAULT_DESIGNER_SHARE) * 100, 2),
                'paid_orders': int(row['paid_orders'] or 0),
                'paid_units': int(row['paid_units'] or 0),
                'total_sales_amount': int(row['total_sales_amount'] or 0),
                'total_deposit_amount': int(row['total_deposit_amount'] or 0),
                'estimated_commission_amount': round(float(row['total_commission_amount'] or 0), 2),
                'pending_commission_amount': round(float(row['pending_commission_amount'] or 0), 2),
                'settled_commission_amount': round(float(row['settled_commission_amount'] or 0), 2),
            }
            by_work.append(item)

            paid_orders_count += item['paid_orders']
            paid_units += item['paid_units']
            total_sales_amount += item['total_sales_amount']
            total_deposit_amount += item['total_deposit_amount']
            estimated_commission_amount += item['estimated_commission_amount']
            pending_commission_amount += item['pending_commission_amount']
            settled_commission_amount += item['settled_commission_amount']

        return {
            'paid_orders_count': paid_orders_count,
            'paid_units': paid_units,
            'total_sales_amount': total_sales_amount,
            'total_deposit_amount': total_deposit_amount,
            'estimated_commission_amount': round(estimated_commission_amount, 2),
            'pending_commission_amount': round(pending_commission_amount, 2),
            'settled_commission_amount': round(settled_commission_amount, 2),
            'pending_orders_count': sum(1 for x in by_work if x['pending_commission_amount'] > 0),
            'settled_orders_count': sum(1 for x in by_work if x['settled_commission_amount'] > 0),
            'by_work': by_work,
        }

    def set_commission_settlement(self, record_id: int, status: str, note: str = '') -> Dict[str, Any]:
        status = (status or '').strip()
        if status not in {'pending', 'settled'}:
            raise ValueError('结算状态非法')

        with self.lock, self._conn() as conn:
            row = conn.execute(
                '''
                SELECT c.*, o.work_name, o.sku_name, o.quantity, o.total_amount, o.paid_at,
                       d.display_name, u.openid, d.user_id AS designer_user_id
                FROM designer_commission_records c
                JOIN orders o ON o.order_id = c.order_id
                JOIN designers d ON d.id = c.designer_id
                LEFT JOIN users u ON u.id = d.user_id
                WHERE c.record_id=?
                LIMIT 1
                ''',
                (int(record_id),),
            ).fetchone()
            if not row:
                raise ValueError('分成记录不存在')

            settled_at = now_iso() if status == 'settled' else ''
            conn.execute(
                '''
                UPDATE designer_commission_records
                SET settlement_status=?, settlement_note=?, settled_at=?, updated_at=?
                WHERE record_id=?
                ''',
                (status, (note or '').strip(), settled_at, now_iso(), int(record_id)),
            )
            conn.commit()

            new_row = conn.execute(
                '''
                SELECT c.*, o.work_name, o.sku_name, o.quantity, o.total_amount, o.paid_at,
                       d.display_name, u.openid, d.user_id AS designer_user_id
                FROM designer_commission_records c
                JOIN orders o ON o.order_id = c.order_id
                JOIN designers d ON d.id = c.designer_id
                LEFT JOIN users u ON u.id = d.user_id
                WHERE c.record_id=?
                LIMIT 1
                ''',
                (int(record_id),),
            ).fetchone()
            return self._serialize_commission_row(new_row)

    def set_commission_settlement_batch(
        self,
        status: str,
        note: str = '',
        record_ids: Optional[List[int]] = None,
        from_status: str = '',
        limit: int = 1000,
    ) -> Dict[str, Any]:
        status = (status or '').strip()
        from_status = (from_status or '').strip()
        if status not in {'pending', 'settled'}:
            raise ValueError('结算状态非法')
        if from_status and from_status not in {'pending', 'settled'}:
            raise ValueError('筛选状态非法')

        ids: List[int] = []
        if record_ids:
            seen = set()
            for raw in record_ids:
                try:
                    rid = int(raw)
                except Exception:
                    continue
                if rid <= 0 or rid in seen:
                    continue
                seen.add(rid)
                ids.append(rid)

        safe_limit = max(1, min(int(limit or 1000), 5000))

        with self.lock, self._conn() as conn:
            target_ids: List[int] = []
            if ids:
                placeholders = ','.join('?' for _ in ids)
                query = f'SELECT record_id FROM designer_commission_records WHERE record_id IN ({placeholders})'
                params: List[Any] = list(ids)
                if from_status:
                    query += ' AND settlement_status=?'
                    params.append(from_status)
                query += ' ORDER BY updated_at DESC LIMIT ?'
                params.append(safe_limit)
                rows = conn.execute(query, tuple(params)).fetchall()
                target_ids = [int(row['record_id']) for row in rows]
            else:
                if not from_status:
                    raise ValueError('未传 record_ids 时必须提供 from_status')
                rows = conn.execute(
                    '''
                    SELECT record_id
                    FROM designer_commission_records
                    WHERE settlement_status=?
                    ORDER BY updated_at DESC
                    LIMIT ?
                    ''',
                    (from_status, safe_limit),
                ).fetchall()
                target_ids = [int(row['record_id']) for row in rows]

            if not target_ids:
                return {'affected_count': 0, 'items': []}

            ts = now_iso()
            settled_at = ts if status == 'settled' else ''
            placeholders = ','.join('?' for _ in target_ids)
            conn.execute(
                f'''
                UPDATE designer_commission_records
                SET settlement_status=?, settlement_note=?, settled_at=?, updated_at=?
                WHERE record_id IN ({placeholders})
                ''',
                tuple([status, (note or '').strip(), settled_at, ts] + target_ids),
            )
            conn.commit()

            rows = conn.execute(
                f'''
                SELECT c.*, o.work_name, o.sku_name, o.quantity, o.total_amount, o.paid_at,
                       d.display_name, u.openid, d.user_id AS designer_user_id
                FROM designer_commission_records c
                JOIN orders o ON o.order_id = c.order_id
                JOIN designers d ON d.id = c.designer_id
                LEFT JOIN users u ON u.id = d.user_id
                WHERE c.record_id IN ({placeholders})
                ORDER BY c.updated_at DESC
                ''',
                tuple(target_ids),
            ).fetchall()
            items = [self._serialize_commission_row(row) for row in rows]
            return {'affected_count': len(target_ids), 'items': items}

    def admin_list_commissions(self, status: str = '', limit: int = 200) -> List[Dict[str, Any]]:
        self._ensure_commission_records()
        with self.lock, self._conn() as conn:
            query = (
                '''
                SELECT c.*, o.work_name, o.sku_name, o.quantity, o.total_amount, o.paid_at,
                       d.display_name, u.openid
                FROM designer_commission_records c
                JOIN orders o ON o.order_id = c.order_id
                JOIN designers d ON d.id = c.designer_id
                LEFT JOIN users u ON u.id = d.user_id
                '''
            )
            params: Tuple[Any, ...]
            if status:
                query += ' WHERE c.settlement_status=? '
                params = (status, max(1, min(limit, 3000)))
            else:
                params = (max(1, min(limit, 3000)),)
            query += ' ORDER BY c.updated_at DESC LIMIT ?'
            rows = conn.execute(query, params).fetchall()
            return [self._serialize_commission_row(row) for row in rows]

    def admin_export_commissions_csv(self, status: str = '', limit: int = 5000) -> str:
        items = self.admin_list_commissions(status=status, limit=limit)
        buff = io.StringIO()
        writer = csv.writer(buff)
        writer.writerow(
            [
                'record_id',
                'designer_name',
                'openid',
                'order_id',
                'work_id',
                'work_name',
                'sku_name',
                'quantity',
                'total_amount',
                'share_percent',
                'commission_amount',
                'settlement_status',
                'settlement_note',
                'paid_at',
                'settled_at',
                'updated_at',
            ]
        )
        for item in items:
            writer.writerow(
                [
                    item['record_id'],
                    item['display_name'],
                    item['openid'],
                    item['order_id'],
                    item['work_id'],
                    item['work_name'],
                    item['sku_name'],
                    item['quantity'],
                    item['total_amount'],
                    item['share_percent'],
                    item['commission_amount'],
                    item['settlement_status_text'],
                    item['settlement_note'],
                    item['paid_at'],
                    item['settled_at'],
                    item['updated_at'],
                ]
            )
        return buff.getvalue()

    def get_designer_dashboard_by_user(self, user_id: int) -> Dict[str, Any]:
        profile = self.get_designer_profile_by_user(user_id)
        if not profile:
            return {
                'is_designer': False,
                'profile': None,
                'assignments': [],
                'sales': {
                    'paid_orders_count': 0,
                    'paid_units': 0,
                    'total_sales_amount': 0,
                    'total_deposit_amount': 0,
                    'estimated_commission_amount': 0,
                    'pending_commission_amount': 0,
                    'settled_commission_amount': 0,
                    'pending_orders_count': 0,
                    'settled_orders_count': 0,
                    'by_work': [],
                },
                'updates': [],
            }

        designer_id = int(profile['designer_id'])
        assignments = self._list_designer_assignments(designer_id)
        sales = self._designer_sales_summary(designer_id)
        updates = self.list_designer_updates(designer_id, limit=20)
        return {
            'is_designer': True,
            'profile': profile,
            'assignments': assignments,
            'sales': sales,
            'updates': updates,
        }

    def get_my_summary(self, user_id: int) -> Dict[str, Any]:
        work = self.get_current_work()
        designer_dashboard = self.get_designer_dashboard_by_user(user_id)
        submissions = self.list_submissions_by_user(user_id, limit=20)
        designer_qualification = self._build_designer_qualification(submissions)
        orders = self.list_orders_by_user(user_id, limit=60)
        order_overview = {
            'total_orders': len(orders),
            'paid_orders': sum(1 for x in orders if x.get('pay_status') == 'paid' and (x.get('refund_status') or 'none') == 'none'),
            'pending_orders': sum(1 for x in orders if x.get('pay_status') == 'pending'),
            'refunding_orders': sum(1 for x in orders if (x.get('refund_status') or 'none') in {'pending_submit', 'processing'}),
            'refunded_orders': sum(
                1
                for x in orders
                if x.get('pay_status') == 'refunded' or (x.get('refund_status') or 'none') == 'refunded'
            ),
            'paid_amount': int(
                sum(int(x.get('paid_amount') or 0) for x in orders if x.get('pay_status') == 'paid' and (x.get('refund_status') or 'none') == 'none')
            ),
            'refund_amount': int(
                sum(
                    int(x.get('refund_amount') or 0)
                    for x in orders
                    if x.get('pay_status') == 'refunded' or (x.get('refund_status') or 'none') == 'refunded'
                )
            ),
            'preorder_orders': sum(1 for x in orders if x.get('sale_mode') == 'preorder'),
            'crowdfunding_orders': sum(1 for x in orders if x.get('sale_mode') == 'crowdfunding'),
        }
        return {
            'reserved': self.has_reservation(user_id, work['work_id']),
            'orders': orders,
            'order_overview': order_overview,
            'submissions': submissions,
            'designer': {
                'is_designer': bool(designer_dashboard['is_designer']),
                'profile': designer_dashboard['profile'],
                'qualification': designer_qualification,
            },
        }

    def admin_list_orders(
        self,
        limit: int = 200,
        keyword: str = '',
        sale_mode: str = '',
        pay_status: str = '',
        order_status: str = '',
        refund_status: str = '',
    ) -> Dict[str, Any]:
        sale_mode = self._normalize_sale_mode(sale_mode) if sale_mode else ''
        pay_status = (pay_status or '').strip()
        order_status = (order_status or '').strip()
        refund_status = (refund_status or '').strip()
        keyword = (keyword or '').strip()

        conditions: List[str] = []
        params: List[Any] = []
        if keyword:
            like_kw = f'%{keyword}%'
            conditions.append(
                '(o.order_id LIKE ? OR o.work_name LIKE ? OR o.sku_name LIKE ? OR u.openid LIKE ? OR u.nickname LIKE ?)'
            )
            params.extend([like_kw, like_kw, like_kw, like_kw, like_kw])
        if sale_mode:
            conditions.append('o.sale_mode=?')
            params.append(sale_mode)
        if pay_status:
            conditions.append('o.pay_status=?')
            params.append(pay_status)
        if order_status:
            conditions.append('o.order_status=?')
            params.append(order_status)
        if refund_status:
            conditions.append('COALESCE(o.refund_status, ?) = ?')
            params.extend(['none', refund_status])

        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ''
        safe_limit = max(1, min(limit, 1000))

        with self.lock, self._conn() as conn:
            rows = conn.execute(
                f'''
                SELECT
                  o.*,
                  COALESCE(u.openid, '') AS user_openid,
                  COALESCE(u.nickname, '') AS user_nickname
                FROM orders o
                LEFT JOIN users u ON u.id = o.user_id
                {where_sql}
                ORDER BY o.created_at DESC, o.order_id DESC
                LIMIT ?
                ''',
                tuple(params + [safe_limit]),
            ).fetchall()

            summary_row = conn.execute(
                f'''
                SELECT
                  COUNT(1) AS total_orders,
                  COALESCE(SUM(CASE WHEN o.pay_status='pending' THEN 1 ELSE 0 END), 0) AS pending_orders,
                  COALESCE(SUM(CASE WHEN o.pay_status='paid' AND COALESCE(o.refund_status, 'none')='none' THEN 1 ELSE 0 END), 0) AS paid_orders,
                  COALESCE(SUM(CASE WHEN COALESCE(o.refund_status, 'none') IN ('pending_submit', 'processing') THEN 1 ELSE 0 END), 0) AS refunding_orders,
                  COALESCE(SUM(CASE WHEN o.pay_status='refunded' OR COALESCE(o.refund_status, 'none')='refunded' THEN 1 ELSE 0 END), 0) AS refunded_orders,
                  COALESCE(SUM(CASE WHEN o.pay_status='paid' AND COALESCE(o.refund_status, 'none')='none' THEN o.paid_amount ELSE 0 END), 0) AS paid_amount,
                  COALESCE(SUM(CASE WHEN o.pay_status='refunded' OR COALESCE(o.refund_status, 'none')='refunded' THEN o.refund_amount ELSE 0 END), 0) AS refunded_amount,
                  COALESCE(SUM(CASE WHEN o.sale_mode='preorder' AND o.pay_status='paid' AND COALESCE(o.refund_status, 'none')='none' THEN 1 ELSE 0 END), 0) AS preorder_paid_orders,
                  COALESCE(SUM(CASE WHEN o.sale_mode='crowdfunding' AND o.pay_status='paid' AND COALESCE(o.refund_status, 'none')='none' THEN 1 ELSE 0 END), 0) AS crowdfunding_paid_orders
                FROM orders o
                LEFT JOIN users u ON u.id = o.user_id
                {where_sql}
                ''',
                tuple(params),
            ).fetchone()

            summary = {
                'total_orders': int(summary_row['total_orders'] or 0),
                'pending_orders': int(summary_row['pending_orders'] or 0),
                'paid_orders': int(summary_row['paid_orders'] or 0),
                'refunding_orders': int(summary_row['refunding_orders'] or 0),
                'refunded_orders': int(summary_row['refunded_orders'] or 0),
                'paid_amount': int(summary_row['paid_amount'] or 0),
                'refunded_amount': int(summary_row['refunded_amount'] or 0),
                'net_amount': int((summary_row['paid_amount'] or 0) - (summary_row['refunded_amount'] or 0)),
                'preorder_paid_orders': int(summary_row['preorder_paid_orders'] or 0),
                'crowdfunding_paid_orders': int(summary_row['crowdfunding_paid_orders'] or 0),
            }

            return {
                'items': [self._serialize_order_row(row) for row in rows],
                'summary': summary,
            }

    def admin_export_orders_csv(
        self,
        limit: int = 5000,
        keyword: str = '',
        sale_mode: str = '',
        pay_status: str = '',
        order_status: str = '',
        refund_status: str = '',
    ) -> str:
        ret = self.admin_list_orders(
            limit=limit,
            keyword=keyword,
            sale_mode=sale_mode,
            pay_status=pay_status,
            order_status=order_status,
            refund_status=refund_status,
        )
        items = ret.get('items') or []
        buff = io.StringIO()
        writer = csv.writer(buff)
        writer.writerow(
            [
                'order_id',
                'user_openid',
                'user_nickname',
                'sale_mode',
                'work_id',
                'work_name',
                'sku_id',
                'sku_name',
                'quantity',
                'total_amount',
                'paid_amount',
                'refund_amount',
                'pay_status',
                'order_status',
                'refund_status',
                'refund_reason',
                'admin_note',
                'payment_channel',
                'transaction_id',
                'created_at',
                'paid_at',
                'refunded_at',
            ]
        )
        for item in items:
            writer.writerow(
                [
                    item.get('order_id', ''),
                    item.get('user_openid', ''),
                    item.get('user_nickname', ''),
                    item.get('sale_mode', ''),
                    item.get('work_id', ''),
                    item.get('work_name', ''),
                    item.get('sku_id', ''),
                    item.get('sku_name', ''),
                    item.get('quantity', 0),
                    item.get('total_amount', 0),
                    item.get('paid_amount', 0),
                    item.get('refund_amount', 0),
                    item.get('pay_status', ''),
                    item.get('order_status_text', item.get('order_status', '')),
                    item.get('refund_status', ''),
                    item.get('refund_reason', ''),
                    item.get('admin_note', ''),
                    item.get('payment_channel', ''),
                    item.get('transaction_id', ''),
                    item.get('created_at', ''),
                    item.get('paid_at', ''),
                    item.get('refunded_at', ''),
                ]
            )
        return buff.getvalue()

    def admin_export_user_orders_csv(self, user_id: int, limit: int = 5000) -> str:
        safe_limit = max(1, min(int(limit or 5000), 10000))
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT
                  o.*,
                  COALESCE(u.openid, '') AS user_openid,
                  COALESCE(u.nickname, '') AS user_nickname
                FROM orders o
                LEFT JOIN users u ON u.id = o.user_id
                WHERE o.user_id=?
                ORDER BY o.created_at DESC, o.order_id DESC
                LIMIT ?
                ''',
                (int(user_id), safe_limit),
            ).fetchall()
            items = [self._serialize_order_row(row) for row in rows]

        buff = io.StringIO()
        writer = csv.writer(buff)
        writer.writerow(
            [
                'order_id',
                'user_openid',
                'user_nickname',
                'sale_mode',
                'work_id',
                'work_name',
                'sku_id',
                'sku_name',
                'quantity',
                'total_amount',
                'paid_amount',
                'refund_amount',
                'pay_status',
                'order_status',
                'refund_status',
                'refund_reason',
                'admin_note',
                'payment_channel',
                'transaction_id',
                'created_at',
                'paid_at',
                'refunded_at',
            ]
        )
        for item in items:
            writer.writerow(
                [
                    item.get('order_id', ''),
                    item.get('user_openid', ''),
                    item.get('user_nickname', ''),
                    item.get('sale_mode', ''),
                    item.get('work_id', ''),
                    item.get('work_name', ''),
                    item.get('sku_id', ''),
                    item.get('sku_name', ''),
                    item.get('quantity', 0),
                    item.get('total_amount', 0),
                    item.get('paid_amount', 0),
                    item.get('refund_amount', 0),
                    item.get('pay_status', ''),
                    item.get('order_status_text', item.get('order_status', '')),
                    item.get('refund_status', ''),
                    item.get('refund_reason', ''),
                    item.get('admin_note', ''),
                    item.get('payment_channel', ''),
                    item.get('transaction_id', ''),
                    item.get('created_at', ''),
                    item.get('paid_at', ''),
                    item.get('refunded_at', ''),
                ]
            )
        return buff.getvalue()

    def admin_list_action_logs(
        self,
        limit: int = 200,
        offset: int = 0,
        actor: str = '',
        action_type: str = '',
        target_type: str = '',
        target_id: str = '',
        related_user_id: int = 0,
        created_from: str = '',
        created_to: str = '',
        sort_by: str = 'created_at',
        sort_order: str = 'desc',
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit or 200), 10000))
        safe_offset = max(0, min(int(offset or 0), 200000))
        actor = (actor or '').strip()
        action_type = (action_type or '').strip()
        target_type = (target_type or '').strip()
        target_id = (target_id or '').strip()
        created_from = (created_from or '').strip()
        created_to = (created_to or '').strip()
        safe_related_user_id = max(0, int(related_user_id or 0))
        sort_by = (sort_by or 'created_at').strip().lower()
        sort_order = (sort_order or 'desc').strip().lower()
        if sort_by not in {'created_at', 'actor', 'action_type'}:
            sort_by = 'created_at'
        if sort_order not in {'asc', 'desc'}:
            sort_order = 'desc'
        sort_sql_order = 'ASC' if sort_order == 'asc' else 'DESC'
        if sort_by == 'created_at':
            tie_sql_order = 'ASC' if sort_sql_order == 'ASC' else 'DESC'
            order_sql = f'{sort_by} {sort_sql_order}, id {tie_sql_order}'
        else:
            order_sql = f'{sort_by} {sort_sql_order}, created_at DESC, id DESC'

        conditions: List[str] = []
        params: List[Any] = []
        if actor:
            conditions.append('actor LIKE ?')
            params.append(f'%{actor}%')
        if action_type:
            conditions.append('action_type=?')
            params.append(action_type)
        if target_type:
            conditions.append('target_type=?')
            params.append(target_type)
        if target_id:
            conditions.append('target_id LIKE ?')
            params.append(f'%{target_id}%')
        if safe_related_user_id > 0:
            conditions.append('related_user_id=?')
            params.append(safe_related_user_id)
        if created_from:
            conditions.append('created_at>=?')
            params.append(created_from)
        if created_to:
            conditions.append('created_at<=?')
            params.append(created_to)

        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ''
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                f'''
                SELECT *
                FROM admin_action_logs
                {where_sql}
                ORDER BY {order_sql}
                LIMIT ?
                OFFSET ?
                ''',
                tuple(params + [safe_limit, safe_offset]),
            ).fetchall()
            items = [self._serialize_admin_action_row(row) for row in rows]

            total_row = conn.execute(
                f'''
                SELECT COUNT(1) AS total
                FROM admin_action_logs
                {where_sql}
                ''',
                tuple(params),
            ).fetchone()
            total = int(total_row['total'] or 0)

            summary_row = conn.execute(
                f'''
                SELECT
                  COUNT(DISTINCT CASE WHEN actor<>'' THEN actor END) AS actors,
                  COUNT(DISTINCT CASE WHEN related_user_id>0 THEN related_user_id END) AS related_users,
                  MAX(created_at) AS latest_at,
                  MIN(created_at) AS earliest_at
                FROM admin_action_logs
                {where_sql}
                ''',
                tuple(params),
            ).fetchone()
            top_action_rows = conn.execute(
                f'''
                SELECT action_type, COUNT(1) AS cnt
                FROM admin_action_logs
                {where_sql}
                GROUP BY action_type
                ORDER BY cnt DESC, action_type ASC
                LIMIT 10
                ''',
                tuple(params),
            ).fetchall()

        return {
            'items': items,
            'summary': {
                'total': total,
                'actors': int(summary_row['actors'] or 0),
                'related_users': int(summary_row['related_users'] or 0),
                'latest_at': summary_row['latest_at'] or '',
                'earliest_at': summary_row['earliest_at'] or '',
                'top_actions': [
                    {'action_type': row['action_type'] or '', 'count': int(row['cnt'] or 0)}
                    for row in top_action_rows
                ],
            },
            'paging': {
                'limit': safe_limit,
                'offset': safe_offset,
                'returned': len(items),
                'has_more': (safe_offset + len(items)) < total,
            },
            'sorting': {'sort_by': sort_by, 'sort_order': sort_order},
        }

    def admin_export_action_logs_csv(
        self,
        limit: int = 5000,
        actor: str = '',
        action_type: str = '',
        target_type: str = '',
        target_id: str = '',
        related_user_id: int = 0,
        created_from: str = '',
        created_to: str = '',
        sort_by: str = 'created_at',
        sort_order: str = 'desc',
    ) -> str:
        ret = self.admin_list_action_logs(
            limit=limit,
            offset=0,
            actor=actor,
            action_type=action_type,
            target_type=target_type,
            target_id=target_id,
            related_user_id=related_user_id,
            created_from=created_from,
            created_to=created_to,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        items = ret.get('items') or []
        buff = io.StringIO()
        writer = csv.writer(buff)
        writer.writerow(
            [
                'id',
                'created_at',
                'actor',
                'action_type',
                'target_type',
                'target_id',
                'related_user_id',
                'detail_json',
            ]
        )
        for item in items:
            writer.writerow(
                [
                    item.get('id', 0),
                    item.get('created_at', ''),
                    item.get('actor', ''),
                    item.get('action_type', ''),
                    item.get('target_type', ''),
                    item.get('target_id', ''),
                    item.get('related_user_id', 0),
                    json.dumps(item.get('detail') or {}, ensure_ascii=False),
                ]
            )
        return buff.getvalue()

    def admin_list_users(self, keyword: str = '', limit: int = 200) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit or 200), 2000))
        keyword = (keyword or '').strip()
        params: List[Any] = []
        where_sql = ''
        if keyword:
            like_kw = f'%{keyword}%'
            where_sql = 'WHERE (u.openid LIKE ? OR u.nickname LIKE ?)'
            params.extend([like_kw, like_kw])

        with self.lock, self._conn() as conn:
            rows = conn.execute(
                f'''
                SELECT
                  u.id,
                  u.openid,
                  u.nickname,
                  u.created_at,
                  u.updated_at,
                  COALESCE(d.id, 0) AS designer_id,
                  COALESCE(d.display_name, '') AS designer_name,
                  COALESCE(d.status, '') AS designer_status,
                  (
                    SELECT COUNT(1)
                    FROM orders o
                    WHERE o.user_id = u.id
                  ) AS total_orders,
                  (
                    SELECT COUNT(1)
                    FROM orders o
                    WHERE o.user_id = u.id AND o.pay_status='pending'
                  ) AS pending_orders,
                  (
                    SELECT COUNT(1)
                    FROM orders o
                    WHERE o.user_id = u.id
                      AND o.pay_status='paid'
                      AND COALESCE(o.refund_status, 'none')='none'
                  ) AS paid_orders,
                  (
                    SELECT COUNT(1)
                    FROM orders o
                    WHERE o.user_id = u.id
                      AND COALESCE(o.refund_status, 'none') IN ('pending_submit','processing')
                  ) AS refunding_orders,
                  (
                    SELECT COUNT(1)
                    FROM orders o
                    WHERE o.user_id = u.id
                      AND (o.pay_status='refunded' OR COALESCE(o.refund_status, 'none')='refunded')
                  ) AS refunded_orders,
                  (
                    SELECT COALESCE(SUM(o.paid_amount), 0)
                    FROM orders o
                    WHERE o.user_id = u.id
                      AND o.pay_status='paid'
                      AND COALESCE(o.refund_status, 'none')='none'
                  ) AS paid_amount,
                  (
                    SELECT COALESCE(SUM(o.refund_amount), 0)
                    FROM orders o
                    WHERE o.user_id = u.id
                      AND (o.pay_status='refunded' OR COALESCE(o.refund_status, 'none')='refunded')
                  ) AS refund_amount,
                  (
                    SELECT COUNT(1)
                    FROM submissions s
                    WHERE s.user_id = u.id
                  ) AS submissions_count,
                  (
                    SELECT COUNT(1)
                    FROM reservations r
                    WHERE r.user_id = u.id
                  ) AS reservations_count,
                  (
                    SELECT MAX(o.created_at)
                    FROM orders o
                    WHERE o.user_id = u.id
                  ) AS last_order_at
                FROM users u
                LEFT JOIN designers d ON d.user_id = u.id
                {where_sql}
                ORDER BY u.updated_at DESC, u.id DESC
                LIMIT ?
                ''',
                tuple(params + [safe_limit]),
            ).fetchall()

        items: List[Dict[str, Any]] = []
        summary = {
            'users': 0,
            'designers': 0,
            'total_orders': 0,
            'paid_orders': 0,
            'refunding_orders': 0,
            'refunded_orders': 0,
            'paid_amount': 0,
            'refund_amount': 0,
            'net_amount': 0,
            'submissions_count': 0,
            'reservations_count': 0,
        }
        for row in rows:
            paid_amount = int(row['paid_amount'] or 0)
            refund_amount = int(row['refund_amount'] or 0)
            item = {
                'user_id': int(row['id']),
                'openid': row['openid'],
                'nickname': row['nickname'] or '',
                'created_at': row['created_at'],
                'updated_at': row['updated_at'],
                'is_designer': bool(int(row['designer_id'] or 0)),
                'designer_name': row['designer_name'] or '',
                'designer_status': row['designer_status'] or '',
                'total_orders': int(row['total_orders'] or 0),
                'pending_orders': int(row['pending_orders'] or 0),
                'paid_orders': int(row['paid_orders'] or 0),
                'refunding_orders': int(row['refunding_orders'] or 0),
                'refunded_orders': int(row['refunded_orders'] or 0),
                'paid_amount': paid_amount,
                'refund_amount': refund_amount,
                'net_amount': paid_amount - refund_amount,
                'submissions_count': int(row['submissions_count'] or 0),
                'reservations_count': int(row['reservations_count'] or 0),
                'last_order_at': row['last_order_at'] or '',
            }
            items.append(item)

            summary['users'] += 1
            summary['designers'] += 1 if item['is_designer'] else 0
            summary['total_orders'] += item['total_orders']
            summary['paid_orders'] += item['paid_orders']
            summary['refunding_orders'] += item['refunding_orders']
            summary['refunded_orders'] += item['refunded_orders']
            summary['paid_amount'] += item['paid_amount']
            summary['refund_amount'] += item['refund_amount']
            summary['submissions_count'] += item['submissions_count']
            summary['reservations_count'] += item['reservations_count']
        summary['net_amount'] = summary['paid_amount'] - summary['refund_amount']
        return {'items': items, 'summary': summary}

    def admin_get_user_detail(
        self,
        user_id: int,
        order_limit: int = 100,
        submission_limit: int = 100,
        reservation_limit: int = 100,
        commission_limit: int = 100,
        action_limit: int = 200,
    ) -> Dict[str, Any]:
        safe_order_limit = max(1, min(int(order_limit or 100), 500))
        safe_submission_limit = max(1, min(int(submission_limit or 100), 500))
        safe_reservation_limit = max(1, min(int(reservation_limit or 100), 500))
        safe_commission_limit = max(1, min(int(commission_limit or 100), 500))
        safe_action_limit = max(1, min(int(action_limit or 200), 1000))

        with self.lock, self._conn() as conn:
            user_row = conn.execute('SELECT * FROM users WHERE id=? LIMIT 1', (int(user_id),)).fetchone()
            if not user_row:
                raise ValueError('用户不存在')

            orders_rows = conn.execute(
                '''
                SELECT *
                FROM orders
                WHERE user_id=?
                ORDER BY created_at DESC, order_id DESC
                LIMIT ?
                ''',
                (int(user_id), safe_order_limit),
            ).fetchall()
            orders = [self._serialize_order_row(row) for row in orders_rows]

            submissions_rows = conn.execute(
                '''
                SELECT *
                FROM submissions
                WHERE user_id=?
                ORDER BY created_at DESC
                LIMIT ?
                ''',
                (int(user_id), safe_submission_limit),
            ).fetchall()
            submissions = [self._serialize_submission_row(row) for row in submissions_rows]

            reservations_rows = conn.execute(
                '''
                SELECT
                  r.id,
                  r.work_id,
                  r.created_at,
                  COALESCE(w.name, r.work_id) AS work_name
                FROM reservations r
                LEFT JOIN works w ON w.work_id = r.work_id
                WHERE r.user_id=?
                ORDER BY r.created_at DESC
                LIMIT ?
                ''',
                (int(user_id), safe_reservation_limit),
            ).fetchall()
            reservations = [
                {
                    'reservation_id': int(row['id']),
                    'work_id': row['work_id'],
                    'work_name': row['work_name'],
                    'created_at': row['created_at'],
                }
                for row in reservations_rows
            ]

            designer_row = conn.execute('SELECT * FROM designers WHERE user_id=? LIMIT 1', (int(user_id),)).fetchone()
            designer_profile: Optional[Dict[str, Any]] = None
            assignments: List[Dict[str, Any]] = []
            commission_items: List[Dict[str, Any]] = []
            commission_summary = {
                'records': 0,
                'pending_records': 0,
                'settled_records': 0,
                'commission_amount': 0.0,
                'pending_commission_amount': 0.0,
                'settled_commission_amount': 0.0,
            }

            if designer_row:
                designer_id = int(designer_row['id'])
                designer_profile = self._serialize_designer_row(
                    designer_row,
                    openid=user_row['openid'],
                    nickname=user_row['nickname'],
                )
                assignment_rows = conn.execute(
                    '''
                    SELECT l.*, COALESCE(w.name, l.work_id) AS work_name
                    FROM designer_work_links l
                    LEFT JOIN works w ON w.work_id = l.work_id
                    WHERE l.designer_id=? AND l.is_active=1
                    ORDER BY l.updated_at DESC
                    ''',
                    (designer_id,),
                ).fetchall()
                assignments = [
                    {
                        'work_id': row['work_id'],
                        'work_name': row['work_name'],
                        'share_ratio': float(row['share_ratio'] or DEFAULT_DESIGNER_SHARE),
                        'share_percent': round(float(row['share_ratio'] or DEFAULT_DESIGNER_SHARE) * 100, 2),
                        'updated_at': row['updated_at'],
                    }
                    for row in assignment_rows
                ]

                commission_rows = conn.execute(
                    '''
                    SELECT
                      c.*,
                      o.work_name,
                      o.sku_name,
                      o.quantity,
                      o.total_amount,
                      o.paid_at,
                      d.display_name,
                      u.openid
                    FROM designer_commission_records c
                    JOIN orders o ON o.order_id = c.order_id
                    JOIN designers d ON d.id = c.designer_id
                    LEFT JOIN users u ON u.id = d.user_id
                    WHERE c.designer_id=?
                    ORDER BY c.updated_at DESC
                    LIMIT ?
                    ''',
                    (designer_id, safe_commission_limit),
                ).fetchall()
                commission_items = [self._serialize_commission_row(row) for row in commission_rows]

                com_row = conn.execute(
                    '''
                    SELECT
                      COUNT(1) AS records,
                      COALESCE(SUM(CASE WHEN settlement_status='pending' THEN 1 ELSE 0 END), 0) AS pending_records,
                      COALESCE(SUM(CASE WHEN settlement_status='settled' THEN 1 ELSE 0 END), 0) AS settled_records,
                      COALESCE(SUM(commission_amount), 0) AS commission_amount,
                      COALESCE(SUM(CASE WHEN settlement_status='pending' THEN commission_amount ELSE 0 END), 0) AS pending_commission_amount,
                      COALESCE(SUM(CASE WHEN settlement_status='settled' THEN commission_amount ELSE 0 END), 0) AS settled_commission_amount
                    FROM designer_commission_records
                    WHERE designer_id=?
                    ''',
                    (designer_id,),
                ).fetchone()
                commission_summary = {
                    'records': int(com_row['records'] or 0),
                    'pending_records': int(com_row['pending_records'] or 0),
                    'settled_records': int(com_row['settled_records'] or 0),
                    'commission_amount': round(float(com_row['commission_amount'] or 0), 2),
                    'pending_commission_amount': round(float(com_row['pending_commission_amount'] or 0), 2),
                    'settled_commission_amount': round(float(com_row['settled_commission_amount'] or 0), 2),
                }

            action_rows = conn.execute(
                '''
                SELECT *
                FROM admin_action_logs
                WHERE related_user_id=?
                ORDER BY id DESC
                LIMIT ?
                ''',
                (int(user_id), safe_action_limit),
            ).fetchall()
            action_logs = [
                self._serialize_admin_action_row(row)
                for row in action_rows
            ]

        order_summary = {
            'total_orders': len(orders),
            'pending_orders': sum(1 for x in orders if x.get('pay_status') == 'pending'),
            'paid_orders': sum(1 for x in orders if x.get('pay_status') == 'paid' and (x.get('refund_status') or 'none') == 'none'),
            'refunding_orders': sum(1 for x in orders if (x.get('refund_status') or 'none') in {'pending_submit', 'processing'}),
            'refunded_orders': sum(
                1
                for x in orders
                if x.get('pay_status') == 'refunded' or (x.get('refund_status') or 'none') == 'refunded'
            ),
            'paid_amount': int(
                sum(int(x.get('paid_amount') or 0) for x in orders if x.get('pay_status') == 'paid' and (x.get('refund_status') or 'none') == 'none')
            ),
            'refund_amount': int(
                sum(
                    int(x.get('refund_amount') or 0)
                    for x in orders
                    if x.get('pay_status') == 'refunded' or (x.get('refund_status') or 'none') == 'refunded'
                )
            ),
        }
        order_summary['net_amount'] = order_summary['paid_amount'] - order_summary['refund_amount']

        return {
            'user': {
                'user_id': int(user_row['id']),
                'openid': user_row['openid'],
                'nickname': user_row['nickname'] or '',
                'created_at': user_row['created_at'],
                'updated_at': user_row['updated_at'],
            },
            'order_summary': order_summary,
            'orders': orders,
            'submissions': submissions,
            'reservations': reservations,
            'designer': {
                'is_designer': bool(designer_profile),
                'profile': designer_profile,
                'assignments': assignments,
                'commission_summary': commission_summary,
                'commission_items': commission_items,
            },
            'action_logs': action_logs,
        }

    def admin_list_submissions(self, status: str = '', limit: int = 200) -> List[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            if status:
                rows = conn.execute('SELECT * FROM submissions WHERE status=? ORDER BY created_at DESC LIMIT ?', (status, max(1, min(limit, 1000)))).fetchall()
            else:
                rows = conn.execute('SELECT * FROM submissions ORDER BY created_at DESC LIMIT ?', (max(1, min(limit, 1000)),)).fetchall()
            return [self._serialize_submission_row(row) for row in rows]

    def admin_review_submission(self, submission_id: str, status: str, note: str = '') -> Dict[str, Any]:
        if status not in {'approved', 'rejected', 'pending'}:
            raise ValueError('审核状态非法')

        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM submissions WHERE submission_id=? LIMIT 1', (submission_id,)).fetchone()
            if not row:
                raise ValueError('投稿不存在')

            conn.execute(
                'UPDATE submissions SET status=?, review_note=?, updated_at=? WHERE submission_id=?',
                (status, note.strip(), now_iso(), submission_id),
            )
            conn.commit()

            refreshed = conn.execute('SELECT * FROM submissions WHERE submission_id=? LIMIT 1', (submission_id,)).fetchone()
            return self._serialize_submission_row(refreshed)

    def admin_activate_designer_from_submission(self, submission_id: str) -> Dict[str, Any]:
        safe_submission_id = (submission_id or '').strip()
        if not safe_submission_id:
            raise ValueError('投稿ID不能为空')

        with self.lock, self._conn() as conn:
            row = conn.execute('SELECT * FROM submissions WHERE submission_id=? LIMIT 1', (safe_submission_id,)).fetchone()
            if not row:
                raise ValueError('投稿不存在')
            status = str(row['status'] or '').strip()
            if status != 'approved':
                raise ValueError('仅审核通过的投稿可开通设计师')
            user_id = int(row['user_id'])
            suggested_name = str(row['designer_name'] or '').strip()

        previous_profile = self.get_designer_profile_by_user(user_id=user_id)
        profile = self.enroll_designer(user_id=user_id, display_name=suggested_name)
        current_profile = profile or self.get_designer_profile_by_user(user_id=user_id)
        if not current_profile:
            raise ValueError('开通设计师失败')

        return {
            'submission': self._serialize_submission_row(row),
            'profile': current_profile,
            'was_designer': bool(previous_profile),
            'created': not bool(previous_profile),
        }

    def admin_list_reservations(self, limit: int = 500) -> List[Dict[str, Any]]:
        with self.lock, self._conn() as conn:
            rows = conn.execute(
                '''
                SELECT r.id, r.work_id, r.created_at, u.openid, u.nickname
                FROM reservations r
                JOIN users u ON u.id = r.user_id
                ORDER BY r.created_at DESC
                LIMIT ?
                ''',
                (max(1, min(limit, 2000)),),
            ).fetchall()
            return [
                {
                    'reservation_id': int(row['id']),
                    'work_id': row['work_id'],
                    'created_at': row['created_at'],
                    'openid': row['openid'],
                    'nickname': row['nickname'],
                }
                for row in rows
            ]

    def admin_dashboard(self) -> Dict[str, Any]:
        self._ensure_commission_records()
        with self.lock, self._conn() as conn:
            users = int(conn.execute('SELECT COUNT(1) AS c FROM users').fetchone()['c'])
            reservations = int(conn.execute('SELECT COUNT(1) AS c FROM reservations').fetchone()['c'])
            orders = int(conn.execute('SELECT COUNT(1) AS c FROM orders').fetchone()['c'])
            paid_orders = int(
                conn.execute(
                    "SELECT COUNT(1) AS c FROM orders WHERE pay_status='paid' AND COALESCE(refund_status, 'none')='none'"
                ).fetchone()['c']
            )
            paid_amount = int(
                conn.execute(
                    "SELECT COALESCE(SUM(paid_amount),0) AS c FROM orders WHERE pay_status='paid' AND COALESCE(refund_status, 'none')='none'"
                ).fetchone()['c']
            )
            refunded_amount = int(
                conn.execute(
                    "SELECT COALESCE(SUM(refund_amount),0) AS c FROM orders WHERE pay_status='refunded' OR COALESCE(refund_status, 'none')='refunded'"
                ).fetchone()['c']
            )
            refunding_orders = int(
                conn.execute(
                    "SELECT COUNT(1) AS c FROM orders WHERE COALESCE(refund_status, 'none') IN ('pending_submit','processing')"
                ).fetchone()['c']
            )
            preorder_paid_orders = int(
                conn.execute(
                    "SELECT COUNT(1) AS c FROM orders WHERE sale_mode='preorder' AND pay_status='paid' AND COALESCE(refund_status, 'none')='none'"
                ).fetchone()['c']
            )
            crowdfunding_paid_orders = int(
                conn.execute(
                    "SELECT COUNT(1) AS c FROM orders WHERE sale_mode='crowdfunding' AND pay_status='paid' AND COALESCE(refund_status, 'none')='none'"
                ).fetchone()['c']
            )
            submissions = int(conn.execute('SELECT COUNT(1) AS c FROM submissions').fetchone()['c'])
            pending_submissions = int(conn.execute("SELECT COUNT(1) AS c FROM submissions WHERE status='pending'").fetchone()['c'])
            designers = int(conn.execute('SELECT COUNT(1) AS c FROM designers').fetchone()['c'])
            designer_links = int(conn.execute('SELECT COUNT(1) AS c FROM designer_work_links WHERE is_active=1').fetchone()['c'])
            feedback_total = int(conn.execute('SELECT COUNT(1) AS c FROM user_feedbacks').fetchone()['c'])
            feedback_pending = int(conn.execute("SELECT COUNT(1) AS c FROM user_feedbacks WHERE status='pending'").fetchone()['c'])
            feedback_processing = int(conn.execute("SELECT COUNT(1) AS c FROM user_feedbacks WHERE status='processing'").fetchone()['c'])
            feedback_resolved = int(conn.execute("SELECT COUNT(1) AS c FROM user_feedbacks WHERE status='resolved'").fetchone()['c'])
            pending_commission_count = int(
                conn.execute("SELECT COUNT(1) AS c FROM designer_commission_records WHERE settlement_status='pending'").fetchone()['c']
            )
            settled_commission_count = int(
                conn.execute("SELECT COUNT(1) AS c FROM designer_commission_records WHERE settlement_status='settled'").fetchone()['c']
            )
            pending_commission_amount = round(
                float(
                    conn.execute(
                        "SELECT COALESCE(SUM(commission_amount),0) AS c FROM designer_commission_records WHERE settlement_status='pending'"
                    ).fetchone()['c']
                ),
                2,
            )
            settled_commission_amount = round(
                float(
                    conn.execute(
                        "SELECT COALESCE(SUM(commission_amount),0) AS c FROM designer_commission_records WHERE settlement_status='settled'"
                    ).fetchone()['c']
                ),
                2,
            )

        return {
            'users': users,
            'reservations': reservations,
            'orders': orders,
            'paid_orders': paid_orders,
            'paid_amount': paid_amount,
            'refunded_amount': refunded_amount,
            'net_amount': paid_amount - refunded_amount,
            'refunding_orders': refunding_orders,
            'preorder_paid_orders': preorder_paid_orders,
            'crowdfunding_paid_orders': crowdfunding_paid_orders,
            'submissions': submissions,
            'pending_submissions': pending_submissions,
            'designers': designers,
            'designer_links': designer_links,
            'feedback_total': feedback_total,
            'feedback_pending': feedback_pending,
            'feedback_processing': feedback_processing,
            'feedback_resolved': feedback_resolved,
            'pending_commission_count': pending_commission_count,
            'settled_commission_count': settled_commission_count,
            'pending_commission_amount': pending_commission_amount,
            'settled_commission_amount': settled_commission_amount,
        }


store = Store()
