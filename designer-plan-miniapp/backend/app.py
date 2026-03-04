import base64
import hashlib
import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urlencode
from urllib.error import HTTPError
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from part_adapter_store import part_adapter_store
from store import DATA_DIR, DEFAULT_DESIGNER_SHARE, now_iso, store

BASE_DIR = Path(__file__).resolve().parent
ADMIN_DIR = BASE_DIR / 'admin'
UPLOAD_DIR = DATA_DIR / 'uploads'
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
PART_ADAPTER_PUBLIC_COOKIE = 'kwc_part_adapter_token'

app = FastAPI(title='KWC Designer Plan API', version='1.2.0')
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
app.mount('/static', StaticFiles(directory=str(DATA_DIR)), name='static')

ADMIN_ROUTE_PERMISSION_RULES: List[Dict[str, Any]] = [
    {'prefix': '/api/admin/part-adapter', 'permission': 'project'},
    {'prefix': '/api/admin/admin-users', 'permission': 'setting'},
    {'prefix': '/api/admin/roles', 'permission': 'setting'},
    {'prefix': '/api/admin/settings', 'permission': 'setting'},
    {'prefix': '/api/admin/action-logs', 'permission': 'log'},
    {'prefix': '/api/admin/feedback', 'permission': 'feedback'},
    {'prefix': '/api/admin/submissions', 'permission': 'submission'},
    {'prefix': '/api/admin/commissions', 'permission': 'designer'},
    {'prefix': '/api/admin/designers', 'permission': 'designer'},
    {'prefix': '/api/admin/orders', 'permission': 'order'},
    {'prefix': '/api/admin/refunds', 'permission': 'order'},
    {'prefix': '/api/admin/users', 'permission': 'user'},
    {'prefix': '/api/admin/projects', 'permission': 'project'},
    {'prefix': '/api/admin/work', 'permission': 'project'},
    {'prefix': '/api/admin/reservations', 'permission': 'project'},
    {'prefix': '/api/admin/uploads', 'permission': 'project'},
    {'prefix': '/api/admin/dashboard', 'permission': 'overview'},
]


class LoginRequest(BaseModel):
    code: str = Field(default='')
    nickname: str = Field(default='')


class UserProfileUpdateRequest(BaseModel):
    nickname: str = Field(default='')


class ReservationRequest(BaseModel):
    work_id: str


class PreorderRequest(BaseModel):
    sku_id: str
    quantity: int = Field(default=1, ge=1, le=20)


class PaymentConfirmRequest(BaseModel):
    order_id: str
    mock_token: str = Field(default='')
    source: str = Field(default='mock')
    transaction_id: str = Field(default='')


class SubmissionCreateRequest(BaseModel):
    designer_name: str
    contact: str
    work_name: str
    category: str
    intro: str
    estimated_pieces: int = Field(ge=1, le=20000)
    image_urls: List[str] = Field(default_factory=list)


class SubmissionReviewRequest(BaseModel):
    status: str
    note: str = Field(default='')


class WorkCommentCreateRequest(BaseModel):
    content: str = Field(default='')


class WorkUpdateRequest(BaseModel):
    name: Optional[str] = None
    subtitle: Optional[str] = None
    sale_mode: Optional[str] = None
    crowdfunding_goal_amount: Optional[int] = None
    crowdfunding_deadline: Optional[str] = None
    cover_image: Optional[str] = None
    gallery_images: Optional[List[str]] = None
    story: Optional[str] = None
    specs: Optional[List[Dict[str, Any]]] = None
    highlights: Optional[List[str]] = None
    sku_list: Optional[List[Dict[str, Any]]] = None


class AdminProjectCreateRequest(BaseModel):
    work_id: str
    name: str
    subtitle: str = Field(default='')
    sale_mode: str = Field(default='preorder')
    crowdfunding_goal_amount: int = Field(default=0)
    crowdfunding_deadline: str = Field(default='')
    cover_image: Optional[str] = None
    gallery_images: Optional[List[str]] = None
    story: Optional[str] = None
    specs: Optional[List[Dict[str, Any]]] = None
    highlights: Optional[List[str]] = None
    sku_list: Optional[List[Dict[str, Any]]] = None
    is_current: bool = Field(default=False)
    designer_openid: Optional[str] = None
    designer_share_ratio: Optional[float] = Field(default=None, gt=0.0, le=1.0)


class AdminProjectUpdateRequest(BaseModel):
    name: Optional[str] = None
    subtitle: Optional[str] = None
    sale_mode: Optional[str] = None
    crowdfunding_goal_amount: Optional[int] = None
    crowdfunding_deadline: Optional[str] = None
    cover_image: Optional[str] = None
    gallery_images: Optional[List[str]] = None
    story: Optional[str] = None
    specs: Optional[List[Dict[str, Any]]] = None
    highlights: Optional[List[str]] = None
    sku_list: Optional[List[Dict[str, Any]]] = None
    is_current: Optional[bool] = None
    designer_openid: Optional[str] = None
    designer_share_ratio: Optional[float] = Field(default=None, gt=0.0, le=1.0)


class DesignerEnrollRequest(BaseModel):
    display_name: str = Field(default='')
    bio: str = Field(default='')


class DesignerUpdateCreateRequest(BaseModel):
    work_id: str
    title: str
    content: str


class DesignerProjectMaintainRequest(BaseModel):
    name: Optional[str] = None
    subtitle: Optional[str] = None
    cover_image: Optional[str] = None
    gallery_images: Optional[List[str]] = None
    story: Optional[str] = None
    highlights: Optional[List[str]] = None
    specs: Optional[List[Dict[str, Any]]] = None


class DesignerCommentReplyRequest(BaseModel):
    reply_content: str = Field(default='')


class DesignerProfileUpdateRequest(BaseModel):
    display_name: str = Field(default='')
    bio: str = Field(default='')
    avatar_url: str = Field(default='')


class AdminSettingsUpdateRequest(BaseModel):
    general: Dict[str, Any] = Field(default_factory=dict)
    api: Dict[str, Any] = Field(default_factory=dict)


class AdminDesignerAssignRequest(BaseModel):
    openid: str
    work_id: str
    share_ratio: float = Field(default=DEFAULT_DESIGNER_SHARE, gt=0.0, le=1.0)


class AdminCommissionSettleRequest(BaseModel):
    settlement_status: str = Field(default='settled')
    settlement_note: str = Field(default='')


class AdminCommissionBatchSettleRequest(BaseModel):
    settlement_status: str = Field(default='settled')
    settlement_note: str = Field(default='')
    record_ids: List[int] = Field(default_factory=list)
    from_status: str = Field(default='')
    limit: int = Field(default=1000, ge=1, le=5000)


class AdminCrowdfundingRefundInitiateRequest(BaseModel):
    limit: int = Field(default=50, ge=1, le=500)
    reason: str = Field(default='众筹截止未达目标，原路退款')


class AdminOrderNoteRequest(BaseModel):
    note: str = Field(default='')


class AdminOrderRetryRefundRequest(BaseModel):
    reason: str = Field(default='众筹订单退款重试')


class FeedbackCreateRequest(BaseModel):
    category: str = Field(default='general')
    priority: str = Field(default='normal')
    content: str = Field(default='')
    contact: str = Field(default='')
    image_urls: List[str] = Field(default_factory=list)


class AdminFeedbackReplyRequest(BaseModel):
    status: str = Field(default='resolved')
    admin_reply: str = Field(default='')
    template_code: str = Field(default='')


class AdminFeedbackTemplateUpsertRequest(BaseModel):
    code: str = Field(default='')
    title: str = Field(default='')
    content: str = Field(default='')
    is_active: bool = Field(default=True)


class AdminAuthLoginRequest(BaseModel):
    username: str = Field(default='')
    password: str = Field(default='')


class AdminRoleCreateRequest(BaseModel):
    role_key: str = Field(default='')
    role_name: str = Field(default='')
    permissions: List[str] = Field(default_factory=list)


class AdminRoleUpdateRequest(BaseModel):
    role_name: Optional[str] = None
    permissions: Optional[List[str]] = None


class AdminUserCreateRequest(BaseModel):
    username: str = Field(default='')
    password: str = Field(default='')
    display_name: str = Field(default='')
    role_key: str = Field(default='')
    status: str = Field(default='active')


class AdminUserUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    role_key: Optional[str] = None
    status: Optional[str] = None
    password: Optional[str] = None


class PartAdapterAnalyzeRequest(BaseModel):
    project_name: str = Field(default='')
    designer_name: str = Field(default='')
    source_name: str = Field(default='')
    bom_text: str = Field(default='')
    color_mode: str = Field(default='balanced')
    allow_display_sub: bool = Field(default=True)
    allow_structural_sub: bool = Field(default=False)


class PartAdapterRulesUpdateRequest(BaseModel):
    gobricks_sync_meta: Optional[Dict[str, Any]] = None
    gobricks_item_index: Optional[Dict[str, Any]] = None
    gobricks_category_index: Optional[Dict[str, Any]] = None
    exact_combo_map: Optional[Dict[str, Any]] = None
    shortage_combo_map: Optional[Dict[str, Any]] = None
    lego_color_catalog: Optional[Dict[str, Any]] = None
    exact_part_map: Optional[Dict[str, Any]] = None
    part_alias_map: Optional[Dict[str, Any]] = None
    color_rules: Optional[Dict[str, Any]] = None
    substitutions: Optional[Dict[str, Any]] = None
    part_meta: Optional[Dict[str, Any]] = None


class PartAdapterReviewUpdateRequest(BaseModel):
    line_no: int = Field(default=0, ge=1)
    review_status: str = Field(default='pending_review')


class PartAdapterGobricksSyncRequest(BaseModel):
    auth_token: str = Field(default='')
    base_url: str = Field(default='https://api.gobricks.cn')
    start_time: str = Field(default='')
    end_time: str = Field(default='')
    need_detail_info: bool = Field(default=True)


class PartAdapterAnalyticsEventRequest(BaseModel):
    event_type: str = Field(default='')
    source_name: str = Field(default='')


def _resolve_gobricks_auth_token(provided: str = '') -> str:
    direct = str(provided or '').strip()
    if direct:
        return direct
    return str(os.environ.get('GOBRICKS_AUTH_TOKEN') or '').strip()


def _part_adapter_visitor_hash(request: Request) -> str:
    client_host = str(request.client.host or '').strip() if request.client else ''
    user_agent = str(request.headers.get('user-agent') or '').strip()
    raw = f'{client_host}|{user_agent}'
    if not raw.strip('|'):
        return ''
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]


def get_admin_token() -> str:
    return os.getenv('ADMIN_TOKEN', 'kwc-admin-dev').strip() or 'kwc-admin-dev'


def get_pay_mode() -> str:
    mode = (os.getenv('PAY_MODE', 'mock').strip().lower() or 'mock')
    return 'wechat' if mode in {'wechat', 'real'} else 'mock'


def get_bool_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, '') or '').strip().lower()
    if not raw:
        return default
    return raw in {'1', 'true', 'yes', 'on'}


def load_text_from_env_or_path(env_key: str, path_key: str) -> str:
    value = str(os.getenv(env_key, '') or '').strip()
    if value:
        return value
    path = str(os.getenv(path_key, '') or '').strip()
    if path:
        return Path(path).read_text(encoding='utf-8')
    return ''


def build_multipart_form_data(
    fields: Dict[str, Any],
    file_field_name: str,
    filename: str,
    file_content: bytes,
    content_type: str = 'application/octet-stream',
) -> Dict[str, Any]:
    boundary = f'----KWCPartAdapter{uuid.uuid4().hex}'
    body = bytearray()
    for key, value in (fields or {}).items():
        body.extend(f'--{boundary}\r\n'.encode('utf-8'))
        body.extend(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode('utf-8'))
        body.extend(str(value if value is not None else '').encode('utf-8'))
        body.extend(b'\r\n')
    safe_filename = os.path.basename(str(filename or 'upload.csv')) or 'upload.csv'
    body.extend(f'--{boundary}\r\n'.encode('utf-8'))
    body.extend(
        f'Content-Disposition: form-data; name="{file_field_name}"; filename="{safe_filename}"\r\n'.encode('utf-8')
    )
    body.extend(f'Content-Type: {content_type}\r\n\r\n'.encode('utf-8'))
    body.extend(file_content)
    body.extend(b'\r\n')
    body.extend(f'--{boundary}--\r\n'.encode('utf-8'))
    return {
        'body': bytes(body),
        'content_type': f'multipart/form-data; boundary={boundary}',
    }


def get_wechat_api_v3_key() -> bytes:
    key = str(os.getenv('WECHAT_PAY_API_V3_KEY', '') or '').strip()
    if len(key.encode('utf-8')) != 32:
        raise ValueError('缺少或未正确配置 WECHAT_PAY_API_V3_KEY（必须是32字节）')
    return key.encode('utf-8')


def load_wechat_merchant_private_key() -> Any:
    pem = load_text_from_env_or_path('WECHAT_PAY_PRIVATE_KEY_PEM', 'WECHAT_PAY_PRIVATE_KEY_PATH')
    if not pem:
        raise ValueError('缺少商户私钥，请配置 WECHAT_PAY_PRIVATE_KEY_PEM 或 WECHAT_PAY_PRIVATE_KEY_PATH')
    return serialization.load_pem_private_key(pem.encode('utf-8'), password=None)


def load_wechat_platform_public_key() -> Any:
    pem = load_text_from_env_or_path('WECHAT_PAY_PLATFORM_PUBLIC_KEY_PEM', 'WECHAT_PAY_PLATFORM_PUBLIC_KEY_PATH')
    if pem:
        try:
            return serialization.load_pem_public_key(pem.encode('utf-8'))
        except Exception:
            cert = x509.load_pem_x509_certificate(pem.encode('utf-8'))
            return cert.public_key()

    cert_path = str(os.getenv('WECHAT_PAY_PLATFORM_CERT_PATH', '') or '').strip()
    if cert_path:
        cert_pem = Path(cert_path).read_bytes()
        cert = x509.load_pem_x509_certificate(cert_pem)
        return cert.public_key()

    raise ValueError('缺少微信平台公钥，请配置 WECHAT_PAY_PLATFORM_PUBLIC_KEY_PEM/WECHAT_PAY_PLATFORM_PUBLIC_KEY_PATH/WECHAT_PAY_PLATFORM_CERT_PATH')


def build_wechatpay_authorization(method: str, path: str, body_text: str) -> Dict[str, str]:
    mchid = str(os.getenv('WECHAT_PAY_MCHID', '') or '').strip()
    serial_no = str(os.getenv('WECHAT_PAY_SERIAL_NO', '') or '').strip()
    if not mchid:
        raise ValueError('缺少 WECHAT_PAY_MCHID')
    if not serial_no:
        raise ValueError('缺少 WECHAT_PAY_SERIAL_NO')

    private_key = load_wechat_merchant_private_key()
    timestamp = str(int(time.time()))
    nonce_str = uuid.uuid4().hex
    message = f'{method}\n{path}\n{timestamp}\n{nonce_str}\n{body_text}\n'
    signature = private_key.sign(message.encode('utf-8'), padding.PKCS1v15(), hashes.SHA256())
    signature_b64 = base64.b64encode(signature).decode('utf-8')

    token = (
        'WECHATPAY2-SHA256-RSA2048 '
        f'mchid="{mchid}",'
        f'nonce_str="{nonce_str}",'
        f'timestamp="{timestamp}",'
        f'serial_no="{serial_no}",'
        f'signature="{signature_b64}"'
    )
    return {'Authorization': token}


def wechat_pay_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    base_url = str(os.getenv('WECHAT_PAY_API_BASE', 'https://api.mch.weixin.qq.com') or 'https://api.mch.weixin.qq.com').strip()
    base_url = base_url.rstrip('/')
    body_text = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
    headers = {
        **build_wechatpay_authorization('POST', path, body_text),
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'User-Agent': 'kwc-designer-plan/1.2',
    }
    req = UrlRequest(url=f'{base_url}{path}', data=body_text.encode('utf-8'), headers=headers, method='POST')
    try:
        with urlopen(req, timeout=12) as resp:
            raw = resp.read().decode('utf-8')
            return json.loads(raw or '{}')
    except HTTPError as exc:
        raw = exc.read().decode('utf-8', errors='ignore')
        try:
            detail = json.loads(raw or '{}')
            message = detail.get('message') or detail.get('detail') or raw
        except Exception:
            message = raw or str(exc)
        raise ValueError(f'微信退款请求失败({exc.code})：{message}')
    except Exception as exc:
        raise ValueError(f'微信退款请求异常：{exc}')


def initiate_wechat_refund_for_order(order: Dict[str, Any], reason: str) -> Dict[str, Any]:
    order_id = str(order.get('order_id') or '').strip()
    if not order_id:
        raise ValueError('订单号缺失')
    refund_amount = int(order.get('paid_amount') or 0)
    if refund_amount <= 0:
        raise ValueError('退款金额必须大于0')

    notify_url = str(os.getenv('WECHAT_PAY_REFUND_NOTIFY_URL', '') or '').strip()
    if not notify_url:
        raise ValueError('缺少 WECHAT_PAY_REFUND_NOTIFY_URL（微信退款回调地址）')

    out_refund_no = f"RFD{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:10].upper()}"[:64]
    payload = {
        'out_trade_no': order_id,
        'out_refund_no': out_refund_no,
        'reason': (reason or '').strip()[:80] or '众筹截止未达目标，原路退款',
        'notify_url': notify_url,
        'amount': {
            'refund': refund_amount,
            'total': int(order.get('paid_amount') or 0),
            'currency': 'CNY',
        },
    }
    response = wechat_pay_post('/v3/refund/domestic/refunds', payload)
    store.mark_order_refund_submitted(order_id=order_id, out_refund_no=out_refund_no, reason=payload['reason'])
    store.log_payment(
        order_id,
        'wechat_refund_submit',
        {
            'out_refund_no': out_refund_no,
            'request': payload,
            'response': response,
        },
    )
    return {
        'order_id': order_id,
        'out_refund_no': out_refund_no,
        'wechat_refund_id': str(response.get('refund_id') or ''),
        'wechat_status': str(response.get('status') or 'PROCESSING'),
    }


def verify_wechat_notify_signature(raw_body: str, timestamp: str, nonce: str, signature: str) -> None:
    if not timestamp or not nonce or not signature:
        raise ValueError('微信回调签名头缺失')

    if not get_bool_env('WECHAT_PAY_SKIP_NOTIFY_TS_CHECK', False):
        now_ts = int(time.time())
        ts = int(timestamp)
        if abs(now_ts - ts) > 300:
            raise ValueError('微信回调时间戳超出允许范围')

    public_key = load_wechat_platform_public_key()
    signed_text = f'{timestamp}\n{nonce}\n{raw_body}\n'.encode('utf-8')
    signature_bytes = base64.b64decode(signature.encode('utf-8'))

    try:
        public_key.verify(signature_bytes, signed_text, padding.PKCS1v15(), hashes.SHA256())
    except InvalidSignature:
        raise ValueError('微信回调签名校验失败')


def decrypt_wechat_notify_resource(resource: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(resource, dict):
        raise ValueError('微信回调 resource 格式错误')
    algorithm = str(resource.get('algorithm') or '').strip()
    if algorithm != 'AEAD_AES_256_GCM':
        raise ValueError(f'不支持的微信加密算法: {algorithm}')

    ciphertext = str(resource.get('ciphertext') or '').strip()
    nonce = str(resource.get('nonce') or '').strip()
    associated_data = str(resource.get('associated_data') or '')
    if not ciphertext or not nonce:
        raise ValueError('微信回调 resource 缺少密文字段')

    aesgcm = AESGCM(get_wechat_api_v3_key())
    plaintext = aesgcm.decrypt(
        nonce.encode('utf-8'),
        base64.b64decode(ciphertext.encode('utf-8')),
        associated_data.encode('utf-8') if associated_data else b'',
    )
    return json.loads(plaintext.decode('utf-8'))


def exchange_wechat_code(code: str) -> str:
    appid = os.getenv('WECHAT_APPID', '').strip()
    secret = os.getenv('WECHAT_APP_SECRET', '').strip()

    if appid and secret:
        qs = urlencode(
            {
                'appid': appid,
                'secret': secret,
                'js_code': code,
                'grant_type': 'authorization_code',
            }
        )
        url = f'https://api.weixin.qq.com/sns/jscode2session?{qs}'
        try:
            with urlopen(url, timeout=8) as resp:
                payload = json.loads(resp.read().decode('utf-8'))
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f'微信登录网关异常: {exc}')

        errcode = payload.get('errcode')
        if errcode:
            raise HTTPException(status_code=400, detail=f"微信登录失败: {payload.get('errmsg')}")

        openid = str(payload.get('openid') or '').strip()
        if not openid:
            raise HTTPException(status_code=400, detail='微信登录未返回 openid')
        return openid

    salt = os.getenv('DEV_OPENID_SALT', 'kwc-dev-salt').strip()
    hashed = hashlib.sha1(f'{salt}:{code}:{datetime.now().date()}'.encode('utf-8')).hexdigest()
    return f'dev_{hashed[:24]}'


def require_user(x_session_token: Optional[str] = Header(default=None, alias='X-Session-Token')) -> Dict[str, Any]:
    token = (x_session_token or '').strip()
    user = store.get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail='登录态失效，请重新进入小程序')
    return user


def _permissions_for_legacy_role(role: str) -> List[str]:
    mapping = {
        'superadmin': ['overview', 'project', 'order', 'user', 'submission', 'designer', 'feedback', 'log', 'setting'],
        'operator': ['overview', 'project', 'order', 'user', 'designer', 'feedback', 'log', 'setting'],
        'finance': ['overview', 'designer', 'log'],
        'reviewer': ['overview', 'submission', 'feedback', 'log'],
    }
    return list(mapping.get(role, mapping['superadmin']))


def _build_legacy_admin_identity(role: str, operator: str = '') -> Dict[str, Any]:
    display_name = operator or 'legacy-admin'
    return {
        'admin_id': 0,
        'username': 'legacy-admin',
        'display_name': display_name,
        'status': 'active',
        'role_key': role,
        'role_name': role,
        'permissions': _permissions_for_legacy_role(role),
        'session_token': '',
        'session_expires_at': '',
    }


def admin_has_permission(identity: Dict[str, Any], permission: str) -> bool:
    perm = str(permission or '').strip().lower()
    if not perm:
        return True
    role_key = str(identity.get('role_key') or '').strip().lower()
    if role_key == 'superadmin':
        return True
    perms = {str(x or '').strip().lower() for x in (identity.get('permissions') or []) if str(x or '').strip()}
    return perm in perms


def get_admin_identity_from_headers(
    x_admin_session: Optional[str] = None,
    x_admin_token: Optional[str] = None,
    x_admin_role: Optional[str] = None,
    x_admin_operator: Optional[str] = None,
    admin_token_cookie: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    session_token = (x_admin_session or '').strip()
    if session_token:
        identity = store.get_admin_identity_by_session(session_token=session_token)
        if identity:
            return identity

    token = (x_admin_token or '').strip()
    if not token:
        token = (admin_token_cookie or '').strip()
    if token == get_admin_token():
        role = normalize_admin_role(x_admin_role)
        operator = resolve_admin_actor(x_admin_operator)
        return _build_legacy_admin_identity(role=role, operator=operator)
    return None


def require_admin(
    request: Request,
    x_admin_session: Optional[str] = Header(default=None, alias='X-Admin-Session'),
    x_admin_token: Optional[str] = Header(default=None, alias='X-Admin-Token'),
    x_admin_role: Optional[str] = Header(default=None, alias='X-Admin-Role'),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    identity = get_admin_identity_from_headers(
        x_admin_session=x_admin_session,
        x_admin_token=x_admin_token,
        x_admin_role=x_admin_role,
        x_admin_operator=x_admin_operator,
        admin_token_cookie=request.cookies.get(PART_ADAPTER_PUBLIC_COOKIE),
    )
    if not identity:
        raise HTTPException(status_code=401, detail='后台登录态失效，请重新登录')
    return identity


def resolve_admin_actor(x_admin_operator: Optional[str] = None, admin_identity: Optional[Dict[str, Any]] = None) -> str:
    if admin_identity:
        name = (
            str(admin_identity.get('display_name') or '').strip()
            or str(admin_identity.get('username') or '').strip()
        )
        if name:
            return name[:64]
    raw = (x_admin_operator or '').strip()
    actor = unquote(raw) if raw else ''
    return actor[:64] if actor else 'admin'


def normalize_admin_role(x_admin_role: Optional[str] = None) -> str:
    role = (x_admin_role or '').strip().lower()
    if role in {'superadmin', 'operator', 'finance', 'reviewer'}:
        return role
    return 'superadmin'


def require_admin_role(
    allowed_roles: List[str],
    x_admin_role: Optional[str] = None,
) -> str:
    role = normalize_admin_role(x_admin_role)
    safe_allowed = {str(x or '').strip().lower() for x in allowed_roles if str(x or '').strip()}
    if role != 'superadmin' and role not in safe_allowed:
        raise HTTPException(status_code=403, detail=f'当前角色({role})无权限执行该操作')
    return role


@app.middleware('http')
async def admin_role_guard(request: Request, call_next: Any) -> Any:
    path = str(request.url.path or '')
    if path.startswith('/api/admin'):
        if path.startswith('/api/admin/auth/login'):
            return await call_next(request)
        identity = get_admin_identity_from_headers(
            x_admin_session=request.headers.get('X-Admin-Session'),
            x_admin_token=request.headers.get('X-Admin-Token'),
            x_admin_role=request.headers.get('X-Admin-Role'),
            x_admin_operator=request.headers.get('X-Admin-Operator'),
            admin_token_cookie=request.cookies.get(PART_ADAPTER_PUBLIC_COOKIE),
        )
        if not identity:
            return JSONResponse(status_code=401, content={'detail': '后台登录态失效，请重新登录'})
        request.state.admin_identity = identity
        required_permission = ''
        for rule in ADMIN_ROUTE_PERMISSION_RULES:
            if path.startswith(str(rule.get('prefix') or '')):
                required_permission = str(rule.get('permission') or '').strip().lower()
                break
        if required_permission and not admin_has_permission(identity, required_permission):
            role_key = str(identity.get('role_key') or 'custom')
            return JSONResponse(status_code=403, content={'detail': f'当前岗位({role_key})无权限访问该模块'})
    return await call_next(request)


def normalize_admin_log_time(raw: str, end_of_day: bool = False) -> str:
    text = (raw or '').strip()
    if not text:
        return ''
    text = text.replace('T', ' ').replace('/', '-')
    dt: Optional[datetime] = None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            dt = datetime.strptime(text, fmt)
            break
        except ValueError:
            continue
    if not dt:
        raise ValueError('时间格式不合法，请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM[:SS]')
    if len(text) <= 10 and end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    elif len(text) <= 10:
        dt = dt.replace(hour=0, minute=0, second=0)
    elif len(text) == 16:
        dt = dt.replace(second=59 if end_of_day else 0)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def normalize_admin_log_sort(sort_by: str, sort_order: str) -> Dict[str, str]:
    key = (sort_by or 'created_at').strip().lower()
    order = (sort_order or 'desc').strip().lower()
    if key not in {'created_at', 'actor', 'action_type'}:
        raise ValueError('排序字段仅支持 created_at / actor / action_type')
    if order not in {'asc', 'desc'}:
        raise ValueError('排序方向仅支持 asc / desc')
    return {'sort_by': key, 'sort_order': order}


def require_designer(user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    dashboard = store.get_designer_dashboard_by_user(user['user_id'])
    if not dashboard.get('is_designer'):
        raise HTTPException(status_code=403, detail='当前账号未开通设计师入口')
    return {'user': user, 'dashboard': dashboard}


def save_image_upload(request: Request, file: UploadFile, folder: str = '') -> Dict[str, str]:
    content_type = (file.content_type or '').lower()
    if not content_type.startswith('image/'):
        raise ValueError('仅支持图片文件')

    raw = file.file.read()
    if len(raw) > 10 * 1024 * 1024:
        raise ValueError('图片大小不能超过 10MB')

    ext = Path(file.filename or '').suffix.lower()
    if ext not in {'.jpg', '.jpeg', '.png', '.webp', '.gif'}:
        ext = '.jpg'

    day = datetime.now().strftime('%Y-%m-%d')
    safe_folder = '/'.join([x for x in str(folder or '').split('/') if x and x not in {'.', '..'}]).strip('/')
    if safe_folder:
        day_dir = UPLOAD_DIR / safe_folder / day
    else:
        day_dir = UPLOAD_DIR / day
    day_dir.mkdir(parents=True, exist_ok=True)

    filename = f'{uuid.uuid4().hex}{ext}'
    target = day_dir / filename
    target.write_bytes(raw)

    relative_path = target.relative_to(DATA_DIR).as_posix()
    url_path = f'/static/{relative_path}'
    absolute_url = f"{str(request.base_url).rstrip('/')}{url_path}"
    return {'url': url_path, 'absolute_url': absolute_url}


def build_payment_payload(order: Dict[str, Any]) -> Dict[str, Any]:
    mode = get_pay_mode()
    if mode == 'mock':
        mock_token = uuid.uuid4().hex
        payload = {'mode': 'mock', 'mock_token': mock_token, 'message': '开发模式支付：用于联调下单链路'}
        store.log_payment(order['order_id'], 'mock', payload)
        return payload

    raise HTTPException(status_code=501, detail='当前环境未启用真实微信支付，请先配置商户参数并接入支付网关。')


@app.get('/health')
def health() -> Dict[str, str]:
    return {'status': 'ok', 'time': now_iso()}


@app.post('/api/auth/login')
def api_auth_login(payload: LoginRequest) -> Dict[str, Any]:
    code = (payload.code or '').strip()
    if not code:
        raise HTTPException(status_code=400, detail='缺少 login code')

    openid = exchange_wechat_code(code)
    user = store.upsert_user_session(openid=openid, nickname=payload.nickname)
    return {
        'session_token': user['session_token'],
        'is_new_user': bool(user.get('is_new_user')),
        'user': {
            'user_id': user['user_id'],
            'openid': user['openid'],
            'nickname': user['nickname'],
        },
    }


@app.post('/api/admin/auth/login')
def api_admin_auth_login(payload: AdminAuthLoginRequest) -> Dict[str, Any]:
    try:
        return store.admin_login(username=(payload.username or '').strip(), password=payload.password or '')
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get('/api/admin/auth/me')
def api_admin_auth_me(admin: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    return {'admin': admin}


@app.post('/api/admin/auth/logout')
def api_admin_auth_logout(
    admin: Dict[str, Any] = Depends(require_admin),
    x_admin_session: Optional[str] = Header(default=None, alias='X-Admin-Session'),
) -> Dict[str, Any]:
    _ = admin
    token = (x_admin_session or '').strip()
    if token:
        store.admin_logout(token)
    return {'ok': True}


@app.get('/api/work/current')
def api_get_current_work() -> Dict[str, Any]:
    return {'work': store.get_current_work()}


@app.get('/api/work/{work_id}/updates')
def api_work_updates(work_id: str, limit: int = 20) -> Dict[str, Any]:
    items = store.list_work_updates_public(work_id=work_id.strip(), limit=limit)
    return {'items': items}


@app.get('/api/work/{work_id}/comments')
def api_work_comments(work_id: str, limit: int = 50) -> Dict[str, Any]:
    items = store.list_project_comments_public(work_id=work_id.strip(), limit=limit)
    return {'items': items}


@app.get('/api/designers/{designer_id}')
def api_designer_public_profile(designer_id: int) -> Dict[str, Any]:
    ret = store.get_designer_public_profile(designer_id=designer_id)
    if not ret:
        raise HTTPException(status_code=404, detail='设计师不存在')
    return ret


@app.post('/api/work/{work_id}/comments')
def api_create_work_comment(
    work_id: str,
    payload: WorkCommentCreateRequest,
    user: Dict[str, Any] = Depends(require_user),
) -> Dict[str, Any]:
    try:
        item = store.create_project_comment(
            user_id=int(user['user_id']),
            work_id=work_id.strip(),
            content=(payload.content or '').strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {'item': item}


@app.post('/api/reservations')
def api_reserve_work(payload: ReservationRequest, user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    if not payload.work_id.strip():
        raise HTTPException(status_code=400, detail='work_id 不能为空')
    return store.reserve_work(user_id=user['user_id'], work_id=payload.work_id.strip())


@app.get('/api/me/summary')
def api_me_summary(user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    return store.get_my_summary(user_id=user['user_id'])


@app.get('/api/me/profile')
def api_me_profile(user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    return {'profile': store.get_user_profile(user_id=user['user_id'])}


@app.put('/api/me/profile')
def api_update_me_profile(payload: UserProfileUpdateRequest, user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    try:
        profile = store.update_user_profile(user_id=user['user_id'], nickname=(payload.nickname or '').strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {'profile': profile}


@app.get('/api/me/feedback')
def api_my_feedback(limit: int = 50, user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    return {'items': store.list_feedback_by_user(user_id=user['user_id'], limit=limit)}


@app.post('/api/me/feedback')
def api_create_feedback(payload: FeedbackCreateRequest, user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    try:
        item = store.create_feedback(
            user_id=user['user_id'],
            category=(payload.category or 'general').strip(),
            priority=(payload.priority or 'normal').strip(),
            content=(payload.content or '').strip(),
            contact=(payload.contact or '').strip(),
            image_urls=[str(x).strip() for x in (payload.image_urls or []) if str(x).strip()],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {'item': item}


@app.get('/api/me/orders')
def api_me_orders(
    limit: int = 20,
    offset: int = 0,
    status: str = '',
    sale_mode: str = '',
    period_days: int = 0,
    user: Dict[str, Any] = Depends(require_user),
) -> Dict[str, Any]:
    return store.list_orders_by_user_filtered(
        user_id=user['user_id'],
        limit=limit,
        offset=offset,
        status=status.strip(),
        sale_mode=sale_mode.strip(),
        period_days=period_days,
    )


@app.post('/api/orders/preorder')
def api_create_preorder(payload: PreorderRequest, user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    try:
        order = store.create_preorder(user_id=user['user_id'], sku_id=payload.sku_id.strip(), quantity=payload.quantity)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    payment = build_payment_payload(order)
    return {'order': order, 'payment': payment}


@app.post('/api/payments/confirm')
def api_confirm_payment(payload: PaymentConfirmRequest, user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    source = (payload.source or 'mock').strip()[:32]
    transaction_id = (payload.transaction_id or '').strip()[:128]

    try:
        order = store.mark_order_paid(
            order_id=payload.order_id.strip(),
            user_id=user['user_id'],
            payment_channel=source,
            transaction_id=transaction_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    store.log_payment(order['order_id'], 'confirm', {'source': source, 'transaction_id': transaction_id})
    return {'ok': True, 'order': order}


@app.post('/api/payments/wechat/refund/notify')
async def api_wechat_refund_notify(
    request: Request,
    wechatpay_timestamp: str = Header(default='', alias='Wechatpay-Timestamp'),
    wechatpay_nonce: str = Header(default='', alias='Wechatpay-Nonce'),
    wechatpay_signature: str = Header(default='', alias='Wechatpay-Signature'),
    wechatpay_serial: str = Header(default='', alias='Wechatpay-Serial'),
) -> Any:
    _ = wechatpay_serial
    try:
        raw_body = (await request.body()).decode('utf-8')
        if not raw_body:
            raise ValueError('微信回调 body 为空')

        if not get_bool_env('WECHAT_PAY_SKIP_NOTIFY_VERIFY', False):
            verify_wechat_notify_signature(
                raw_body=raw_body,
                timestamp=wechatpay_timestamp,
                nonce=wechatpay_nonce,
                signature=wechatpay_signature,
            )

        payload = json.loads(raw_body)
        resource = payload.get('resource') or {}
        decrypted = decrypt_wechat_notify_resource(resource)

        order_id = str(decrypted.get('out_trade_no') or '').strip()
        if not order_id:
            raise ValueError('微信退款回调缺少 out_trade_no')

        amount = decrypted.get('amount') or {}
        refund_amount = int(amount.get('refund') or 0)
        refund_status = str(decrypted.get('refund_status') or '').strip().upper()
        if not refund_status:
            event_type = str(payload.get('event_type') or '').strip().upper()
            if event_type == 'REFUND.SUCCESS':
                refund_status = 'SUCCESS'
            elif event_type == 'REFUND.ABNORMAL':
                refund_status = 'ABNORMAL'
            elif event_type == 'REFUND.CLOSED':
                refund_status = 'CLOSED'
            else:
                refund_status = 'PROCESSING'

        order = store.mark_order_refund_by_notify(
            order_id=order_id,
            wechat_refund_status=refund_status,
            refund_amount=refund_amount,
            refunded_at=str(decrypted.get('success_time') or ''),
            reason=str(payload.get('summary') or ''),
            out_refund_no=str(decrypted.get('out_refund_no') or ''),
            refund_id=str(decrypted.get('refund_id') or ''),
        )
        store.log_payment(
            order_id,
            'wechat_refund_notify_event',
            {
                'event_id': payload.get('id'),
                'event_type': payload.get('event_type'),
                'refund_status': refund_status,
                'out_refund_no': decrypted.get('out_refund_no'),
                'order_status': order.get('order_status'),
            },
        )
        return JSONResponse(status_code=200, content={'code': 'SUCCESS', 'message': '成功'})
    except Exception as exc:
        return JSONResponse(status_code=500, content={'code': 'FAIL', 'message': str(exc)[:180]})


@app.post('/api/submissions')
def api_create_submission(payload: SubmissionCreateRequest, user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    if len(payload.intro.strip()) < 30:
        raise HTTPException(status_code=400, detail='作品简介至少 30 字')
    if not payload.image_urls:
        raise HTTPException(status_code=400, detail='请至少上传 1 张作品图')

    try:
        submission = store.create_submission(user_id=user['user_id'], payload=payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {'submission': submission}


@app.post('/api/designer/enroll')
def api_designer_enroll(payload: DesignerEnrollRequest, user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    try:
        profile = store.enroll_designer(user_id=user['user_id'], display_name=payload.display_name, bio=payload.bio)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    dashboard = store.get_designer_dashboard_by_user(user['user_id'])
    return {'profile': profile, 'dashboard': dashboard}


@app.get('/api/designer/me/dashboard')
def api_designer_dashboard(designer: Dict[str, Any] = Depends(require_designer)) -> Dict[str, Any]:
    return designer['dashboard']


@app.get('/api/designer/me/orders')
def api_designer_orders(limit: int = 100, designer: Dict[str, Any] = Depends(require_designer)) -> Dict[str, Any]:
    profile = designer['dashboard']['profile']
    items = store.list_designer_orders(designer_id=int(profile['designer_id']), limit=limit)
    return {'items': items}


@app.get('/api/designer/me/updates')
def api_designer_updates(limit: int = 50, designer: Dict[str, Any] = Depends(require_designer)) -> Dict[str, Any]:
    profile = designer['dashboard']['profile']
    items = store.list_designer_updates(designer_id=int(profile['designer_id']), limit=limit)
    return {'items': items}


@app.post('/api/designer/me/updates')
def api_designer_create_update(payload: DesignerUpdateCreateRequest, designer: Dict[str, Any] = Depends(require_designer)) -> Dict[str, Any]:
    profile = designer['dashboard']['profile']
    try:
        item = store.create_designer_update(
            designer_id=int(profile['designer_id']),
            work_id=payload.work_id,
            title=payload.title,
            content=payload.content,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {'item': item}


@app.get('/api/designer/me/projects')
def api_designer_projects(limit: int = 100, designer: Dict[str, Any] = Depends(require_designer)) -> Dict[str, Any]:
    profile = designer['dashboard']['profile']
    items = store.list_designer_projects(designer_id=int(profile['designer_id']), limit=limit)
    return {'items': items}


@app.put('/api/designer/me/projects/{work_id}')
def api_designer_update_project(
    work_id: str,
    payload: DesignerProjectMaintainRequest,
    designer: Dict[str, Any] = Depends(require_designer),
) -> Dict[str, Any]:
    profile = designer['dashboard']['profile']
    try:
        item = store.designer_update_project(
            designer_id=int(profile['designer_id']),
            work_id=work_id.strip(),
            payload=payload.model_dump(exclude_none=True),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {'item': item}


@app.put('/api/designer/me/profile')
def api_designer_update_profile(
    payload: DesignerProfileUpdateRequest,
    designer: Dict[str, Any] = Depends(require_designer),
) -> Dict[str, Any]:
    profile = designer['dashboard']['profile']
    try:
        item = store.update_designer_profile(
            designer_id=int(profile['designer_id']),
            display_name=(payload.display_name or '').strip(),
            bio=(payload.bio or '').strip(),
            avatar_url=(payload.avatar_url or '').strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {'profile': item}


@app.get('/api/designer/me/comments')
def api_designer_comments(
    work_id: str = '',
    limit: int = 100,
    designer: Dict[str, Any] = Depends(require_designer),
) -> Dict[str, Any]:
    profile = designer['dashboard']['profile']
    try:
        items = store.list_designer_comments(
            designer_id=int(profile['designer_id']),
            work_id=work_id.strip(),
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {'items': items}


@app.post('/api/designer/me/comments/{comment_id}/reply')
def api_designer_reply_comment(
    comment_id: str,
    payload: DesignerCommentReplyRequest,
    designer: Dict[str, Any] = Depends(require_designer),
) -> Dict[str, Any]:
    profile = designer['dashboard']['profile']
    try:
        item = store.reply_project_comment(
            designer_id=int(profile['designer_id']),
            comment_id=comment_id.strip(),
            reply_content=(payload.reply_content or '').strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {'item': item}


@app.post('/api/uploads/image')
async def api_upload_image(request: Request, file: UploadFile = File(...), user: Dict[str, Any] = Depends(require_user)) -> Dict[str, str]:
    _ = user
    try:
        return save_image_upload(request=request, file=file, folder='')
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post('/api/admin/uploads/image')
async def api_admin_upload_image(
    request: Request,
    scene: str = 'project',
    file: UploadFile = File(...),
    admin: bool = Depends(require_admin),
) -> Dict[str, str]:
    _ = admin
    safe_scene = (scene or 'project').strip().lower()
    if safe_scene not in {'project', 'submission', 'feedback', 'other'}:
        safe_scene = 'project'
    try:
        return save_image_upload(request=request, file=file, folder=f'admin/{safe_scene}')
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get('/api/admin/dashboard')
def api_admin_dashboard(admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return {'dashboard': store.admin_dashboard(), 'pay_mode': get_pay_mode()}


@app.get('/api/admin/settings')
def api_admin_get_settings(admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return store.get_admin_settings()


@app.put('/api/admin/settings')
def api_admin_update_settings(payload: AdminSettingsUpdateRequest, admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    try:
        return store.update_admin_settings(payload=payload.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get('/api/admin/roles')
def api_admin_roles(admin: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return {'items': store.list_admin_roles()}


@app.post('/api/admin/roles')
def api_admin_create_role(
    payload: AdminRoleCreateRequest,
    admin: Dict[str, Any] = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    actor = resolve_admin_actor(x_admin_operator, admin_identity=admin)
    try:
        item = store.create_admin_role(
            role_key=(payload.role_key or '').strip(),
            role_name=(payload.role_name or '').strip(),
            permissions=payload.permissions or [],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.log_admin_action(
        actor=actor,
        action_type='admin_role_create',
        target_type='admin_role',
        target_id=str(item.get('role_key') or ''),
        related_user_id=0,
        detail={'role_name': item.get('role_name', ''), 'permissions': item.get('permissions', [])},
    )
    return {'item': item}


@app.put('/api/admin/roles/{role_key}')
def api_admin_update_role(
    role_key: str,
    payload: AdminRoleUpdateRequest,
    admin: Dict[str, Any] = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    actor = resolve_admin_actor(x_admin_operator, admin_identity=admin)
    try:
        item = store.update_admin_role(
            role_key=role_key.strip().lower(),
            role_name=payload.role_name.strip() if isinstance(payload.role_name, str) else None,
            permissions=payload.permissions if isinstance(payload.permissions, list) else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.log_admin_action(
        actor=actor,
        action_type='admin_role_update',
        target_type='admin_role',
        target_id=str(item.get('role_key') or ''),
        related_user_id=0,
        detail={'role_name': item.get('role_name', ''), 'permissions': item.get('permissions', [])},
    )
    return {'item': item}


@app.get('/api/admin/admin-users')
def api_admin_admin_users(limit: int = 500, admin: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return {'items': store.list_admin_users(limit=limit)}


@app.post('/api/admin/admin-users')
def api_admin_create_admin_user(
    payload: AdminUserCreateRequest,
    admin: Dict[str, Any] = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    actor = resolve_admin_actor(x_admin_operator, admin_identity=admin)
    try:
        item = store.create_admin_user(
            username=(payload.username or '').strip(),
            password=payload.password or '',
            role_key=(payload.role_key or '').strip().lower(),
            display_name=(payload.display_name or '').strip(),
            status=(payload.status or 'active').strip().lower(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.log_admin_action(
        actor=actor,
        action_type='admin_user_create',
        target_type='admin_user',
        target_id=str(item.get('admin_id') or ''),
        related_user_id=0,
        detail={'username': item.get('username', ''), 'role_key': item.get('role_key', ''), 'status': item.get('status', '')},
    )
    return {'item': item}


@app.put('/api/admin/admin-users/{admin_id}')
def api_admin_update_admin_user(
    admin_id: int,
    payload: AdminUserUpdateRequest,
    admin: Dict[str, Any] = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    actor = resolve_admin_actor(x_admin_operator, admin_identity=admin)
    try:
        item = store.update_admin_user(
            admin_id=int(admin_id),
            display_name=payload.display_name.strip() if isinstance(payload.display_name, str) else None,
            role_key=payload.role_key.strip().lower() if isinstance(payload.role_key, str) else None,
            status=payload.status.strip().lower() if isinstance(payload.status, str) else None,
            password=payload.password if isinstance(payload.password, str) else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.log_admin_action(
        actor=actor,
        action_type='admin_user_update',
        target_type='admin_user',
        target_id=str(item.get('admin_id') or ''),
        related_user_id=0,
        detail={'username': item.get('username', ''), 'role_key': item.get('role_key', ''), 'status': item.get('status', '')},
    )
    return {'item': item}


@app.get('/api/admin/orders')
def api_admin_orders(
    limit: int = 200,
    keyword: str = '',
    sale_mode: str = '',
    pay_status: str = '',
    order_status: str = '',
    refund_status: str = '',
    admin: bool = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    ret = store.admin_list_orders(
        limit=limit,
        keyword=keyword.strip(),
        sale_mode=sale_mode.strip(),
        pay_status=pay_status.strip(),
        order_status=order_status.strip(),
        refund_status=refund_status.strip(),
    )
    return ret


@app.get('/api/admin/orders/export.csv')
def api_admin_export_orders_csv(
    limit: int = 5000,
    keyword: str = '',
    sale_mode: str = '',
    pay_status: str = '',
    order_status: str = '',
    refund_status: str = '',
    admin: bool = Depends(require_admin),
) -> Response:
    _ = admin
    csv_text = store.admin_export_orders_csv(
        limit=limit,
        keyword=keyword.strip(),
        sale_mode=sale_mode.strip(),
        pay_status=pay_status.strip(),
        order_status=order_status.strip(),
        refund_status=refund_status.strip(),
    )
    filename = f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return Response(content=csv_text, media_type='text/csv; charset=utf-8', headers=headers)


@app.post('/api/admin/orders/{order_id}/note')
def api_admin_set_order_note(
    order_id: str,
    payload: AdminOrderNoteRequest,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    try:
        order = store.set_order_admin_note(order_id=order_id.strip(), note=(payload.note or '').strip())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    store.log_admin_action(
        actor=actor,
        action_type='order_note_update',
        target_type='order',
        target_id=order.get('order_id', ''),
        related_user_id=int(order.get('user_id') or 0),
        detail={'note': order.get('admin_note', '')},
    )
    return {'order': order}


@app.post('/api/admin/orders/{order_id}/retry-refund')
def api_admin_retry_order_refund(
    order_id: str,
    payload: AdminOrderRetryRefundRequest,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    if get_pay_mode() != 'wechat':
        raise HTTPException(status_code=400, detail='当前 PAY_MODE 不是 wechat，无法发起真实退款重试')

    order = store.get_order_by_id(order_id.strip())
    if not order:
        raise HTTPException(status_code=404, detail='订单不存在')
    if order.get('sale_mode') != 'crowdfunding':
        raise HTTPException(status_code=400, detail='仅众筹订单支持退款重试')
    if order.get('pay_status') != 'paid':
        raise HTTPException(status_code=400, detail='仅已支付且未退款订单支持退款重试')
    if (order.get('refund_status') or 'none') == 'processing':
        raise HTTPException(status_code=400, detail='该订单退款处理中，请稍后查看结果')

    try:
        result = initiate_wechat_refund_for_order(order=order, reason=(payload.reason or '').strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    latest = store.get_order_by_id(order_id.strip())
    store.log_admin_action(
        actor=actor,
        action_type='order_refund_retry',
        target_type='order',
        target_id=order.get('order_id', ''),
        related_user_id=int(order.get('user_id') or 0),
        detail={
            'reason': (payload.reason or '').strip(),
            'wechat_status': result.get('wechat_status'),
            'out_refund_no': result.get('out_refund_no'),
        },
    )
    return {'ok': True, 'result': result, 'order': latest}


@app.get('/api/admin/users')
def api_admin_users(keyword: str = '', limit: int = 200, admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return store.admin_list_users(keyword=keyword.strip(), limit=limit)


@app.get('/api/admin/feedback')
def api_admin_feedback(
    status: str = '',
    keyword: str = '',
    priority: str = '',
    limit: int = 200,
    admin: bool = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    return store.admin_list_feedback(status=status.strip(), keyword=keyword.strip(), priority=priority.strip(), limit=limit)


@app.get('/api/admin/feedback/export.csv')
def api_admin_export_feedback_csv(
    status: str = '',
    keyword: str = '',
    priority: str = '',
    limit: int = 5000,
    admin: bool = Depends(require_admin),
) -> Response:
    _ = admin
    csv_text = store.admin_export_feedback_csv(
        status=status.strip(),
        keyword=keyword.strip(),
        priority=priority.strip(),
        limit=limit,
    )
    filename = f"feedback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return Response(content=csv_text, media_type='text/csv; charset=utf-8', headers=headers)


@app.post('/api/admin/feedback/{feedback_id}/reply')
def api_admin_reply_feedback(
    feedback_id: int,
    payload: AdminFeedbackReplyRequest,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    final_reply = (payload.admin_reply or '').strip()
    template_code = (payload.template_code or '').strip()
    if template_code and not final_reply:
        tpl = store.get_feedback_template_by_code(template_code)
        if tpl and tpl.get('is_active'):
            final_reply = str(tpl.get('content') or '').strip()
    try:
        item = store.admin_reply_feedback(
            feedback_id=int(feedback_id),
            status=(payload.status or '').strip(),
            admin_reply=final_reply,
            reply_operator=actor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.log_admin_action(
        actor=actor,
        action_type='feedback_reply_update',
        target_type='feedback',
        target_id=str(item.get('id') or ''),
        related_user_id=int(item.get('user_id') or 0),
        detail={
            'status': item.get('status', ''),
            'admin_reply': item.get('admin_reply', ''),
            'template_code': template_code,
        },
    )
    return {'item': item}


@app.get('/api/admin/feedback/templates')
def api_admin_feedback_templates(active_only: int = 0, limit: int = 200, admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return {'items': store.list_feedback_templates(active_only=bool(int(active_only or 0)), limit=limit)}


@app.post('/api/admin/feedback/templates/upsert')
def api_admin_upsert_feedback_template(
    payload: AdminFeedbackTemplateUpsertRequest,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    try:
        item = store.upsert_feedback_template(
            code=(payload.code or '').strip(),
            title=(payload.title or '').strip(),
            content=(payload.content or '').strip(),
            is_active=bool(payload.is_active),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.log_admin_action(
        actor=actor,
        action_type='feedback_template_upsert',
        target_type='feedback_template',
        target_id=str(item.get('code') or ''),
        related_user_id=0,
        detail={
            'title': item.get('title', ''),
            'is_active': bool(item.get('is_active')),
        },
    )
    return {'item': item}


@app.get('/api/admin/users/{user_id}/detail')
def api_admin_user_detail(
    user_id: int,
    order_limit: int = 100,
    submission_limit: int = 100,
    reservation_limit: int = 100,
    commission_limit: int = 100,
    action_limit: int = 200,
    admin: bool = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    try:
        detail = store.admin_get_user_detail(
            user_id=int(user_id),
            order_limit=order_limit,
            submission_limit=submission_limit,
            reservation_limit=reservation_limit,
            commission_limit=commission_limit,
            action_limit=action_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return detail


@app.get('/api/admin/users/{user_id}/orders/export.csv')
def api_admin_export_user_orders_csv(user_id: int, limit: int = 5000, admin: bool = Depends(require_admin)) -> Response:
    _ = admin
    detail = store.admin_get_user_detail(user_id=int(user_id), order_limit=1)
    user = detail.get('user') or {}
    openid = str(user.get('openid') or 'user')
    csv_text = store.admin_export_user_orders_csv(user_id=int(user_id), limit=limit)
    filename = f"user_orders_{openid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return Response(content=csv_text, media_type='text/csv; charset=utf-8', headers=headers)


@app.get('/api/admin/action-logs')
def api_admin_action_logs(
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
    admin: bool = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    try:
        safe_created_from = normalize_admin_log_time(created_from, end_of_day=False) if created_from else ''
        safe_created_to = normalize_admin_log_time(created_to, end_of_day=True) if created_to else ''
        safe_sort = normalize_admin_log_sort(sort_by, sort_order)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return store.admin_list_action_logs(
        limit=limit,
        offset=offset,
        actor=actor.strip(),
        action_type=action_type.strip(),
        target_type=target_type.strip(),
        target_id=target_id.strip(),
        related_user_id=max(0, int(related_user_id or 0)),
        created_from=safe_created_from,
        created_to=safe_created_to,
        sort_by=safe_sort['sort_by'],
        sort_order=safe_sort['sort_order'],
    )


@app.get('/api/admin/action-logs/export.csv')
def api_admin_export_action_logs_csv(
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
    admin: bool = Depends(require_admin),
) -> Response:
    _ = admin
    try:
        safe_created_from = normalize_admin_log_time(created_from, end_of_day=False) if created_from else ''
        safe_created_to = normalize_admin_log_time(created_to, end_of_day=True) if created_to else ''
        safe_sort = normalize_admin_log_sort(sort_by, sort_order)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    csv_text = store.admin_export_action_logs_csv(
        limit=limit,
        actor=actor.strip(),
        action_type=action_type.strip(),
        target_type=target_type.strip(),
        target_id=target_id.strip(),
        related_user_id=max(0, int(related_user_id or 0)),
        created_from=safe_created_from,
        created_to=safe_created_to,
        sort_by=safe_sort['sort_by'],
        sort_order=safe_sort['sort_order'],
    )
    filename = f"admin_action_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return Response(content=csv_text, media_type='text/csv; charset=utf-8', headers=headers)


@app.post('/api/admin/refunds/crowdfunding/initiate')
def api_admin_initiate_crowdfunding_refunds(
    payload: AdminCrowdfundingRefundInitiateRequest,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    if get_pay_mode() != 'wechat':
        raise HTTPException(status_code=400, detail='当前 PAY_MODE 不是 wechat，无法提交真实退款')

    candidates = store.list_pending_crowdfunding_refunds(limit=int(payload.limit or 50))
    if not candidates:
        return {'ok': True, 'total': 0, 'success': 0, 'failed': 0, 'items': []}

    items: List[Dict[str, Any]] = []
    success = 0
    failed = 0
    for order in candidates:
        try:
            result = initiate_wechat_refund_for_order(order=order, reason=(payload.reason or '').strip())
            items.append({'ok': True, **result})
            success += 1
            store.log_admin_action(
                actor=actor,
                action_type='crowdfunding_refund_submit',
                target_type='order',
                target_id=str(order.get('order_id') or ''),
                related_user_id=int(order.get('user_id') or 0),
                detail={
                    'reason': (payload.reason or '').strip(),
                    'wechat_status': result.get('wechat_status'),
                    'out_refund_no': result.get('out_refund_no'),
                },
            )
        except Exception as exc:
            failed += 1
            order_id = str(order.get('order_id') or '')
            if order_id:
                store.log_payment(order_id, 'wechat_refund_submit_error', {'error': str(exc)})
                store.log_admin_action(
                    actor=actor,
                    action_type='crowdfunding_refund_submit_error',
                    target_type='order',
                    target_id=order_id,
                    related_user_id=int(order.get('user_id') or 0),
                    detail={'error': str(exc)},
                )
            items.append({'ok': False, 'order_id': order_id, 'error': str(exc)})
    return {'ok': failed == 0, 'total': len(candidates), 'success': success, 'failed': failed, 'items': items}


@app.get('/api/admin/submissions')
def api_admin_submissions(status: str = '', limit: int = 200, admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return {'items': store.admin_list_submissions(status=status.strip(), limit=limit)}


@app.post('/api/admin/submissions/{submission_id}/review')
def api_admin_review_submission(submission_id: str, payload: SubmissionReviewRequest, admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    try:
        item = store.admin_review_submission(submission_id=submission_id.strip(), status=payload.status.strip(), note=payload.note.strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {'submission': item}


@app.post('/api/admin/submissions/{submission_id}/activate-designer')
def api_admin_activate_designer_from_submission(
    submission_id: str,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    try:
        ret = store.admin_activate_designer_from_submission(submission_id=submission_id.strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    profile = ret.get('profile') or {}
    submission = ret.get('submission') or {}
    store.log_admin_action(
        actor=actor,
        action_type='designer_activate_from_submission',
        target_type='submission',
        target_id=str(submission.get('submission_id') or submission_id),
        related_user_id=int(profile.get('user_id') or submission.get('user_id') or 0),
        detail={
            'designer_id': int(profile.get('designer_id') or 0),
            'display_name': str(profile.get('display_name') or ''),
            'was_designer': bool(ret.get('was_designer')),
            'created': bool(ret.get('created')),
        },
    )
    return ret


@app.get('/api/admin/reservations')
def api_admin_reservations(limit: int = 500, admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return {'items': store.admin_list_reservations(limit=limit)}


@app.get('/api/admin/projects/designers/options')
def api_admin_project_designer_options(
    limit: int = 500,
    active_only: bool = True,
    admin: bool = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    items = store.list_designers(limit=limit)
    if active_only:
        items = [it for it in items if str(it.get('status') or 'active') == 'active']
    result = []
    for it in items:
        openid = str(it.get('openid') or '').strip()
        if not openid:
            continue
        result.append(
            {
                'designer_id': int(it.get('designer_id') or 0),
                'openid': openid,
                'display_name': str(it.get('display_name') or '').strip() or str(it.get('nickname') or '').strip() or openid,
                'status': str(it.get('status') or ''),
                'status_text': str(it.get('status_text') or ''),
                'default_share_ratio': float(it.get('default_share_ratio') or DEFAULT_DESIGNER_SHARE),
                'default_share_percent': float(it.get('default_share_percent') or round(DEFAULT_DESIGNER_SHARE * 100, 2)),
                'avatar_url': str(it.get('avatar_url') or ''),
            }
        )
    return {'items': result}


@app.get('/api/admin/designers')
def api_admin_designers(limit: int = 200, admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return {'items': store.list_designers(limit=limit)}


@app.post('/api/admin/designers/assign')
def api_admin_assign_designer(payload: AdminDesignerAssignRequest, admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    try:
        ret = store.bind_designer_work(openid=payload.openid, work_id=payload.work_id, share_ratio=float(payload.share_ratio))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return ret


@app.get('/api/admin/commissions')
def api_admin_commissions(status: str = '', limit: int = 200, admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return {'items': store.admin_list_commissions(status=status.strip(), limit=limit)}


@app.post('/api/admin/commissions/{record_id}/settle')
def api_admin_settle_commission(
    record_id: int,
    payload: AdminCommissionSettleRequest,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    try:
        item = store.set_commission_settlement(
            record_id=int(record_id),
            status=(payload.settlement_status or 'settled').strip(),
            note=(payload.settlement_note or '').strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.log_admin_action(
        actor=actor,
        action_type='commission_settlement_update',
        target_type='commission',
        target_id=str(item.get('record_id', '')),
        related_user_id=int(item.get('designer_user_id') or 0),
        detail={
            'settlement_status': item.get('settlement_status'),
            'settlement_note': item.get('settlement_note', ''),
            'order_id': item.get('order_id', ''),
            'commission_amount': item.get('commission_amount', 0),
        },
    )
    return {'item': item}


@app.post('/api/admin/commissions/batch-settle')
def api_admin_batch_settle_commission(
    payload: AdminCommissionBatchSettleRequest,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    try:
        ret = store.set_commission_settlement_batch(
            status=(payload.settlement_status or 'settled').strip(),
            note=(payload.settlement_note or '').strip(),
            record_ids=payload.record_ids or [],
            from_status=(payload.from_status or '').strip(),
            limit=int(payload.limit or 1000),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    for item in ret.get('items') or []:
        store.log_admin_action(
            actor=actor,
            action_type='commission_settlement_batch_update',
            target_type='commission',
            target_id=str(item.get('record_id', '')),
            related_user_id=int(item.get('designer_user_id') or 0),
            detail={
                'settlement_status': item.get('settlement_status'),
                'settlement_note': item.get('settlement_note', ''),
                'order_id': item.get('order_id', ''),
                'commission_amount': item.get('commission_amount', 0),
            },
        )
    return ret


@app.get('/api/admin/commissions/export.csv')
def api_admin_export_commissions(status: str = '', limit: int = 5000, admin: bool = Depends(require_admin)) -> Response:
    _ = admin
    csv_text = store.admin_export_commissions_csv(status=status.strip(), limit=limit)
    filename = f"designer_commissions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return Response(content=csv_text, media_type='text/csv; charset=utf-8', headers=headers)


@app.get('/api/admin/projects')
def api_admin_projects(
    keyword: str = '',
    sale_mode: str = '',
    is_current: int = -1,
    limit: int = 200,
    admin: bool = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    return store.admin_list_projects(
        keyword=keyword.strip(),
        sale_mode=sale_mode.strip(),
        is_current=int(is_current),
        limit=limit,
    )


@app.post('/api/admin/projects')
def api_admin_create_project(
    payload: AdminProjectCreateRequest,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    designer_openid = (payload.designer_openid or '').strip()
    designer_share_ratio = float(payload.designer_share_ratio or DEFAULT_DESIGNER_SHARE)
    if not designer_openid:
        raise HTTPException(status_code=400, detail='创建项目必须绑定设计师 openid')
    if designer_openid:
        target_user = store.get_user_by_openid(designer_openid)
        if not target_user:
            raise HTTPException(status_code=400, detail='绑定失败：未找到该 openid 对应用户，请先登录小程序')
        qualification = store.get_designer_qualification_by_user(user_id=int(target_user['user_id']), limit=200)
        if not qualification.get('can_enroll'):
            raise HTTPException(status_code=400, detail='绑定失败：该用户尚无审核通过投稿，不能绑定为设计师')
    try:
        item = store.admin_create_project(payload=payload.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    designer_binding: Dict[str, Any] = {}
    if designer_openid:
        try:
            designer_binding = store.bind_designer_work(
                openid=designer_openid,
                work_id=str(item.get('work_id') or ''),
                share_ratio=designer_share_ratio,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f'项目创建成功，但设计师绑定失败：{exc}')
    store.log_admin_action(
        actor=actor,
        action_type='project_create',
        target_type='project',
        target_id=str(item.get('work_id') or ''),
        related_user_id=0,
        detail={
            'name': item.get('name', ''),
            'sale_mode': item.get('sale_mode', ''),
            'is_current': bool(item.get('is_current')),
            'designer_openid': designer_openid,
            'designer_share_ratio': designer_share_ratio if designer_openid else 0,
        },
    )
    return {'item': item, 'designer_binding': designer_binding}


@app.put('/api/admin/projects/{work_id}')
def api_admin_update_project(
    work_id: str,
    payload: AdminProjectUpdateRequest,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    designer_openid = (payload.designer_openid or '').strip()
    designer_share_ratio = float(payload.designer_share_ratio or DEFAULT_DESIGNER_SHARE)
    if designer_openid:
        target_user = store.get_user_by_openid(designer_openid)
        if not target_user:
            raise HTTPException(status_code=400, detail='绑定失败：未找到该 openid 对应用户，请先登录小程序')
        qualification = store.get_designer_qualification_by_user(user_id=int(target_user['user_id']), limit=200)
        if not qualification.get('can_enroll'):
            raise HTTPException(status_code=400, detail='绑定失败：该用户尚无审核通过投稿，不能绑定为设计师')
    try:
        item = store.admin_update_project(
            work_id=work_id.strip(),
            payload=payload.model_dump(exclude_none=True),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    designer_binding: Dict[str, Any] = {}
    if designer_openid:
        try:
            designer_binding = store.bind_designer_work(
                openid=designer_openid,
                work_id=str(item.get('work_id') or ''),
                share_ratio=designer_share_ratio,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f'项目更新成功，但设计师绑定失败：{exc}')
    store.log_admin_action(
        actor=actor,
        action_type='project_update',
        target_type='project',
        target_id=str(item.get('work_id') or ''),
        related_user_id=0,
        detail={
            'name': item.get('name', ''),
            'sale_mode': item.get('sale_mode', ''),
            'is_current': bool(item.get('is_current')),
            'designer_openid': designer_openid,
            'designer_share_ratio': designer_share_ratio if designer_openid else 0,
        },
    )
    return {'item': item, 'designer_binding': designer_binding}


@app.post('/api/admin/projects/{work_id}/set-current')
def api_admin_set_current_project(
    work_id: str,
    admin: bool = Depends(require_admin),
    x_admin_operator: Optional[str] = Header(default=None, alias='X-Admin-Operator'),
) -> Dict[str, Any]:
    _ = admin
    actor = resolve_admin_actor(x_admin_operator)
    try:
        item = store.admin_set_current_project(work_id=work_id.strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.log_admin_action(
        actor=actor,
        action_type='project_set_current',
        target_type='project',
        target_id=str(item.get('work_id') or ''),
        related_user_id=0,
        detail={'name': item.get('name', '')},
    )
    return {'item': item}


@app.put('/api/admin/work/current')
def api_admin_update_work(payload: WorkUpdateRequest, admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    try:
        updated = store.update_current_work(payload=payload.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {'work': updated}


@app.get('/api/admin/part-adapter/sources')
def api_admin_part_adapter_sources(admin: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return {'items': part_adapter_store.get_sources()}


@app.get('/api/admin/part-adapter/rules')
def api_admin_part_adapter_rules(admin: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return part_adapter_store.get_rules_summary()


@app.put('/api/admin/part-adapter/rules')
def api_admin_part_adapter_update_rules(
    payload: PartAdapterRulesUpdateRequest,
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    rules = part_adapter_store.update_rules(
        gobricks_sync_meta=payload.gobricks_sync_meta,
        gobricks_item_index=payload.gobricks_item_index,
        gobricks_category_index=payload.gobricks_category_index,
        exact_combo_map=payload.exact_combo_map,
        shortage_combo_map=payload.shortage_combo_map,
        lego_color_catalog=payload.lego_color_catalog,
        exact_part_map=payload.exact_part_map,
        part_alias_map=payload.part_alias_map,
        color_rules=payload.color_rules,
        substitutions=payload.substitutions,
        part_meta=payload.part_meta,
    )
    return {'ok': True, 'updated_at': rules.get('updated_at', ''), 'rules': rules}


@app.post('/api/admin/part-adapter/import-bom')
async def api_admin_part_adapter_import_bom(
    file: UploadFile = File(...),
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    content = await file.read()
    try:
        parsed = part_adapter_store.import_bom_file(filename=file.filename or '', content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    part_adapter_store.record_event('import_bom', route='/api/admin/part-adapter/import-bom', source_name=file.filename or '')
    return parsed


@app.post('/api/admin/part-adapter/import-gobricks-result')
async def api_admin_part_adapter_import_gobricks_result(
    file: UploadFile = File(...),
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    content = await file.read()
    try:
        parsed = part_adapter_store.import_gobricks_result_file(filename=file.filename or '', content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return parsed


@app.post('/api/admin/part-adapter/import-gobricks-catalog')
async def api_admin_part_adapter_import_gobricks_catalog(
    file: UploadFile = File(...),
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    content = await file.read()
    try:
        parsed = part_adapter_store.import_gobricks_catalog_file(filename=file.filename or '', content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return parsed


@app.post('/api/admin/part-adapter/import-gobricks-categories')
async def api_admin_part_adapter_import_gobricks_categories(
    file: UploadFile = File(...),
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    content = await file.read()
    try:
        parsed = part_adapter_store.import_gobricks_category_file(filename=file.filename or '', content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return parsed


@app.post('/api/admin/part-adapter/import-rebrickable-relationships')
async def api_admin_part_adapter_import_rebrickable_relationships(
    file: UploadFile = File(...),
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    content = await file.read()
    try:
        return part_adapter_store.import_rebrickable_relationships_file(filename=file.filename or '', content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post('/api/admin/part-adapter/import-rebrickable-parts')
async def api_admin_part_adapter_import_rebrickable_parts(
    file: UploadFile = File(...),
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    content = await file.read()
    try:
        return part_adapter_store.import_rebrickable_parts_file(filename=file.filename or '', content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post('/api/admin/part-adapter/import-rebrickable-part-categories')
async def api_admin_part_adapter_import_rebrickable_part_categories(
    file: UploadFile = File(...),
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    content = await file.read()
    try:
        return part_adapter_store.import_rebrickable_part_categories_file(filename=file.filename or '', content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post('/api/admin/part-adapter/import-rebrickable-colors')
async def api_admin_part_adapter_import_rebrickable_colors(
    file: UploadFile = File(...),
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    content = await file.read()
    try:
        return part_adapter_store.import_rebrickable_colors_file(filename=file.filename or '', content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post('/api/admin/part-adapter/import-rebrickable-elements')
async def api_admin_part_adapter_import_rebrickable_elements(
    file: UploadFile = File(...),
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    content = await file.read()
    try:
        return part_adapter_store.import_rebrickable_elements_file(filename=file.filename or '', content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post('/api/admin/part-adapter/sync-gobricks-items')
def api_admin_part_adapter_sync_gobricks_items(
    payload: PartAdapterGobricksSyncRequest,
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    auth_token = _resolve_gobricks_auth_token(payload.auth_token)
    if not auth_token:
        raise HTTPException(status_code=400, detail='请先提供高砖 auth_token，或在服务端配置 GOBRICKS_AUTH_TOKEN')
    base_url = str(payload.base_url or 'https://api.gobricks.cn').strip().rstrip('/') or 'https://api.gobricks.cn'
    request_payload = {
        'auth_token': auth_token,
        'start_time': str(payload.start_time or '').strip(),
        'end_time': str(payload.end_time or '').strip(),
        'need_detail_info': bool(payload.need_detail_info),
    }
    path = '/trade/external/v1/getItemsByModifyTime'
    try:
        req = UrlRequest(
            f'{base_url}{path}',
            data=json.dumps(request_payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        with urlopen(req, timeout=30) as resp:
            body_text = resp.read().decode('utf-8', errors='replace')
    except HTTPError as exc:
        detail_text = exc.read().decode('utf-8', errors='replace')
        try:
            body = json.loads(detail_text or '{}')
        except Exception:
            body = {'msg': detail_text or '高砖同步失败'}
        raise HTTPException(status_code=400, detail=body.get('msg') or body.get('message') or '高砖同步失败')
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f'高砖同步失败: {exc}')

    try:
        body = json.loads(body_text or '{}')
    except Exception:
        raise HTTPException(status_code=400, detail='高砖返回内容无法解析')
    if int(body.get('ret') or 0) != 1:
        raise HTTPException(status_code=400, detail=body.get('msg') or '高砖同步失败')

    data_items = body.get('data') if isinstance(body.get('data'), list) else []
    sync_ret = part_adapter_store.sync_gobricks_items(
        items=data_items,
        start_time=request_payload['start_time'],
        end_time=request_payload['end_time'],
        base_url=base_url,
        need_detail_info=request_payload['need_detail_info'],
    )
    return {
        'ok': True,
        'remote_count': len(data_items),
        'sync': sync_ret,
        'message': body.get('msg') or '成功',
    }


@app.post('/api/admin/part-adapter/convert-gobricks')
async def api_admin_part_adapter_convert_gobricks(
    auth_token: str = Form(default=''),
    base_url: str = Form(default='https://api.gobricks.cn'),
    file: UploadFile = File(...),
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    safe_token = _resolve_gobricks_auth_token(auth_token)
    if not safe_token:
        raise HTTPException(status_code=400, detail='请先提供高砖 auth_token，或在服务端配置 GOBRICKS_AUTH_TOKEN')
    safe_base_url = str(base_url or 'https://api.gobricks.cn').strip().rstrip('/') or 'https://api.gobricks.cn'
    file_bytes = await file.read()
    try:
        imported = part_adapter_store.import_bom_file(filename=file.filename or '', content=file_bytes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    multipart = build_multipart_form_data(
        fields={'auth_token': safe_token},
        file_field_name='file',
        filename=file.filename or 'upload.csv',
        file_content=file_bytes,
        content_type=file.content_type or 'text/csv',
    )
    path = '/trade/external/v1/convertToGdsItemList'
    try:
        req = UrlRequest(
            f'{safe_base_url}{path}',
            data=multipart['body'],
            headers={'Content-Type': multipart['content_type']},
            method='POST',
        )
        with urlopen(req, timeout=60) as resp:
            body_text = resp.read().decode('utf-8', errors='replace')
    except HTTPError as exc:
        detail_text = exc.read().decode('utf-8', errors='replace')
        try:
            body = json.loads(detail_text or '{}')
        except Exception:
            body = {'msg': detail_text or '高砖转换失败'}
        raise HTTPException(status_code=400, detail=body.get('msg') or body.get('message') or '高砖转换失败')
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f'高砖转换失败: {exc}')

    try:
        body = json.loads(body_text or '{}')
    except Exception:
        raise HTTPException(status_code=400, detail='高砖返回内容无法解析')
    if int(body.get('ret') or 0) != 1:
        raise HTTPException(status_code=400, detail=body.get('msg') or '高砖转换失败')

    remote_data = body.get('data') if isinstance(body.get('data'), dict) else {}
    report = part_adapter_store.process_gobricks_conversion_result(
        source_file=file.filename or '',
        bom_text=imported.get('bom_text', ''),
        remote_data=remote_data,
    )
    part_adapter_store.record_event('convert_gobricks', route='/api/admin/part-adapter/convert-gobricks', source_name=file.filename or '')
    return {
        'ok': True,
        'message': body.get('msg') or '成功',
        'bom': imported,
        'report': report,
    }


@app.post('/api/admin/part-adapter/analyze')
def api_admin_part_adapter_analyze(
    payload: PartAdapterAnalyzeRequest,
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    bom_text = str(payload.bom_text or '').strip()
    if not bom_text:
        raise HTTPException(status_code=400, detail='请先提供 BOM 文本')
    color_mode = str(payload.color_mode or 'balanced').strip().lower()
    if color_mode not in {'safe', 'balanced', 'aggressive'}:
        color_mode = 'balanced'
    job = part_adapter_store.analyze(
        project_name=payload.project_name,
        designer_name=payload.designer_name,
        source_name=payload.source_name,
        bom_text=bom_text,
        color_mode=color_mode,
        allow_display_sub=payload.allow_display_sub,
        allow_structural_sub=payload.allow_structural_sub,
    )
    part_adapter_store.record_event('analyze', route='/api/admin/part-adapter/analyze', source_name=payload.source_name or payload.project_name)
    return {'job': job}


@app.get('/api/admin/part-adapter/analytics')
def api_admin_part_adapter_analytics(admin: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return {'summary': part_adapter_store.get_analytics_summary()}


@app.post('/api/admin/part-adapter/analytics/event')
def api_admin_part_adapter_analytics_event(
    payload: PartAdapterAnalyticsEventRequest,
    request: Request,
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    event_type = str(payload.event_type or '').strip()
    if not event_type:
        raise HTTPException(status_code=400, detail='缺少事件类型')
    part_adapter_store.record_event(
        event_type=event_type,
        route=str(request.url.path or ''),
        source_name=payload.source_name,
        visitor_key=_part_adapter_visitor_hash(request),
    )
    return {'ok': True}


@app.get('/api/admin/part-adapter/jobs')
def api_admin_part_adapter_jobs(limit: int = 20, admin: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    return part_adapter_store.list_jobs(limit=limit)


@app.get('/api/admin/part-adapter/jobs/{job_id}')
def api_admin_part_adapter_job_detail(job_id: str, admin: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin
    job = part_adapter_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail='适配记录不存在')
    return {'job': job}


@app.put('/api/admin/part-adapter/jobs/{job_id}/review')
def api_admin_part_adapter_update_review(
    job_id: str,
    payload: PartAdapterReviewUpdateRequest,
    admin: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin
    job = part_adapter_store.update_review_status(
        job_id=job_id,
        line_no=payload.line_no,
        review_status=payload.review_status,
    )
    if not job:
        raise HTTPException(status_code=404, detail='适配记录不存在')
    return {'job': job}


@app.get('/api/admin/part-adapter/jobs/{job_id}/export.csv')
def api_admin_part_adapter_export(job_id: str, admin: Dict[str, Any] = Depends(require_admin)) -> Response:
    _ = admin
    csv_text = part_adapter_store.export_job_csv(job_id)
    if csv_text is None:
        raise HTTPException(status_code=404, detail='适配记录不存在')
    part_adapter_store.record_event('export_csv', route=f'/api/admin/part-adapter/jobs/{job_id}/export.csv', source_name=job_id)
    return Response(
        content=csv_text,
        media_type='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename=\"{job_id}.csv\"'},
    )


@app.get('/admin', response_class=HTMLResponse)
def admin_page() -> Any:
    overview_file = ADMIN_DIR / 'overview.html'
    if overview_file.exists():
        return FileResponse(overview_file)
    index_file = ADMIN_DIR / 'index.html'
    if index_file.exists():
        return FileResponse(index_file)
    raise HTTPException(status_code=404, detail='后台页面不存在')


@app.get('/admin-assets/{asset_name}')
def admin_assets(asset_name: str) -> Any:
    safe_name = os.path.basename(asset_name or '')
    if not safe_name or safe_name != asset_name:
        raise HTTPException(status_code=404, detail='资源不存在')
    if safe_name not in {'admin.css', 'admin.js'}:
        raise HTTPException(status_code=404, detail='资源不存在')
    file_path = ADMIN_DIR / safe_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail='后台页面不存在')
    media_type = 'text/css; charset=utf-8' if safe_name.endswith('.css') else 'application/javascript; charset=utf-8'
    return FileResponse(file_path, media_type=media_type)


@app.get('/admin/{module}', response_class=HTMLResponse)
def admin_module_page(module: str, request: Request) -> Any:
    safe_module = (module or '').strip().lower()
    if safe_module not in {
        'overview',
        'projects',
        'orders',
        'users',
        'submissions',
        'designers',
        'feedback',
        'logs',
        'settings',
        'part-adapter'
    }:
        raise HTTPException(status_code=404, detail='后台模块不存在')
    module_file = ADMIN_DIR / f'{safe_module}.html'
    if module_file.exists():
        if safe_module == 'part-adapter':
            part_adapter_store.record_event(
                'page_view_admin',
                route='/admin/part-adapter',
                visitor_key=_part_adapter_visitor_hash(request),
            )
        return FileResponse(module_file)
    index_file = ADMIN_DIR / 'index.html'
    if not index_file.exists():
        raise HTTPException(status_code=404, detail='后台页面不存在')
    return FileResponse(index_file)


@app.get('/tools/part-adapter', response_class=HTMLResponse)
def public_part_adapter_page(request: Request) -> Any:
    module_file = ADMIN_DIR / 'part-adapter.html'
    if not module_file.exists():
        raise HTTPException(status_code=404, detail='工具页面不存在')
    part_adapter_store.record_event(
        'page_view_public',
        route='/tools/part-adapter',
        visitor_key=_part_adapter_visitor_hash(request),
    )
    response = FileResponse(module_file)
    response.set_cookie(
        key=PART_ADAPTER_PUBLIC_COOKIE,
        value=get_admin_token(),
        httponly=True,
        samesite='lax',
        secure=False,
        path='/',
    )
    return response


@app.get('/')
def root() -> Dict[str, str]:
    return {'name': 'KWC Designer Plan API', 'health': '/health', 'admin': '/admin/overview'}
