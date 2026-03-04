# 酷玩潮 × 高砖 原创设计师计划小程序（可联调版）

当前版本能力：

1. 交易链路：支持 `预售` 与 `众筹` 两种模式，预约 -> 下单 -> 支付确认（默认 `mock` 支付）。
2. 设计师链路：开通入口 -> 查看销售与分成 -> 发布作品动态。
3. 运营后台：看板、订单、投稿审核、设计师分成绑定、分成结算与CSV导出。

## 本次新增

1. 面向用户展示“作品动态流”：作品详情页可看设计师发布的维护/进度消息。
2. 设计师仅可发布并查看“自己绑定作品”的动态。
3. 分成结算状态：`未结算 / 已结算`。
4. 后台分成报表导出：`/api/admin/commissions/export.csv`。
5. 作品发布配置：后台可切换 `预售/众筹` 并配置众筹目标与截止时间。
6. 众筹状态机：`进行中 -> 达标生产中`，或 `进行中 -> 失败退款中（自动退款）`。
7. 微信退款接入：支持“众筹失败后待退款订单提交到微信 + 微信退款回调落库”。
8. 后台订单中心增强：支持多条件筛选（关键词/模式/支付/退款/订单状态）与经营汇总。
9. 个人中心增强：新增订单总览数据（支付/退款/净额）与订单状态筛选视图。
10. 个人中心订单增强：支持时间筛选与分页“加载更多”。
11. 后台新增用户中心：支持按 openid/昵称检索用户并查看订单/交易/投稿/预约汇总。
12. 后台订单导出增强：可按当前筛选条件导出订单 CSV。
13. 后台用户详情页：支持查看单用户的订单时间线、投稿记录、预约记录与设计师分成信息。
14. 后台订单操作增强：支持订单运营备注与众筹订单手动退款重试。
15. 后台用户流水导出：支持按用户导出该用户的全部订单流水 CSV。
16. 用户详情新增操作日志：展示“订单备注 / 退款重试 / 分成结算变更”等后台操作轨迹（含操作人和时间）。
17. 后台操作日志中心：支持按操作人/动作类型/目标类型/目标ID/关联用户/时间范围筛选，并支持审计 CSV 导出。
18. 后台操作日志支持分页浏览，且可从“关联用户ID”一键跳转到该用户详情。
19. 后台操作日志分页新增“页码直跳”，支持输入页码回车快速跳转并展示总页数。
20. 后台操作日志分页新增“首页/末页”，并记忆筛选条件与每页条数（刷新后自动恢复）。
21. 后台操作日志支持按“时间/操作人/动作类型”排序（升降序切换），并增加空结果态提示。
22. 后台操作日志排序支持 URL 参数同步（`action_sort_by/action_sort_order`），便于分享同一视图。
23. 后台重构为“模块化导航”视图：总览、订单、用户、投稿、设计师分成、反馈中心、操作日志分区显示，减少单页堆叠混乱。
24. 小程序新增账号中心：支持微信登录、首次注册引导（完善昵称）、资料更新。
25. 小程序新增帮助中心、反馈中心、设置中心；后台新增反馈工单管理（状态流转与官方回复）。
26. 反馈中心增强：支持优先级（低/普通/高/紧急）与截图附件上传（用户端）并在后台展示。
27. 后台反馈支持回复模板管理（模板编码/标题/内容/启停）与一键套用。
28. 后台支持角色权限头 `X-Admin-Role`（`superadmin/operator/finance/reviewer`）并按模块限权。
29. 后台支持路由级模块地址：`/admin/overview`、`/admin/projects`、`/admin/orders`、`/admin/users`、`/admin/submissions`、`/admin/designers`、`/admin/feedback`、`/admin/logs`。
30. 后台模块拆分为独立页面入口文件（每个模块单独 URL 页面壳），支持分模块导航与独立访问。
31. 后台反馈中心新增附件缩略图放大预览与筛选条件 CSV 导出。
32. 后台新增项目管理：可创建/编辑项目，并在“预售/众筹”项目间切换当前在售项目。
33. 项目支持图片字段：主图 `cover_image` + 图集 `gallery_images`，首页/作品详情/下单页自动展示后台配置图片。
34. 项目管理支持后台直接上传图片文件（主图/图集），上传后自动回填 `cover_image/gallery_images`。
35. 投稿审核支持“审核通过后一键开通设计师”，避免手工二次录入。
36. 项目管理支持“创建/更新项目时绑定设计师”（openid + 分成比例）。
37. 设计师前台新增“项目信息维护”能力，可维护已绑定项目的名称/副标题/故事/主图/图集/亮点。
38. 作品详情新增玩家评论，设计师可在设计师中心回复评论。
39. 项目管理支持编辑“参数信息（specs）”与“版本与权益（sku_list）”，并在后台项目页查看项目预约情况。

## 目录结构

- `miniprogram/` 微信小程序前端
- `backend/` FastAPI 后端（SQLite 存储）
- `backend/admin/index.html` 后台兼容跳转页（自动跳转到新模块路由）
- `backend/admin/*.html` 后台模块独立入口页（overview/orders/users/submissions/designers/feedback/logs）
- `backend/admin/admin.css` / `backend/admin/admin.js` 后台公共样式与脚本

## 本地启动

### 1) 启动后端

```bash
cd /Users/jackyyu/Documents/酷玩潮/designer-plan-miniapp/backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8002 --reload
```

### 2) 打开小程序

- 微信开发者工具导入：`/Users/jackyyu/Documents/酷玩潮/designer-plan-miniapp/miniprogram`
- `project.private.config.json` 已关闭合法域名校验（本地联调用）。
- 默认接口地址：`miniprogram/config/env.js` 中 `http://127.0.0.1:8002`

### 3) 打开运营后台

- 浏览器访问：`http://127.0.0.1:8002/admin`
- 模块独立地址：
  - `http://127.0.0.1:8002/admin/overview`
  - `http://127.0.0.1:8002/admin/projects`
  - `http://127.0.0.1:8002/admin/orders`
  - `http://127.0.0.1:8002/admin/users`
  - `http://127.0.0.1:8002/admin/submissions`
  - `http://127.0.0.1:8002/admin/designers`
  - `http://127.0.0.1:8002/admin/feedback`
  - `http://127.0.0.1:8002/admin/logs`
- 默认后台令牌：`kwc-admin-dev`（可在环境变量 `ADMIN_TOKEN` 修改）
- 后台页面可填写“操作人”，会通过 `X-Admin-Operator` 写入操作日志
- 后台可切换角色（请求头 `X-Admin-Role`）：`superadmin/operator/finance/reviewer`

## 设计师分成逻辑

- 默认分成比例：`15%`
- 统计口径：`已支付订单销售额（total_amount）`
- 预计分成：`销售额 × 分成比例`
- 结算状态：后台可标记为 `未结算/已结算`

## 众筹状态机

- 状态：`active`（进行中）/`producing`（达标生产中）/`failed`（失败退款中）
- 自动达标转生产：已筹金额 >= 目标金额时自动切到 `producing`
- 自动失败退款：到达截止时间且未达目标时，已支付众筹订单自动标记退款
- 退款后的众筹订单状态：`crowdfunding_refunded`

## 真实微信能力接入说明

### 微信登录

在 `backend/.env.example` 中配置：

- `WECHAT_APPID`
- `WECHAT_APP_SECRET`

配置后，`/api/auth/login` 会走真实 `code2session`。

### 微信支付

当前 `PAY_MODE=mock`，用于完整联调下单流程。

切到真实支付需补充：

1. 微信商户参数（商户号、证书、APIv3密钥等）
2. 支付下单网关实现（`/api/orders/preorder` 返回可直接 `wx.requestPayment` 的参数）
3. 支付回调验签与异步记账

### 微信退款（已接入）

当众筹失败时：

1. 系统会把众筹已支付订单标记为 `crowdfunding_refunding` + `refund_status=pending_submit`。
2. 后台调用 `POST /api/admin/refunds/crowdfunding/initiate` 后，系统会向微信提交退款申请。
3. 微信回调 `POST /api/payments/wechat/refund/notify` 后，订单自动更新为：
   - 成功：`crowdfunding_refunded`
   - 处理中：`crowdfunding_refunding`
   - 异常：`crowdfunding_refund_failed`

需要配置以下环境变量（见 `backend/.env.example`）：

- `PAY_MODE=wechat`
- `WECHAT_PAY_MCHID`
- `WECHAT_PAY_SERIAL_NO`
- `WECHAT_PAY_PRIVATE_KEY_PATH` 或 `WECHAT_PAY_PRIVATE_KEY_PEM`
- `WECHAT_PAY_API_V3_KEY`
- `WECHAT_PAY_REFUND_NOTIFY_URL`
- 平台公钥三选一：
  - `WECHAT_PAY_PLATFORM_PUBLIC_KEY_PATH`
  - `WECHAT_PAY_PLATFORM_PUBLIC_KEY_PEM`
  - `WECHAT_PAY_PLATFORM_CERT_PATH`

## 已实现接口（核心）

- `POST /api/auth/login`
- `GET /api/me/profile`
- `PUT /api/me/profile`
- `GET /api/me/feedback`
- `POST /api/me/feedback`
- `GET /api/work/current`
- `GET /api/work/{work_id}/updates`
- `GET /api/work/{work_id}/comments`
- `POST /api/work/{work_id}/comments`
- `POST /api/reservations`
- `GET /api/me/summary`
- `GET /api/me/orders`
- `POST /api/orders/preorder`
- `POST /api/payments/confirm`
- `POST /api/payments/wechat/refund/notify`
- `POST /api/submissions`
- `POST /api/uploads/image`
- `POST /api/admin/uploads/image`
- `POST /api/designer/enroll`
- `GET /api/designer/me/dashboard`
- `GET /api/designer/me/orders`
- `GET /api/designer/me/updates`
- `POST /api/designer/me/updates`
- `GET /api/designer/me/projects`
- `PUT /api/designer/me/projects/{work_id}`
- `GET /api/designer/me/comments`
- `POST /api/designer/me/comments/{comment_id}/reply`
- `GET /api/admin/dashboard`
- `GET /api/admin/projects`
- `POST /api/admin/projects`
- `PUT /api/admin/projects/{work_id}`
- `POST /api/admin/projects/{work_id}/set-current`
- `GET /api/admin/reservations`
- `GET /api/admin/orders`
- `GET /api/admin/orders/export.csv`
- `POST /api/admin/orders/{order_id}/note`
- `POST /api/admin/orders/{order_id}/retry-refund`
- `GET /api/admin/users`
- `GET /api/admin/feedback`
- `GET /api/admin/feedback/export.csv`
- `POST /api/admin/feedback/{feedback_id}/reply`
- `GET /api/admin/feedback/templates`
- `POST /api/admin/feedback/templates/upsert`
- `GET /api/admin/users/{user_id}/detail`
- `GET /api/admin/users/{user_id}/orders/export.csv`
- `GET /api/admin/action-logs`
- `GET /api/admin/action-logs/export.csv`
- `POST /api/admin/refunds/crowdfunding/initiate`
- `GET /api/admin/submissions`
- `POST /api/admin/submissions/{submission_id}/review`
- `POST /api/admin/submissions/{submission_id}/activate-designer`
- `GET /api/admin/designers`
- `POST /api/admin/designers/assign`
- `GET /api/admin/commissions`
- `POST /api/admin/commissions/{record_id}/settle`
- `POST /api/admin/commissions/batch-settle`
- `GET /api/admin/commissions/export.csv`
