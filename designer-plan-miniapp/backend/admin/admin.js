      const adminPageRoot = document.getElementById('adminPageRoot') || document.body
      const singleModuleMode = String(adminPageRoot.dataset.singleModule || '') === '1'
      const presetModuleFromDom = String(adminPageRoot.dataset.adminModule || '').trim()
      const selectedCommissionIds = new Set()
      let currentCommissionItems = []
      let currentUserDetailId = 0
      let actionLogOffset = 0
      let actionLogLimit = 100
      let actionLogTotal = 0
      let actionLogSortBy = 'created_at'
      let actionLogSortOrder = 'desc'
      let currentAdminModule = 'overview'
      let feedbackTemplates = []
      let currentProjectEditId = ''
      let adminRoleItemsCache = []
      let adminUserItemsCache = []
      let projectDesignerOptions = []
      let currentAdminIdentity = null
      let currentAdminPermissions = []
      const query = new URLSearchParams(window.location.search || '')
      const ACTION_LOG_PREFS_KEY = 'kwc_admin_action_log_prefs_v1'
      const ACTION_LOG_SORT_BY_QS = 'action_sort_by'
      const ACTION_LOG_SORT_ORDER_QS = 'action_sort_order'
      const ADMIN_SESSION_STORAGE_KEY = 'kwc_admin_session_token_v1'
      const ADMIN_MODULE_ROUTES = {
        overview: '/admin/overview',
        project: '/admin/projects',
        order: '/admin/orders',
        user: '/admin/users',
        submission: '/admin/submissions',
        designer: '/admin/designers',
        feedback: '/admin/feedback',
        log: '/admin/logs',
        setting: '/admin/settings'
      }
      const MODULE_PERMISSION_MAP = {
        overview: 'overview',
        project: 'project',
        order: 'order',
        user: 'user',
        submission: 'submission',
        designer: 'designer',
        feedback: 'feedback',
        log: 'log',
        setting: 'setting'
      }
      const ADMIN_PERMISSION_META = [
        { key: 'overview', label: '总览' },
        { key: 'project', label: '项目管理' },
        { key: 'order', label: '订单管理' },
        { key: 'user', label: '用户管理' },
        { key: 'submission', label: '投稿审核' },
        { key: 'designer', label: '设计师分成' },
        { key: 'feedback', label: '反馈中心' },
        { key: 'log', label: '操作日志' },
        { key: 'setting', label: '设置中心' }
      ]

      function normalizeAdminModule(raw) {
        const value = String(raw || '').trim().toLowerCase()
        const aliasMap = {
          overview: 'overview',
          project: 'project',
          projects: 'project',
          order: 'order',
          orders: 'order',
          user: 'user',
          users: 'user',
          submission: 'submission',
          submissions: 'submission',
          designer: 'designer',
          designers: 'designer',
          feedback: 'feedback',
          log: 'log',
          logs: 'log',
          setting: 'setting',
          settings: 'setting'
        }
        return aliasMap[value] || 'overview'
      }

      function getStoredAdminSession() {
        try {
          return String(localStorage.getItem(ADMIN_SESSION_STORAGE_KEY) || '').trim()
        } catch (err) {
          return ''
        }
      }

      function setStoredAdminSession(token) {
        const safe = String(token || '').trim()
        try {
          if (safe) {
            localStorage.setItem(ADMIN_SESSION_STORAGE_KEY, safe)
          } else {
            localStorage.removeItem(ADMIN_SESSION_STORAGE_KEY)
          }
        } catch (err) {
          // ignore storage error
        }
      }

      function getCfg() {
        const baseUrlEl = document.getElementById('baseUrl')
        const adminTokenEl = document.getElementById('adminToken')
        const adminRoleEl = document.getElementById('adminRole')
        const operatorEl = document.getElementById('adminOperator')
        const baseUrl = String((baseUrlEl && baseUrlEl.value) || 'http://127.0.0.1:8002')
          .trim()
          .replace(/\/$/, '')
        const adminRole =
          String((adminRoleEl && adminRoleEl.value) || '') ||
          String((currentAdminIdentity && currentAdminIdentity.role_key) || 'superadmin')
        const operatorFromIdentity = String(
          (currentAdminIdentity && (currentAdminIdentity.display_name || currentAdminIdentity.username)) || ''
        ).trim()
        const adminOperatorRaw = String((operatorEl && operatorEl.value) || operatorFromIdentity || 'admin').trim() || 'admin'
        return {
          baseUrl,
          adminToken: String((adminTokenEl && adminTokenEl.value) || '').trim(),
          adminRole: adminRole.trim() || 'superadmin',
          adminSession: getStoredAdminSession(),
          adminOperator: adminOperatorRaw,
          adminOperatorHeader: encodeURIComponent(adminOperatorRaw)
        }
      }

      function buildAdminHeaders(extraHeaders, withJson) {
        const cfg = getCfg()
        const headers = Object.assign({}, extraHeaders || {})
        if (cfg.adminSession) {
          headers['X-Admin-Session'] = cfg.adminSession
        } else if (cfg.adminToken) {
          headers['X-Admin-Token'] = cfg.adminToken
          headers['X-Admin-Role'] = cfg.adminRole
        }
        if (cfg.adminOperatorHeader) {
          headers['X-Admin-Operator'] = cfg.adminOperatorHeader
        }
        if (withJson) {
          headers['Content-Type'] = 'application/json'
        }
        return headers
      }

      function parseResponseBodyText(text) {
        const raw = String(text || '').trim()
        if (!raw) {
          return {}
        }
        try {
          return JSON.parse(raw)
        } catch (err) {
          return { detail: raw }
        }
      }

      async function readResponseBody(res) {
        const text = await res.text()
        return parseResponseBodyText(text)
      }

      function clearAdminSessionIdentity(silent) {
        setStoredAdminSession('')
        currentAdminIdentity = null
        currentAdminPermissions = []
        syncAdminIdentityToDom()
        if (!silent) {
          setStatus('登录态失效，请重新登录')
        }
      }

      function normalizePermissionList(source) {
        const list = Array.isArray(source) ? source : []
        const result = []
        const seen = new Set()
        list.forEach((item) => {
          const key = String(item || '').trim().toLowerCase()
          if (!key || seen.has(key)) {
            return
          }
          seen.add(key)
          result.push(key)
        })
        return result
      }

      async function apiGet(path) {
        const cfg = getCfg()
        const res = await fetch(cfg.baseUrl + path, {
          headers: buildAdminHeaders()
        })
        const body = await readResponseBody(res)
        if (!res.ok) {
          if (res.status === 401 && getStoredAdminSession()) {
            clearAdminSessionIdentity(false)
          }
          throw new Error(body.detail || '请求失败')
        }
        return body
      }

      async function apiPost(path, payload) {
        const cfg = getCfg()
        const res = await fetch(cfg.baseUrl + path, {
          method: 'POST',
          headers: buildAdminHeaders({}, true),
          body: JSON.stringify(payload || {})
        })
        const body = await readResponseBody(res)
        if (!res.ok) {
          if (res.status === 401 && getStoredAdminSession()) {
            clearAdminSessionIdentity(false)
          }
          throw new Error(body.detail || '请求失败')
        }
        return body
      }

      async function apiPut(path, payload) {
        const cfg = getCfg()
        const res = await fetch(cfg.baseUrl + path, {
          method: 'PUT',
          headers: buildAdminHeaders({}, true),
          body: JSON.stringify(payload || {})
        })
        const body = await readResponseBody(res)
        if (!res.ok) {
          if (res.status === 401 && getStoredAdminSession()) {
            clearAdminSessionIdentity(false)
          }
          throw new Error(body.detail || '请求失败')
        }
        return body
      }

      function toAbsoluteMediaUrl(rawUrl) {
        const value = String(rawUrl || '').trim()
        if (!value) {
          return ''
        }
        if (/^https?:\/\//i.test(value)) {
          return value
        }
        const cfg = getCfg()
        if (value.startsWith('/')) {
          return `${cfg.baseUrl}${value}`
        }
        return `${cfg.baseUrl}/${value}`
      }

      async function apiUploadAdminImage(file, scene = 'project') {
        const cfg = getCfg()
        const form = new FormData()
        form.append('file', file)
        const safeScene = encodeURIComponent(String(scene || 'project').trim() || 'project')
        const res = await fetch(`${cfg.baseUrl}/api/admin/uploads/image?scene=${safeScene}`, {
          method: 'POST',
          headers: buildAdminHeaders(),
          body: form
        })
        const body = await readResponseBody(res)
        if (!res.ok) {
          if (res.status === 401 && getStoredAdminSession()) {
            clearAdminSessionIdentity(false)
          }
          throw new Error(body.detail || '上传失败')
        }
        return body
      }

      function setStatus(msg) {
        document.getElementById('status').innerText = msg || ''
      }

      function applyAdminModule() {
        const cards = document.querySelectorAll('.module-card')
        cards.forEach((card) => {
          const modules = String(card.dataset.modules || '')
            .split(/\s+/)
            .map((x) => x.trim())
            .filter(Boolean)
          const visible = modules.includes(currentAdminModule)
          card.style.display = visible ? '' : 'none'
        })
        const buttons = document.querySelectorAll('[data-module-btn]')
        buttons.forEach((btn) => {
          btn.classList.toggle('active', btn.dataset.moduleBtn === currentAdminModule)
        })
      }

      function inferAdminModuleFromPath() {
        const byDom = normalizeAdminModule(presetModuleFromDom)
        if (byDom !== 'overview' || presetModuleFromDom) {
          return byDom
        }
        const byQuery = normalizeAdminModule(query.get('module'))
        if (byQuery !== 'overview' || String(query.get('module') || '').trim()) {
          return byQuery
        }
        const path = String(window.location.pathname || '').toLowerCase()
        const mappings = [
          ['project', '/admin/projects'],
          ['order', '/admin/orders'],
          ['user', '/admin/users'],
          ['submission', '/admin/submissions'],
          ['designer', '/admin/designers'],
          ['feedback', '/admin/feedback'],
          ['log', '/admin/logs'],
          ['setting', '/admin/settings'],
          ['overview', '/admin/overview'],
          ['overview', '/admin']
        ]
        for (let i = 0; i < mappings.length; i += 1) {
          const item = mappings[i]
          if (path === item[1]) {
            return item[0]
          }
        }
        return 'overview'
      }

      function syncAdminModuleRoute(push = false) {
        if (singleModuleMode) {
          return
        }
        const route = ADMIN_MODULE_ROUTES[currentAdminModule] || ADMIN_MODULE_ROUTES.overview
        const current = String(window.location.pathname || '')
        if (current === route) {
          return
        }
        const fn = push ? 'pushState' : 'replaceState'
        window.history[fn](null, '', route)
      }

      function setAdminModule(moduleName, pushHistory = true) {
        const safe = String(moduleName || 'overview').trim() || 'overview'
        currentAdminModule = Object.prototype.hasOwnProperty.call(ADMIN_MODULE_ROUTES, safe) ? safe : 'overview'
        if (singleModuleMode) {
          const route = ADMIN_MODULE_ROUTES[currentAdminModule] || ADMIN_MODULE_ROUTES.overview
          if (String(window.location.pathname || '') !== route) {
            window.location.href = route
          }
          return
        }
        syncAdminModuleRoute(Boolean(pushHistory))
        applyAdminModule()
      }

      function allowedModulesByRole(role) {
        const x = String(role || '').toLowerCase()
        if (x === 'operator') {
          return ['overview', 'project', 'order', 'user', 'designer', 'feedback', 'log', 'setting']
        }
        if (x === 'finance') {
          return ['overview', 'designer', 'log']
        }
        if (x === 'reviewer') {
          return ['overview', 'submission', 'feedback', 'log']
        }
        return ['overview', 'project', 'order', 'user', 'submission', 'designer', 'feedback', 'log', 'setting']
      }

      function allowedModulesByPermissions(permissions) {
        const perms = normalizePermissionList(permissions)
        const modules = []
        Object.keys(MODULE_PERMISSION_MAP).forEach((moduleName) => {
          const permission = MODULE_PERMISSION_MAP[moduleName]
          if (perms.includes(permission)) {
            modules.push(moduleName)
          }
        })
        if (!modules.length && perms.includes('setting')) {
          modules.push('setting')
        }
        return modules
      }

      function resolveAllowedModules() {
        if (currentAdminPermissions && currentAdminPermissions.length) {
          return allowedModulesByPermissions(currentAdminPermissions)
        }
        const roleEl = document.getElementById('adminRole')
        const role = roleEl ? roleEl.value || 'superadmin' : 'superadmin'
        return allowedModulesByRole(role)
      }

      function applyRoleModuleVisibility() {
        const allowed = resolveAllowedModules()
        const buttons = document.querySelectorAll('[data-module-btn]')
        buttons.forEach((btn) => {
          const m = btn.dataset.moduleBtn
          btn.style.display = allowed.includes(m) ? '' : 'none'
        })
        if (!allowed.includes(currentAdminModule)) {
          if (singleModuleMode) {
            const fallback = allowed[0] || 'overview'
            const route = ADMIN_MODULE_ROUTES[fallback] || ADMIN_MODULE_ROUTES.overview
            if (String(window.location.pathname || '') !== route) {
              window.location.href = route
            }
            return
          }
          setAdminModule(allowed[0] || 'overview', false)
          return
        }
        applyAdminModule()
      }

      function handleAdminRoleChange() {
        if (currentAdminIdentity) {
          return
        }
        applyRoleModuleVisibility()
        loadAll()
      }

      function renderKpis(data) {
        const kpis = document.getElementById('kpis')
        if (!kpis) {
          return
        }
        const keys = [
          ['users', '用户数'],
          ['reservations', '预约数'],
          ['orders', '订单数'],
          ['paid_orders', '有效已支付单'],
          ['paid_amount', '有效成交额(元)'],
          ['refunded_amount', '退款总额(元)'],
          ['net_amount', '净成交额(元)'],
          ['refunding_orders', '退款处理中'],
          ['preorder_paid_orders', '预售已支付'],
          ['crowdfunding_paid_orders', '众筹已支付'],
          ['submissions', '投稿数'],
          ['pending_submissions', '待审核投稿'],
          ['designers', '设计师数'],
          ['designer_links', '分成绑定数'],
          ['feedback_total', '反馈总量'],
          ['feedback_pending', '反馈待处理'],
          ['feedback_processing', '反馈处理中'],
          ['feedback_resolved', '反馈已解决'],
          ['pending_commission_count', '未结算分成单'],
          ['settled_commission_count', '已结算分成单']
        ]
        kpis.innerHTML = keys
          .map(([key, label]) => `<div class="kpi"><div>${label}</div><div class="val">${data[key] || 0}</div></div>`)
          .join('')
      }

      function renderOrders(items, summary) {
        const sum = summary || {}
        document.getElementById('orderSummary').innerText =
          `共 ${sum.total_orders || 0} 单｜待支付 ${sum.pending_orders || 0}｜已支付 ${sum.paid_orders || 0}｜退款中 ${sum.refunding_orders || 0}｜已退款 ${sum.refunded_orders || 0}｜成交￥${sum.paid_amount || 0}｜退款￥${sum.refunded_amount || 0}｜净额￥${sum.net_amount || 0}`
        const rows = document.getElementById('orderRows')
        rows.innerHTML = (items || [])
          .map(
            (it) => `<tr>
              <td>${it.order_id}</td>
              <td>${it.user_nickname || '-'}<br /><span class="muted">${it.user_openid || '-'}</span></td>
              <td>${it.sale_mode === 'crowdfunding' ? '众筹' : '预售'}</td>
              <td>${it.work_name}</td>
              <td>${it.sku_name}</td>
              <td>${it.quantity}</td>
              <td>应付￥${it.total_amount}<br/>实付￥${it.paid_amount}<br/>退款￥${it.refund_amount || 0}</td>
              <td>${it.pay_status}</td>
              <td>${it.order_status_text}</td>
              <td>${it.refund_status || '-'}</td>
              <td>${it.admin_note || '-'}</td>
              <td>${it.payment_channel || '-'}</td>
              <td>下单：${it.created_at}<br/>支付：${it.paid_at || '-'}</td>
              <td>
                <button class="btn-small" onclick="editOrderNote('${it.order_id}', '${encodeURIComponent(it.admin_note || '')}')">备注</button>
                ${
                  it.sale_mode === 'crowdfunding' && it.pay_status === 'paid' && (it.refund_status === 'failed' || it.refund_status === 'pending_submit')
                    ? `<button class="btn-small" onclick="retryOrderRefund('${it.order_id}')">重试退款</button>`
                    : ''
                }
              </td>
            </tr>`
          )
          .join('')
      }

      async function editOrderNote(orderId, encodedNote) {
        const current = decodeURIComponent(encodedNote || '')
        const next = prompt('请输入运营备注（最多500字）', current || '')
        if (next == null) {
          return
        }
        try {
          await apiPost(`/api/admin/orders/${encodeURIComponent(orderId)}/note`, { note: next })
          await loadOrders()
          setStatus(`订单 ${orderId} 备注已更新`)
        } catch (err) {
          alert(err.message || '备注更新失败')
        }
      }

      async function retryOrderRefund(orderId) {
        const reason = prompt('请输入退款重试原因（可选）', '众筹订单退款重试') || '众筹订单退款重试'
        if (!confirm(`确认重试退款订单 ${orderId} 吗？`)) {
          return
        }
        try {
          await apiPost(`/api/admin/orders/${encodeURIComponent(orderId)}/retry-refund`, {
            reason
          })
          await Promise.all([loadOrders(), loadDashboard()])
          setStatus(`订单 ${orderId} 已提交退款重试`)
        } catch (err) {
          alert(err.message || '退款重试失败')
        }
      }

      function renderReservations(items) {
        const rows = document.getElementById('reservationRows')
        rows.innerHTML = (items || [])
          .map(
            (it) => `<tr>
              <td>${it.openid}</td>
              <td>${it.nickname || '-'}</td>
              <td>${it.work_id}</td>
              <td>${it.created_at}</td>
            </tr>`
          )
          .join('')
      }

      function renderSubmissions(items) {
        const rows = document.getElementById('submissionRows')
        rows.innerHTML = (items || [])
          .map((it) => {
            const actionButtons = []
            if (it.status !== 'approved') {
              actionButtons.push(`<button class="btn-small" onclick="reviewSubmission('${it.submission_id}','approved')">通过</button>`)
            }
            if (it.status !== 'rejected') {
              actionButtons.push(`<button class="btn-small" onclick="reviewSubmission('${it.submission_id}','rejected')">驳回</button>`)
            }
            if (it.status === 'approved') {
              actionButtons.push(
                `<button class="btn-small" onclick="activateDesignerFromSubmission('${it.submission_id}')">开通设计师</button>`
              )
            }
            return `<tr>
              <td>${it.submission_id}</td>
              <td>${it.work_name}</td>
              <td>${it.category}</td>
              <td>${it.estimated_pieces}</td>
              <td>${it.status_text}</td>
              <td>${it.created_at}</td>
              <td>${actionButtons.join('')}</td>
            </tr>`
          })
          .join('')
      }

      function renderDesigners(items) {
        const rows = document.getElementById('designerRows')
        rows.innerHTML = (items || [])
          .map((it) => {
            const tags = (it.assignments || [])
              .map((a) => `<span class="tag">${a.work_name} · ${a.share_percent}%</span>`)
              .join('')
            const rawAvatar = String(it.avatar_url || it.avatarUrl || '').trim()
            const avatarUrl = rawAvatar ? toAbsoluteMediaUrl(rawAvatar) : ''
            const avatarHtml = avatarUrl
              ? `<a href="${avatarUrl}" target="_blank" rel="noopener noreferrer"><img class="designer-avatar-thumb" src="${avatarUrl}" alt="头像" /></a>`
              : '<span class="muted">未设置</span>'
            return `<tr>
              <td>${it.display_name || '-'}</td>
              <td>${avatarHtml}</td>
              <td>${it.openid}</td>
              <td>${it.default_share_percent}%</td>
              <td>${tags || '-'}</td>
              <td>${it.status_text}</td>
            </tr>`
          })
          .join('')
      }

      function renderUsers(items, summary) {
        const sum = summary || {}
        document.getElementById('userSummary').innerText =
          `共 ${sum.users || 0} 用户｜设计师 ${sum.designers || 0}｜总订单 ${sum.total_orders || 0}｜已支付 ${sum.paid_orders || 0}｜退款中 ${sum.refunding_orders || 0}｜已退款 ${sum.refunded_orders || 0}｜成交￥${sum.paid_amount || 0}｜退款￥${sum.refund_amount || 0}｜净额￥${sum.net_amount || 0}`

        const rows = document.getElementById('userRows')
        rows.innerHTML = (items || [])
          .map(
            (it) => `<tr>
              <td>${it.nickname || '-'}<br /><span class="muted">${it.openid}</span></td>
              <td>${it.is_designer ? `设计师 ${it.designer_name || ''} (${it.designer_status || '-'})` : '普通用户'}</td>
              <td>总 ${it.total_orders} / 待付 ${it.pending_orders} / 已付 ${it.paid_orders}<br/>退款中 ${it.refunding_orders} / 已退款 ${it.refunded_orders}</td>
              <td>成交￥${it.paid_amount}<br/>退款￥${it.refund_amount}<br/>净额￥${it.net_amount}</td>
              <td>投稿 ${it.submissions_count} / 预约 ${it.reservations_count}</td>
              <td>最近下单：${it.last_order_at || '-'}<br/>最近登录：${it.updated_at || '-'}</td>
              <td><button class="btn-small" onclick="loadUserDetail(${it.user_id})">查看详情</button></td>
            </tr>`
          )
          .join('')
      }

      function renderFeedbacks(items, summary) {
        const sum = summary || {}
        document.getElementById('feedbackSummary').innerText =
          `共 ${sum.total || 0} 条｜待处理 ${sum.pending_count || 0}｜处理中 ${sum.processing_count || 0}｜已解决 ${sum.resolved_count || 0}｜已驳回 ${sum.rejected_count || 0}`
        const rows = document.getElementById('feedbackRows')
        rows.innerHTML = (items || [])
          .map(
            (it) => `<tr>
              <td>${it.id}</td>
              <td>${it.user_nickname || '-'}<br/><span class="muted">${it.user_openid || '-'}</span></td>
              <td>${it.category || '-'}</td>
              <td>${it.priority_text || it.priority || '-'}</td>
              <td>${it.content || '-'}</td>
              <td>${renderFeedbackImages(it.image_urls || [])}</td>
              <td>${it.status_text || it.status || '-'}</td>
              <td>${it.admin_reply || '-'}<br/><span class="muted">${it.reply_operator || '-'} ${it.replied_at || ''}</span></td>
              <td>提交：${it.created_at || '-'}<br/>更新：${it.updated_at || '-'}</td>
              <td>
                <button class="btn-small" onclick="replyFeedback(${it.id}, 'processing')">处理中</button>
                <button class="btn-small" onclick="replyFeedback(${it.id}, 'resolved')">已解决</button>
                <button class="btn-small" onclick="replyFeedback(${it.id}, 'rejected')">驳回</button>
              </td>
            </tr>`
          )
          .join('')
      }

      function renderFeedbackImages(urls) {
        const list = (urls || []).filter(Boolean)
        if (!list.length) {
          return '-'
        }
        const imgNodes = list
          .map((u, idx) => {
            const encoded = encodeURIComponent(String(u))
            return `<div>
              <img class="feedback-thumb" src="${u}" alt="附件${idx + 1}" onclick="previewFeedbackImage('${encoded}')" />
              <div><a class="feedback-img-link" href="${u}" target="_blank" rel="noopener noreferrer">附件${idx + 1}</a></div>
            </div>`
          })
          .join('')
        return `<div class="feedback-images">${imgNodes}</div>`
      }

      function previewFeedbackImage(encodedUrl) {
        const url = decodeURIComponent(String(encodedUrl || ''))
        if (!url) {
          return
        }
        const mask = document.getElementById('imgPreviewMask')
        const img = document.getElementById('imgPreviewImage')
        const link = document.getElementById('imgPreviewOpenLink')
        img.src = url
        link.href = url
        mask.classList.add('show')
      }

      function closeFeedbackImagePreview(event) {
        if (event) {
          event.preventDefault()
          const panel = event.target && event.target.closest ? event.target.closest('.img-preview-panel') : null
          if (panel && event.currentTarget === document.getElementById('imgPreviewMask')) {
            return
          }
        }
        const mask = document.getElementById('imgPreviewMask')
        const img = document.getElementById('imgPreviewImage')
        const link = document.getElementById('imgPreviewOpenLink')
        if (mask) {
          mask.classList.remove('show')
        }
        if (img) {
          img.src = ''
        }
        if (link) {
          link.href = '#'
        }
      }

      function renderFeedbackTemplates(items) {
        feedbackTemplates = (items || []).slice()
        const select = document.getElementById('feedbackTemplateSelect')
        const options = ['<option value="">回复模板（可选）</option>'].concat(
          feedbackTemplates.map((it) => `<option value="${it.code}">${it.title} (${it.code})</option>`)
        )
        select.innerHTML = options.join('')
      }

      function getSelectedFeedbackTemplate() {
        const code = document.getElementById('feedbackTemplateSelect').value
        if (!code) {
          return null
        }
        return feedbackTemplates.find((x) => x.code === code) || null
      }

      async function loadFeedbackTemplates() {
        const ret = await apiGet('/api/admin/feedback/templates?limit=200')
        renderFeedbackTemplates(ret.items || [])
      }

      async function saveFeedbackTemplate() {
        const code = document.getElementById('feedbackTplCode').value.trim()
        const title = document.getElementById('feedbackTplTitle').value.trim()
        const content = document.getElementById('feedbackTplContent').value.trim()
        const isActive = document.getElementById('feedbackTplActive').value === '1'
        if (!code || !title || !content) {
          alert('请完整填写模板编码、标题和内容')
          return
        }
        try {
          await apiPost('/api/admin/feedback/templates/upsert', {
            code,
            title,
            content,
            is_active: isActive
          })
          await loadFeedbackTemplates()
          setStatus(`反馈模板 ${code} 已保存`)
        } catch (err) {
          alert(err.message || '模板保存失败')
        }
      }

      async function replyFeedback(feedbackId, status) {
        const label = status === 'resolved' ? '已解决回复' : status === 'rejected' ? '驳回说明' : '处理说明（可选）'
        const template = getSelectedFeedbackTemplate()
        const defaultText = template ? template.content || '' : ''
        const reply = prompt(`请输入${label}`, status === 'processing' ? defaultText : defaultText)
        if (reply == null) {
          return
        }
        try {
          await apiPost(`/api/admin/feedback/${encodeURIComponent(feedbackId)}/reply`, {
            status,
            admin_reply: reply,
            template_code: template ? template.code : ''
          })
          await Promise.all([loadFeedbacks(), loadDashboard(), loadActionLogs()])
          setStatus(`反馈 ${feedbackId} 已更新`)
        } catch (err) {
          alert(err.message || '反馈处理失败')
        }
      }

      async function loadFeedbacks() {
        const status = document.getElementById('feedbackStatusFilter').value
        const priority = document.getElementById('feedbackPriorityFilter').value
        const keyword = document.getElementById('feedbackKeyword').value.trim()
        const params = new URLSearchParams()
        params.set('limit', '300')
        if (status) {
          params.set('status', status)
        }
        if (priority) {
          params.set('priority', priority)
        }
        if (keyword) {
          params.set('keyword', keyword)
        }
        const ret = await apiGet(`/api/admin/feedback?${params.toString()}`)
        renderFeedbacks(ret.items || [], ret.summary || {})
      }

      async function exportFeedbackCsv() {
        const cfg = getCfg()
        const status = document.getElementById('feedbackStatusFilter').value
        const priority = document.getElementById('feedbackPriorityFilter').value
        const keyword = document.getElementById('feedbackKeyword').value.trim()
        const params = new URLSearchParams()
        params.set('limit', '5000')
        if (status) {
          params.set('status', status)
        }
        if (priority) {
          params.set('priority', priority)
        }
        if (keyword) {
          params.set('keyword', keyword)
        }
        const href = `${cfg.baseUrl}/api/admin/feedback/export.csv?${params.toString()}`
        try {
          const res = await fetch(href, {
            headers: buildAdminHeaders()
          })
          if (!res.ok) {
            let message = '导出失败'
            try {
              const body = await res.json()
              message = body.detail || message
            } catch (err) {
              // ignore parse error
            }
            throw new Error(message)
          }
          const blob = await res.blob()
          const url = URL.createObjectURL(blob)
          const link = document.createElement('a')
          link.href = url
          const stamp = new Date().toISOString().slice(0, 19).replace(/[-:T]/g, '')
          link.download = `feedback_${stamp}.csv`
          document.body.appendChild(link)
          link.click()
          link.remove()
          URL.revokeObjectURL(url)
        } catch (err) {
          alert(err.message || '导出失败')
        }
      }

      function collectActionLogFilters() {
        return {
          actor: document.getElementById('actionActor').value.trim(),
          action_type: document.getElementById('actionType').value.trim(),
          target_type: document.getElementById('actionTargetType').value,
          target_id: document.getElementById('actionTargetId').value.trim(),
          related_user_id: document.getElementById('actionRelatedUserId').value.trim(),
          created_from: document.getElementById('actionCreatedFrom').value.trim(),
          created_to: document.getElementById('actionCreatedTo').value.trim()
        }
      }

      function loadActionLogPrefs() {
        try {
          const raw = localStorage.getItem(ACTION_LOG_PREFS_KEY)
          if (!raw) {
            return null
          }
          const prefs = JSON.parse(raw)
          return prefs && typeof prefs === 'object' ? prefs : null
        } catch (err) {
          return null
        }
      }

      function saveActionLogPrefs() {
        try {
          const f = collectActionLogFilters()
          const payload = {
            ...f,
            limit: actionLogLimit,
            sort_by: actionLogSortBy,
            sort_order: actionLogSortOrder
          }
          localStorage.setItem(ACTION_LOG_PREFS_KEY, JSON.stringify(payload))
        } catch (err) {
          // ignore storage failure
        }
      }

      function restoreActionLogPrefs() {
        const prefs = loadActionLogPrefs()
        if (!prefs) {
          return
        }
        document.getElementById('actionActor').value = String(prefs.actor || '')
        document.getElementById('actionType').value = String(prefs.action_type || '')
        document.getElementById('actionTargetType').value = String(prefs.target_type || '')
        document.getElementById('actionTargetId').value = String(prefs.target_id || '')
        document.getElementById('actionRelatedUserId').value = String(prefs.related_user_id || '')
        document.getElementById('actionCreatedFrom').value = String(prefs.created_from || '')
        document.getElementById('actionCreatedTo').value = String(prefs.created_to || '')
        const limit = Number(prefs.limit || 100)
        if ([50, 100, 200].includes(limit)) {
          actionLogLimit = limit
          document.getElementById('actionLogLimit').value = String(limit)
        }
        const sortBy = String(prefs.sort_by || 'created_at')
        const sortOrder = String(prefs.sort_order || 'desc')
        if (['created_at', 'actor', 'action_type'].includes(sortBy)) {
          actionLogSortBy = sortBy
        }
        if (['asc', 'desc'].includes(sortOrder)) {
          actionLogSortOrder = sortOrder
        }
        syncActionLogSortIndicators()
      }

      function applyActionLogSortFromUrl() {
        try {
          const params = new URLSearchParams(window.location.search || '')
          const rawBy = String(params.get(ACTION_LOG_SORT_BY_QS) || '').trim().toLowerCase()
          const rawOrder = String(params.get(ACTION_LOG_SORT_ORDER_QS) || '').trim().toLowerCase()
          if (!rawBy && !rawOrder) {
            return false
          }
          if (['created_at', 'actor', 'action_type'].includes(rawBy)) {
            actionLogSortBy = rawBy
          }
          if (['asc', 'desc'].includes(rawOrder)) {
            actionLogSortOrder = rawOrder
          } else {
            actionLogSortOrder = actionLogSortBy === 'created_at' ? 'desc' : 'asc'
          }
          return true
        } catch (err) {
          return false
        }
      }

      function syncActionLogSortToUrl() {
        try {
          const url = new URL(window.location.href)
          const isDefault = actionLogSortBy === 'created_at' && actionLogSortOrder === 'desc'
          if (isDefault) {
            url.searchParams.delete(ACTION_LOG_SORT_BY_QS)
            url.searchParams.delete(ACTION_LOG_SORT_ORDER_QS)
          } else {
            url.searchParams.set(ACTION_LOG_SORT_BY_QS, actionLogSortBy)
            url.searchParams.set(ACTION_LOG_SORT_ORDER_QS, actionLogSortOrder)
          }
          const next = `${url.pathname}${url.search}${url.hash}`
          const current = `${window.location.pathname}${window.location.search}${window.location.hash}`
          if (next !== current) {
            window.history.replaceState(null, '', next)
          }
        } catch (err) {
          // ignore URL sync failure
        }
      }

      function applyActionLogFilters() {
        actionLogOffset = 0
        saveActionLogPrefs()
        loadActionLogs()
      }

      function changeActionLogLimit() {
        actionLogLimit = Number(document.getElementById('actionLogLimit').value || '100')
        actionLogOffset = 0
        saveActionLogPrefs()
        loadActionLogs()
      }

      function goFirstActionLogPage() {
        if (actionLogOffset <= 0) {
          return
        }
        actionLogOffset = 0
        loadActionLogs()
      }

      function goLastActionLogPage() {
        const totalPages = Math.max(1, Math.ceil(Number(actionLogTotal || 0) / Math.max(1, actionLogLimit)))
        const lastOffset = (totalPages - 1) * actionLogLimit
        if (lastOffset === actionLogOffset) {
          return
        }
        actionLogOffset = lastOffset
        loadActionLogs()
      }

      function changeActionLogPage(step) {
        const next = actionLogOffset + Number(step || 0) * actionLogLimit
        if (next < 0) {
          return
        }
        actionLogOffset = next
        loadActionLogs()
      }

      function syncActionLogSortIndicators() {
        const defs = [
          { id: 'actionSortCreatedAt', key: 'created_at', label: '时间' },
          { id: 'actionSortActor', key: 'actor', label: '操作人' },
          { id: 'actionSortActionType', key: 'action_type', label: '动作类型' }
        ]
        defs.forEach((item) => {
          const el = document.getElementById(item.id)
          if (!el) {
            return
          }
          const active = actionLogSortBy === item.key
          el.classList.toggle('active', active)
          el.innerText = `${item.label}${active ? (actionLogSortOrder === 'asc' ? ' ↑' : ' ↓') : ''}`
        })
      }

      function toggleActionLogSort(field) {
        const nextField = String(field || '').trim()
        if (!['created_at', 'actor', 'action_type'].includes(nextField)) {
          return
        }
        if (actionLogSortBy === nextField) {
          actionLogSortOrder = actionLogSortOrder === 'asc' ? 'desc' : 'asc'
        } else {
          actionLogSortBy = nextField
          actionLogSortOrder = nextField === 'created_at' ? 'desc' : 'asc'
        }
        actionLogOffset = 0
        syncActionLogSortIndicators()
        saveActionLogPrefs()
        syncActionLogSortToUrl()
        loadActionLogs()
      }

      function handleActionLogJumpKey(event) {
        if (event && event.key === 'Enter') {
          event.preventDefault()
          jumpActionLogPage()
        }
      }

      function jumpActionLogPage() {
        const totalPages = Math.max(1, Math.ceil(Number(actionLogTotal || 0) / Math.max(1, actionLogLimit)))
        const raw = Number(document.getElementById('actionLogJumpPage').value || '1')
        const targetPage = Math.max(1, Math.min(totalPages, Number.isFinite(raw) ? Math.floor(raw) : 1))
        actionLogOffset = (targetPage - 1) * actionLogLimit
        document.getElementById('actionLogJumpPage').value = String(targetPage)
        loadActionLogs()
      }

      function resetActionLogFilters() {
        document.getElementById('actionActor').value = ''
        document.getElementById('actionType').value = ''
        document.getElementById('actionTargetType').value = ''
        document.getElementById('actionTargetId').value = ''
        document.getElementById('actionRelatedUserId').value = ''
        document.getElementById('actionCreatedFrom').value = ''
        document.getElementById('actionCreatedTo').value = ''
        actionLogOffset = 0
        saveActionLogPrefs()
        loadActionLogs()
      }

      function openUserDetailFromAction(userId) {
        const uid = Number(userId || 0)
        if (!uid) {
          return
        }
        setAdminModule('user')
        loadUserDetail(uid)
        const card = document.getElementById('userDetailCard')
        if (card) {
          setTimeout(() => {
            card.scrollIntoView({ behavior: 'smooth', block: 'start' })
          }, 120)
        }
      }

      function renderActionLogs(items, summary, paging) {
        const sum = summary || {}
        const page = paging || {}
        syncActionLogSortIndicators()
        const topActions = (sum.top_actions || [])
          .slice(0, 5)
          .map((it) => `${it.action_type}: ${it.count}`)
          .join('｜')
        document.getElementById('actionLogSummary').innerText =
          `共 ${sum.total || 0} 条｜操作人 ${sum.actors || 0}｜关联用户 ${sum.related_users || 0}｜最早 ${sum.earliest_at || '-'}｜最新 ${sum.latest_at || '-'}${topActions ? `｜高频动作 ${topActions}` : ''}`
        const total = Number(sum.total || 0)
        const offset = Number(page.offset || 0)
        const returned = Number(page.returned || 0)
        const limit = Math.max(1, Number(page.limit || actionLogLimit || 100))
        actionLogTotal = total
        const start = total > 0 ? offset + 1 : 0
        const end = total > 0 ? offset + returned : 0
        const totalPages = Math.max(1, Math.ceil(total / limit))
        const currentPage = Math.min(totalPages, Math.floor(offset / limit) + 1)
        document.getElementById('actionLogPaging').innerText =
          `第 ${currentPage}/${totalPages} 页｜当前 ${start}-${end} / ${total}（每页 ${limit}）`
        document.getElementById('actionLogFirstBtn').disabled = offset <= 0
        document.getElementById('actionLogPrevBtn').disabled = offset <= 0
        document.getElementById('actionLogNextBtn').disabled = !page.has_more
        document.getElementById('actionLogLastBtn').disabled = !page.has_more
        const jumpInput = document.getElementById('actionLogJumpPage')
        jumpInput.value = String(currentPage)
        jumpInput.max = String(totalPages)
        jumpInput.min = '1'

        const rows = document.getElementById('actionLogRows')
        if (!(items || []).length) {
          rows.innerHTML = `<tr><td colspan="7" class="empty-row">${total > 0 ? '当前页暂无记录，请切换页码查看。' : '暂无符合条件的操作日志，请调整筛选条件后重试。'}</td></tr>`
          return
        }
        rows.innerHTML = (items || [])
          .map(
            (it) => `<tr>
              <td>${it.id || '-'}</td>
              <td>${it.created_at || '-'}</td>
              <td>${it.actor || '-'}</td>
              <td>${it.action_type || '-'}</td>
              <td>${it.target_type || '-'} / ${it.target_id || '-'}</td>
              <td>${
                it.related_user_id
                  ? `<button class="btn-small" onclick="openUserDetailFromAction(${it.related_user_id})">${it.related_user_id}</button>`
                  : '-'
              }</td>
              <td>${formatActionDetail(it.detail)}</td>
            </tr>`
          )
          .join('')
      }

      function refreshActionLogExportLink() {
        const cfg = getCfg()
        const f = collectActionLogFilters()
        const params = new URLSearchParams()
        params.set('limit', '5000')
        if (f.actor) {
          params.set('actor', f.actor)
        }
        if (f.action_type) {
          params.set('action_type', f.action_type)
        }
        if (f.target_type) {
          params.set('target_type', f.target_type)
        }
        if (f.target_id) {
          params.set('target_id', f.target_id)
        }
        if (f.related_user_id) {
          params.set('related_user_id', f.related_user_id)
        }
        if (f.created_from) {
          params.set('created_from', f.created_from)
        }
        if (f.created_to) {
          params.set('created_to', f.created_to)
        }
        params.set('sort_by', actionLogSortBy)
        params.set('sort_order', actionLogSortOrder)
        const href = `${cfg.baseUrl}/api/admin/action-logs/export.csv?${params.toString()}`
        const el = document.getElementById('actionLogExportLink')
        el.href = href
        el.onclick = async (e) => {
          e.preventDefault()
          try {
            const res = await fetch(href, {
              headers: buildAdminHeaders()
            })
            if (!res.ok) {
              let message = '导出失败'
              try {
                const body = await res.json()
                message = body.detail || message
              } catch (err) {
                // ignore parse error
              }
              throw new Error(message)
            }
            const blob = await res.blob()
            const url = URL.createObjectURL(blob)
            const link = document.createElement('a')
            link.href = url
            const stamp = new Date().toISOString().slice(0, 19).replace(/[-:T]/g, '')
            link.download = `admin_action_logs_${stamp}.csv`
            document.body.appendChild(link)
            link.click()
            link.remove()
            URL.revokeObjectURL(url)
          } catch (err) {
            alert(err.message || '导出失败')
          }
        }
      }

      async function loadActionLogs() {
        const f = collectActionLogFilters()
        saveActionLogPrefs()
        const params = new URLSearchParams()
        params.set('limit', String(actionLogLimit))
        params.set('offset', String(actionLogOffset))
        if (f.actor) {
          params.set('actor', f.actor)
        }
        if (f.action_type) {
          params.set('action_type', f.action_type)
        }
        if (f.target_type) {
          params.set('target_type', f.target_type)
        }
        if (f.target_id) {
          params.set('target_id', f.target_id)
        }
        if (f.related_user_id) {
          params.set('related_user_id', f.related_user_id)
        }
        if (f.created_from) {
          params.set('created_from', f.created_from)
        }
        if (f.created_to) {
          params.set('created_to', f.created_to)
        }
        params.set('sort_by', actionLogSortBy)
        params.set('sort_order', actionLogSortOrder)
        const ret = await apiGet(`/api/admin/action-logs?${params.toString()}`)
        const sum = ret.summary || {}
        const page = ret.paging || {}
        const sorting = ret.sorting || {}
        if (sorting.sort_by && ['created_at', 'actor', 'action_type'].includes(String(sorting.sort_by))) {
          actionLogSortBy = String(sorting.sort_by)
        }
        if (sorting.sort_order && ['asc', 'desc'].includes(String(sorting.sort_order))) {
          actionLogSortOrder = String(sorting.sort_order)
        }
        syncActionLogSortToUrl()
        const total = Number(sum.total || 0)
        const returned = Number(page.returned || 0)
        if (total > 0 && returned === 0 && actionLogOffset > 0) {
          const lastOffset = Math.max(0, (Math.ceil(total / actionLogLimit) - 1) * actionLogLimit)
          if (lastOffset !== actionLogOffset) {
            actionLogOffset = lastOffset
            return loadActionLogs()
          }
        }
        renderActionLogs(ret.items || [], ret.summary || {}, ret.paging || {})
        refreshActionLogExportLink()
      }

      function closeUserDetail() {
        currentUserDetailId = 0
        document.getElementById('userDetailCard').style.display = 'none'
      }

      function formatActionDetail(detail) {
        if (!detail || typeof detail !== 'object') {
          return '-'
        }
        const keys = Object.keys(detail)
        if (!keys.length) {
          return '-'
        }
        return keys
          .map((k) => `${k}: ${typeof detail[k] === 'object' ? JSON.stringify(detail[k]) : detail[k]}`)
          .join(' | ')
      }

      function renderUserDetail(detail) {
        const user = detail.user || {}
        const orderSummary = detail.order_summary || {}
        const designer = detail.designer || {}
        document.getElementById('userDetailCard').style.display = 'block'
        currentUserDetailId = Number(user.user_id || 0)
        document.getElementById('userDetailTitle').innerText = `${user.nickname || '-'} (${user.openid || '-'})`
        document.getElementById('userDetailSummary').innerText =
          `订单 ${orderSummary.total_orders || 0}｜已付 ${orderSummary.paid_orders || 0}｜退款中 ${orderSummary.refunding_orders || 0}｜已退款 ${orderSummary.refunded_orders || 0}｜成交￥${orderSummary.paid_amount || 0}｜退款￥${orderSummary.refund_amount || 0}｜净额￥${orderSummary.net_amount || 0}`
        const cfg = getCfg()
        const exportHref = `${cfg.baseUrl}/api/admin/users/${encodeURIComponent(currentUserDetailId)}/orders/export.csv?limit=5000`
        const exportEl = document.getElementById('userDetailExportLink')
        exportEl.href = exportHref
        exportEl.onclick = async (e) => {
          e.preventDefault()
          try {
            const res = await fetch(exportHref, {
              headers: buildAdminHeaders()
            })
            if (!res.ok) {
              let message = '导出失败'
              try {
                const body = await res.json()
                message = body.detail || message
              } catch (err) {}
              throw new Error(message)
            }
            const blob = await res.blob()
            const url = URL.createObjectURL(blob)
            const link = document.createElement('a')
            link.href = url
            const stamp = new Date().toISOString().slice(0, 19).replace(/[-:T]/g, '')
            link.download = `user_orders_${currentUserDetailId}_${stamp}.csv`
            document.body.appendChild(link)
            link.click()
            link.remove()
            URL.revokeObjectURL(url)
          } catch (err) {
            alert(err.message || '导出失败')
          }
        }

        const orderRows = document.getElementById('userDetailOrderRows')
        orderRows.innerHTML = (detail.orders || [])
          .map(
            (it) => `<tr>
              <td>${it.order_id}</td>
              <td>${it.sale_mode === 'crowdfunding' ? '众筹' : '预售'}</td>
              <td>应付￥${it.total_amount}<br/>实付￥${it.paid_amount}<br/>退款￥${it.refund_amount || 0}</td>
              <td>${it.order_status_text}<br/><span class="muted">${it.pay_status} / ${it.refund_status || '-'}</span><br/><span class="muted">备注：${it.admin_note || '-'}</span></td>
              <td>下单：${it.created_at}<br/>支付：${it.paid_at || '-'}<br/>退款：${it.refunded_at || '-'}</td>
            </tr>`
          )
          .join('')

        const submissionRows = document.getElementById('userDetailSubmissionRows')
        submissionRows.innerHTML = (detail.submissions || [])
          .map(
            (it) => `<tr>
              <td>${it.work_name}</td>
              <td>${it.status_text}</td>
              <td>${it.created_at}</td>
            </tr>`
          )
          .join('')

        const reservationRows = document.getElementById('userDetailReservationRows')
        reservationRows.innerHTML = (detail.reservations || [])
          .map(
            (it) => `<tr>
              <td>${it.work_name || it.work_id}</td>
              <td>${it.created_at}</td>
            </tr>`
          )
          .join('')

        const designerBox = document.getElementById('userDetailDesignerBox')
        if (!designer.is_designer) {
          designerBox.innerText = '该用户不是设计师账号。'
        } else {
          const profile = designer.profile || {}
          const assignments = designer.assignments || []
          const com = designer.commission_summary || {}
          const assignText = assignments.length
            ? assignments.map((x) => `${x.work_name}(${x.share_percent}%)`).join('；')
            : '无绑定作品'
          designerBox.innerText =
            `设计师：${profile.display_name || '-'}｜状态：${profile.status_text || profile.status || '-'}｜绑定：${assignText}｜分成单 ${com.records || 0}（未结算 ${com.pending_records || 0} / 已结算 ${com.settled_records || 0}）｜分成总额￥${com.commission_amount || 0}`
        }

        const actionRows = document.getElementById('userDetailActionRows')
        actionRows.innerHTML = (detail.action_logs || [])
          .map(
            (it) => `<tr>
              <td>${it.created_at || '-'}</td>
              <td>${it.actor || '-'}</td>
              <td>${it.action_type || '-'}</td>
              <td>${it.target_type || '-'} / ${it.target_id || '-'}</td>
              <td>${formatActionDetail(it.detail)}</td>
            </tr>`
          )
          .join('')
      }

      async function loadUserDetail(userId) {
        try {
          const ret = await apiGet(`/api/admin/users/${encodeURIComponent(userId)}/detail`)
          renderUserDetail(ret || {})
        } catch (err) {
          alert(err.message || '加载用户详情失败')
        }
      }

      function renderCommissions(items) {
        currentCommissionItems = (items || []).slice()
        const rows = document.getElementById('commissionRows')
        rows.innerHTML = (items || [])
          .map(
            (it) => `<tr>
              <td>
                <input
                  type="checkbox"
                  ${selectedCommissionIds.has(it.record_id) ? 'checked' : ''}
                  onchange="toggleCommissionSelection(${it.record_id}, this.checked)"
                />
              </td>
              <td>${it.record_id}</td>
              <td>${it.display_name || '-'}</td>
              <td>${it.order_id}</td>
              <td>${it.work_name}</td>
              <td>￥${it.total_amount}</td>
              <td>${it.share_percent}%</td>
              <td>￥${it.commission_amount}</td>
              <td>${it.settlement_status_text}</td>
              <td>${it.settled_at || '-'}</td>
              <td>
                <button class="btn-small" onclick="settleCommission(${it.record_id}, 'settled')">标记已结算</button>
                <button class="btn-small" onclick="settleCommission(${it.record_id}, 'pending')">改回未结算</button>
              </td>
            </tr>`
          )
          .join('')
      }

      function toggleCommissionSelection(recordId, checked) {
        if (checked) {
          selectedCommissionIds.add(Number(recordId))
        } else {
          selectedCommissionIds.delete(Number(recordId))
        }
      }

      function toggleSelectAllCommissions(selectAll) {
        if (selectAll) {
          ;(currentCommissionItems || []).forEach((it) => {
            selectedCommissionIds.add(Number(it.record_id))
          })
        } else {
          selectedCommissionIds.clear()
        }
        renderCommissions(currentCommissionItems)
      }

      function refreshExportLink() {
        const cfg = getCfg()
        const status = document.getElementById('commissionStatusFilter').value
        const qs = status ? `?status=${encodeURIComponent(status)}` : ''
        const href = `${cfg.baseUrl}/api/admin/commissions/export.csv${qs}`
        const el = document.getElementById('exportLink')
        el.href = href
        el.onclick = async (e) => {
          e.preventDefault()
          try {
            const res = await fetch(href, {
              headers: buildAdminHeaders()
            })
            if (!res.ok) {
              let message = '导出失败'
              try {
                const body = await res.json()
                message = body.detail || message
              } catch (err) {
                // ignore parse error
              }
              throw new Error(message)
            }
            const blob = await res.blob()
            const url = URL.createObjectURL(blob)
            const link = document.createElement('a')
            link.href = url
            const stamp = new Date().toISOString().slice(0, 19).replace(/[-:T]/g, '')
            link.download = `designer_commissions_${stamp}.csv`
            document.body.appendChild(link)
            link.click()
            link.remove()
            URL.revokeObjectURL(url)
          } catch (err) {
            alert(err.message || '导出失败')
          }
        }
      }

      function refreshOrderExportLink() {
        const cfg = getCfg()
        const f = collectOrderFilters()
        const params = new URLSearchParams()
        params.set('limit', '5000')
        if (f.keyword) {
          params.set('keyword', f.keyword)
        }
        if (f.sale_mode) {
          params.set('sale_mode', f.sale_mode)
        }
        if (f.pay_status) {
          params.set('pay_status', f.pay_status)
        }
        if (f.refund_status) {
          params.set('refund_status', f.refund_status)
        }
        if (f.order_status) {
          params.set('order_status', f.order_status)
        }
        const href = `${cfg.baseUrl}/api/admin/orders/export.csv?${params.toString()}`
        const el = document.getElementById('orderExportLink')
        el.href = href
        el.onclick = async (e) => {
          e.preventDefault()
          try {
            const res = await fetch(href, {
              headers: buildAdminHeaders()
            })
            if (!res.ok) {
              let message = '导出失败'
              try {
                const body = await res.json()
                message = body.detail || message
              } catch (err) {
                // ignore parse error
              }
              throw new Error(message)
            }
            const blob = await res.blob()
            const url = URL.createObjectURL(blob)
            const link = document.createElement('a')
            link.href = url
            const stamp = new Date().toISOString().slice(0, 19).replace(/[-:T]/g, '')
            link.download = `orders_${stamp}.csv`
            document.body.appendChild(link)
            link.click()
            link.remove()
            URL.revokeObjectURL(url)
          } catch (err) {
            alert(err.message || '导出失败')
          }
        }
      }

      function syncWorkForm(work) {
        if (!work) {
          return
        }
        const nameEl = document.getElementById('workNameInput')
        const subtitleEl = document.getElementById('workSubtitleInput')
        const saleModeEl = document.getElementById('workSaleMode')
        const goalEl = document.getElementById('crowdGoalInput')
        const deadlineEl = document.getElementById('crowdDeadlineInput')
        const assignWorkIdEl = document.getElementById('assignWorkId')
        const statusLineEl = document.getElementById('workStatusLine')
        if (nameEl) nameEl.value = work.name || ''
        if (subtitleEl) subtitleEl.value = work.subtitle || ''
        if (saleModeEl) saleModeEl.value = work.sale_mode || 'preorder'
        if (goalEl) goalEl.value = Number(work.crowdfunding_goal_amount || 0)
        if (deadlineEl) deadlineEl.value = work.crowdfunding_deadline || ''
        if (assignWorkIdEl) assignWorkIdEl.value = work.work_id || ''
        const statusText = work.crowdfunding_status_text || '-'
        if (statusLineEl) {
          statusLineEl.innerText = `当前模式：${work.sale_mode || 'preorder'} · 众筹状态：${statusText}`
        }
      }

      async function loadWorkConfig() {
        const cfg = getCfg()
        const ret = await fetch(cfg.baseUrl + '/api/work/current')
        const body = await ret.json()
        if (!ret.ok) {
          throw new Error(body.detail || '加载作品配置失败')
        }
        syncWorkForm(body.work || null)
      }

      async function saveWorkConfig() {
        const saleMode = document.getElementById('workSaleMode').value
        const goal = Number(document.getElementById('crowdGoalInput').value || '0')
        const payload = {
          name: document.getElementById('workNameInput').value.trim(),
          subtitle: document.getElementById('workSubtitleInput').value.trim(),
          sale_mode: saleMode,
          crowdfunding_goal_amount: goal,
          crowdfunding_deadline: document.getElementById('crowdDeadlineInput').value.trim()
        }
        if (!payload.name) {
          alert('作品名称不能为空')
          return
        }
        if (saleMode === 'crowdfunding' && goal <= 0) {
          alert('众筹模式下目标金额必须大于0')
          return
        }
        try {
          const ret = await apiPut('/api/admin/work/current', payload)
          syncWorkForm(ret.work || null)
          await Promise.all([loadDashboard(), loadCommissions()])
          setStatus('作品发布配置已保存')
        } catch (err) {
          alert(err.message)
        }
      }

      function permissionLabelByKey(key) {
        const safe = String(key || '').trim().toLowerCase()
        const matched = ADMIN_PERMISSION_META.find((x) => x.key === safe)
        return matched ? matched.label : safe
      }

      function renderPermissionCheckboxes(containerId, selectedPermissions, disabledPermissions) {
        const container = document.getElementById(containerId)
        if (!container) {
          return
        }
        const selectedSet = new Set(normalizePermissionList(selectedPermissions))
        const disabledSet = new Set(normalizePermissionList(disabledPermissions))
        container.innerHTML = ADMIN_PERMISSION_META.map((item) => {
          const checked = selectedSet.has(item.key)
          const disabled = disabledSet.has(item.key)
          return `<label class="perm-check">
            <input
              type="checkbox"
              value="${item.key}"
              ${checked ? 'checked' : ''}
              ${disabled ? 'disabled' : ''}
            />
            <span>${item.label}</span>
          </label>`
        }).join('')
      }

      function collectPermissionSelection(containerId) {
        const container = document.getElementById(containerId)
        if (!container) {
          return []
        }
        const selected = Array.from(container.querySelectorAll('input[type="checkbox"]:checked')).map((el) => el.value)
        return normalizePermissionList(selected)
      }

      function syncAdminIdentityToDom() {
        const roleEl = document.getElementById('adminRole')
        const operatorEl = document.getElementById('adminOperator')
        const tokenEl = document.getElementById('adminToken')
        const roleHintEl = document.getElementById('adminRoleHint')
        if (currentAdminIdentity) {
          const roleKey = String(currentAdminIdentity.role_key || '').trim() || 'superadmin'
          if (roleEl) {
            roleEl.value = roleKey
            roleEl.disabled = true
          }
          const operatorName =
            String(currentAdminIdentity.display_name || '').trim() || String(currentAdminIdentity.username || '').trim()
          if (operatorEl && operatorName) {
            operatorEl.value = operatorName
          }
          if (tokenEl) {
            tokenEl.disabled = true
          }
          if (roleHintEl) {
            roleHintEl.innerText = `当前岗位：${currentAdminIdentity.role_name || roleKey}`
          }
        } else {
          if (roleEl) {
            roleEl.disabled = false
          }
          if (tokenEl) {
            tokenEl.disabled = false
          }
          if (roleHintEl) {
            roleHintEl.innerText = ''
          }
        }
        renderAdminAuthState()
        applyRoleModuleVisibility()
      }

      function renderAdminAuthState() {
        const stateEl = document.getElementById('adminAuthState')
        const loginBtn = document.getElementById('adminLoginBtn')
        const logoutBtn = document.getElementById('adminLogoutBtn')
        const legacyTipEl = document.getElementById('adminLegacyTip')
        if (currentAdminIdentity) {
          if (stateEl) {
            stateEl.innerText = `已登录：${currentAdminIdentity.display_name || currentAdminIdentity.username}（${
              currentAdminIdentity.role_name || currentAdminIdentity.role_key || '-'
            }）`
          }
          if (loginBtn) loginBtn.disabled = true
          if (logoutBtn) logoutBtn.disabled = false
          if (legacyTipEl) {
            legacyTipEl.innerText = ''
          }
        } else {
          if (stateEl) {
            stateEl.innerText = '未登录（可使用用户名密码登录，或继续使用旧版Token）'
          }
          if (loginBtn) loginBtn.disabled = false
          if (logoutBtn) logoutBtn.disabled = true
          if (legacyTipEl) {
            legacyTipEl.innerText = '兼容模式：未登录会话时将使用上方 Token + 角色访问。'
          }
        }
      }

      async function loadAdminAuthMe(silent = true) {
        const session = getStoredAdminSession()
        if (!session) {
          currentAdminIdentity = null
          currentAdminPermissions = []
          syncAdminIdentityToDom()
          return null
        }
        try {
          const ret = await apiGet('/api/admin/auth/me')
          const admin = (ret && ret.admin) || null
          if (!admin) {
            throw new Error('登录信息无效')
          }
          currentAdminIdentity = admin
          currentAdminPermissions = normalizePermissionList(admin.permissions || [])
          syncAdminIdentityToDom()
          return admin
        } catch (err) {
          clearAdminSessionIdentity(Boolean(silent))
          if (!silent) {
            alert(err.message || '登录态失效，请重新登录')
          }
          return null
        }
      }

      async function adminLogin() {
        const usernameEl = document.getElementById('adminUsername')
        const passwordEl = document.getElementById('adminPassword')
        const username = String((usernameEl && usernameEl.value) || '').trim()
        const password = String((passwordEl && passwordEl.value) || '')
        const cfg = getCfg()
        if (!username || !password) {
          alert('请输入用户名和密码')
          return
        }
        try {
          const res = await fetch(`${cfg.baseUrl}/api/admin/auth/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
          })
          const body = await readResponseBody(res)
          if (!res.ok) {
            throw new Error(body.detail || '登录失败')
          }
          const token = String(body.session_token || '').trim()
          if (!token) {
            throw new Error('登录返回缺少会话令牌')
          }
          setStoredAdminSession(token)
          currentAdminIdentity = body.admin || null
          currentAdminPermissions = normalizePermissionList((body.admin && body.admin.permissions) || [])
          if (passwordEl) {
            passwordEl.value = ''
          }
          syncAdminIdentityToDom()
          await loadAll()
          setStatus('后台登录成功')
        } catch (err) {
          alert(err.message || '登录失败')
        }
      }

      async function adminLogout() {
        try {
          if (getStoredAdminSession()) {
            await apiPost('/api/admin/auth/logout', {})
          }
        } catch (err) {
          // ignore logout error
        }
        clearAdminSessionIdentity(true)
        setStatus('已退出后台登录')
      }

      function renderAdminRoles(items) {
        const list = Array.isArray(items) ? items : []
        adminRoleItemsCache = list.slice()
        const rows = document.getElementById('adminRoleRows')
        const summary = document.getElementById('adminRoleSummary')
        if (summary) {
          summary.innerText = `岗位 ${list.length} 个`
        }
        const roleSelect = document.getElementById('adminRoleEditKey')
        if (roleSelect) {
          const previous = roleSelect.value
          roleSelect.innerHTML = ['<option value="">请选择岗位</option>']
            .concat(
              list.map(
                (it) => `<option value="${it.role_key}">${it.role_name || it.role_key} (${it.role_key})${it.is_system ? ' [系统]' : ''}</option>`
              )
            )
            .join('')
          if (previous && list.some((x) => x.role_key === previous)) {
            roleSelect.value = previous
          }
        }
        const userCreateRole = document.getElementById('adminUserCreateRole')
        const userEditRole = document.getElementById('adminUserEditRole')
        ;[userCreateRole, userEditRole].forEach((el) => {
          if (!el) return
          const previous = el.value
          el.innerHTML = list
            .map((it) => `<option value="${it.role_key}">${it.role_name || it.role_key} (${it.role_key})</option>`)
            .join('')
          if (previous && list.some((x) => x.role_key === previous)) {
            el.value = previous
          } else if (list.length) {
            el.value = list[0].role_key
          }
        })
        if (rows) {
          rows.innerHTML = list
            .map((it) => {
              const perms = normalizePermissionList(it.permissions || [])
              const permText = perms.map((x) => permissionLabelByKey(x)).join(' / ') || '-'
              return `<tr>
                <td>${it.role_name || '-'}</td>
                <td>${it.role_key}</td>
                <td>${permText}</td>
                <td>${it.is_system ? '系统岗位' : '自定义岗位'}</td>
                <td>${it.updated_at || '-'}</td>
              </tr>`
            })
            .join('')
        }
        const selectedKey = roleSelect ? roleSelect.value : ''
        if (selectedKey) {
          handleAdminRoleEditChange()
        } else {
          renderPermissionCheckboxes('adminRoleCreatePermissions', [], [])
          renderPermissionCheckboxes('adminRoleEditPermissions', [], [])
        }
      }

      function handleAdminRoleEditChange() {
        const roleKey = String((document.getElementById('adminRoleEditKey') || {}).value || '').trim()
        const role = adminRoleItemsCache.find((x) => x.role_key === roleKey) || null
        const nameEl = document.getElementById('adminRoleEditName')
        const roleReadonlyEl = document.getElementById('adminRoleEditReadonlyTip')
        if (!role) {
          if (nameEl) nameEl.value = ''
          renderPermissionCheckboxes('adminRoleEditPermissions', [], [])
          if (roleReadonlyEl) roleReadonlyEl.innerText = ''
          return
        }
        if (nameEl) {
          nameEl.value = role.role_name || role.role_key
        }
        const isSuperadmin = role.role_key === 'superadmin'
        renderPermissionCheckboxes(
          'adminRoleEditPermissions',
          role.permissions || [],
          isSuperadmin ? ADMIN_PERMISSION_META.map((x) => x.key) : []
        )
        if (roleReadonlyEl) {
          roleReadonlyEl.innerText = role.is_system ? '系统岗位可改名称与权限；superadmin 权限固定为全量。' : ''
        }
      }

      async function loadAdminRoles() {
        if (!document.getElementById('adminRoleRows')) {
          return
        }
        const ret = await apiGet('/api/admin/roles')
        renderAdminRoles(ret.items || [])
      }

      async function createAdminRole() {
        const key = String((document.getElementById('adminRoleCreateKey') || {}).value || '')
          .trim()
          .toLowerCase()
        const name = String((document.getElementById('adminRoleCreateName') || {}).value || '').trim()
        const permissions = collectPermissionSelection('adminRoleCreatePermissions')
        if (!key || !name) {
          alert('请先填写岗位标识与岗位名称')
          return
        }
        if (!permissions.length) {
          alert('请至少选择 1 项权限')
          return
        }
        try {
          await apiPost('/api/admin/roles', {
            role_key: key,
            role_name: name,
            permissions
          })
          ;(document.getElementById('adminRoleCreateKey') || {}).value = ''
          ;(document.getElementById('adminRoleCreateName') || {}).value = ''
          renderPermissionCheckboxes('adminRoleCreatePermissions', [], [])
          await Promise.all([loadAdminRoles(), loadAdminUsers()])
          setStatus(`岗位 ${key} 已创建`)
        } catch (err) {
          alert(err.message || '创建岗位失败')
        }
      }

      async function updateAdminRole() {
        const roleKey = String((document.getElementById('adminRoleEditKey') || {}).value || '').trim()
        const roleName = String((document.getElementById('adminRoleEditName') || {}).value || '').trim()
        if (!roleKey) {
          alert('请先选择岗位')
          return
        }
        const role = adminRoleItemsCache.find((x) => x.role_key === roleKey) || null
        const permissions =
          roleKey === 'superadmin' ? ADMIN_PERMISSION_META.map((x) => x.key) : collectPermissionSelection('adminRoleEditPermissions')
        if (!roleName) {
          alert('岗位名称不能为空')
          return
        }
        if (!permissions.length) {
          alert('请至少选择 1 项权限')
          return
        }
        try {
          await apiPut(`/api/admin/roles/${encodeURIComponent(roleKey)}`, {
            role_name: roleName,
            permissions
          })
          await Promise.all([loadAdminRoles(), loadAdminUsers()])
          setStatus(`岗位 ${role && role.role_name ? role.role_name : roleKey} 已更新`)
        } catch (err) {
          alert(err.message || '更新岗位失败')
        }
      }

      function renderAdminUsers(items) {
        const list = Array.isArray(items) ? items : []
        adminUserItemsCache = list.slice()
        const rows = document.getElementById('adminUserRows')
        const summary = document.getElementById('adminUserSummary')
        if (summary) {
          summary.innerText = `管理员账号 ${list.length} 个`
        }
        if (!rows) {
          return
        }
        rows.innerHTML = list
          .map(
            (it) => `<tr>
              <td>${it.display_name || '-'}</td>
              <td>${it.username}</td>
              <td>${it.role_name || it.role_key} (${it.role_key})</td>
              <td>${(it.permissions || []).map((x) => permissionLabelByKey(x)).join(' / ') || '-'}</td>
              <td>${it.status_text || it.status || '-'}</td>
              <td>${it.last_login_at || '-'}</td>
              <td>${it.updated_at || '-'}</td>
              <td><button class="btn-small" onclick="startEditAdminUser(${it.admin_id})">编辑</button></td>
            </tr>`
          )
          .join('')
      }

      async function loadAdminUsers() {
        if (!document.getElementById('adminUserRows')) {
          return
        }
        const ret = await apiGet('/api/admin/admin-users?limit=500')
        renderAdminUsers(ret.items || [])
      }

      async function createAdminUser() {
        const username = String((document.getElementById('adminUserCreateUsername') || {}).value || '').trim()
        const password = String((document.getElementById('adminUserCreatePassword') || {}).value || '')
        const displayName = String((document.getElementById('adminUserCreateDisplayName') || {}).value || '').trim()
        const roleKey = String((document.getElementById('adminUserCreateRole') || {}).value || '').trim()
        const status = String((document.getElementById('adminUserCreateStatus') || {}).value || 'active').trim()
        if (!username || !password || !roleKey) {
          alert('请填写用户名、密码、岗位')
          return
        }
        try {
          await apiPost('/api/admin/admin-users', {
            username,
            password,
            display_name: displayName,
            role_key: roleKey,
            status
          })
          ;(document.getElementById('adminUserCreateUsername') || {}).value = ''
          ;(document.getElementById('adminUserCreatePassword') || {}).value = ''
          ;(document.getElementById('adminUserCreateDisplayName') || {}).value = ''
          ;(document.getElementById('adminUserCreateStatus') || {}).value = 'active'
          await loadAdminUsers()
          setStatus(`管理员 ${username} 已创建`)
        } catch (err) {
          alert(err.message || '创建管理员失败')
        }
      }

      function startEditAdminUser(adminId) {
        const safeId = Number(adminId || 0)
        if (!safeId) {
          return
        }
        const user = adminUserItemsCache.find((x) => Number(x.admin_id || 0) === safeId) || null
        if (!user) {
          return
        }
        ;(document.getElementById('adminUserEditId') || {}).value = String(user.admin_id)
        ;(document.getElementById('adminUserEditUsername') || {}).value = user.username || ''
        ;(document.getElementById('adminUserEditDisplayName') || {}).value = user.display_name || ''
        ;(document.getElementById('adminUserEditRole') || {}).value = user.role_key || ''
        ;(document.getElementById('adminUserEditStatus') || {}).value = user.status || 'active'
        ;(document.getElementById('adminUserEditPassword') || {}).value = ''
      }

      async function updateAdminUser() {
        const adminId = Number((document.getElementById('adminUserEditId') || {}).value || '0')
        if (!adminId) {
          alert('请先从列表中选择一个管理员进行编辑')
          return
        }
        const payload = {
          display_name: String((document.getElementById('adminUserEditDisplayName') || {}).value || '').trim(),
          role_key: String((document.getElementById('adminUserEditRole') || {}).value || '').trim(),
          status: String((document.getElementById('adminUserEditStatus') || {}).value || 'active').trim()
        }
        const password = String((document.getElementById('adminUserEditPassword') || {}).value || '')
        if (password.trim()) {
          payload.password = password
        }
        if (!payload.role_key) {
          alert('岗位不能为空')
          return
        }
        try {
          await apiPut(`/api/admin/admin-users/${encodeURIComponent(adminId)}`, payload)
          ;(document.getElementById('adminUserEditPassword') || {}).value = ''
          await Promise.all([loadAdminUsers(), loadAdminAuthMe(true)])
          setStatus(`管理员 #${adminId} 已更新`)
        } catch (err) {
          alert(err.message || '更新管理员失败')
        }
      }

      function renderAdminSettings(ret) {
        const payload = ret || {}
        const settings = payload.settings || {}
        const general = settings.general || {}
        const api = settings.api || {}
        const map = [
          ['settingSiteName', general.site_name || ''],
          ['settingSiteSubtitle', general.site_subtitle || ''],
          ['settingContactEmail', general.contact_email || ''],
          ['settingContactWechat', general.contact_wechat || ''],
          ['settingAnnouncement', general.announcement || ''],
          ['settingApiBaseUrl', api.api_base_url || ''],
          ['settingMediaBaseUrl', api.media_base_url || ''],
          ['settingPaymentMode', api.payment_mode || 'mock'],
          ['settingRequestTimeout', String(Number(api.request_timeout_ms || 8000))]
        ]
        map.forEach(([id, value]) => {
          const el = document.getElementById(id)
          if (el) {
            el.value = value
          }
        })
        const loginEl = document.getElementById('settingWechatLoginEnabled')
        if (loginEl) {
          loginEl.value = api.wechat_login_enabled ? '1' : '0'
        }
        const updatedEl = document.getElementById('settingUpdatedAt')
        if (updatedEl) {
          updatedEl.innerText = payload.updated_at ? `最后更新：${payload.updated_at}` : ''
        }
      }

      async function loadAdminSettings() {
        const ret = await apiGet('/api/admin/settings')
        renderAdminSettings(ret || {})
        await Promise.all([loadAdminRoles(), loadAdminUsers()])
      }

      async function saveAdminSettings() {
        const payload = {
          general: {
            site_name: String((document.getElementById('settingSiteName') || {}).value || '').trim(),
            site_subtitle: String((document.getElementById('settingSiteSubtitle') || {}).value || '').trim(),
            contact_email: String((document.getElementById('settingContactEmail') || {}).value || '').trim(),
            contact_wechat: String((document.getElementById('settingContactWechat') || {}).value || '').trim(),
            announcement: String((document.getElementById('settingAnnouncement') || {}).value || '').trim()
          },
          api: {
            api_base_url: String((document.getElementById('settingApiBaseUrl') || {}).value || '').trim(),
            media_base_url: String((document.getElementById('settingMediaBaseUrl') || {}).value || '').trim(),
            wechat_login_enabled: String((document.getElementById('settingWechatLoginEnabled') || {}).value || '1') === '1',
            payment_mode: String((document.getElementById('settingPaymentMode') || {}).value || 'mock').trim(),
            request_timeout_ms: Number((document.getElementById('settingRequestTimeout') || {}).value || '8000')
          }
        }
        if (!payload.general.site_name) {
          alert('站点名称不能为空')
          return
        }
        try {
          const ret = await apiPut('/api/admin/settings', payload)
          renderAdminSettings(ret || {})
          setStatus('设置已保存')
        } catch (err) {
          alert(err.message || '设置保存失败')
        }
      }

      function renderProjectUploadHints() {
        const coverEl = document.getElementById('projectCoverImage')
        const galleryEl = document.getElementById('projectGalleryImages')
        const coverHintEl = document.getElementById('projectCoverUploadHint')
        const galleryHintEl = document.getElementById('projectGalleryUploadHint')
        if (coverHintEl) {
          const cover = String((coverEl || {}).value || '').trim()
          if (cover) {
            const href = toAbsoluteMediaUrl(cover)
            coverHintEl.innerHTML = `主图已配置：<a class="feedback-img-link" href="${href}" target="_blank" rel="noopener noreferrer">查看主图</a>`
          } else {
            coverHintEl.innerHTML = ''
          }
        }
        if (galleryHintEl) {
          const galleryLines = String((galleryEl || {}).value || '')
            .split('\n')
            .map((x) => x.trim())
            .filter(Boolean)
          if (!galleryLines.length) {
            galleryHintEl.innerHTML = ''
          } else {
            const links = galleryLines
              .slice(0, 5)
              .map((url, idx) => {
                const href = toAbsoluteMediaUrl(url)
                return `<a class="feedback-img-link" href="${href}" target="_blank" rel="noopener noreferrer">图${idx + 1}</a>`
              })
              .join(' / ')
            galleryHintEl.innerHTML = `图集共 ${galleryLines.length} 张：${links}${galleryLines.length > 5 ? ' ...' : ''}`
          }
        }
      }

      async function uploadProjectCoverImage() {
        const inputEl = document.getElementById('projectCoverFile')
        const coverEl = document.getElementById('projectCoverImage')
        const file = inputEl && inputEl.files ? inputEl.files[0] : null
        if (!file) {
          alert('请先选择主图文件')
          return
        }
        try {
          setStatus('主图上传中...')
          const ret = await apiUploadAdminImage(file, 'project')
          if (coverEl) {
            coverEl.value = String(ret.url || ret.absolute_url || '').trim()
          }
          if (inputEl) {
            inputEl.value = ''
          }
          renderProjectUploadHints()
          setStatus('主图上传成功')
        } catch (err) {
          alert(err.message || '主图上传失败')
          setStatus('主图上传失败')
        }
      }

      async function uploadProjectGalleryImages() {
        const inputEl = document.getElementById('projectGalleryFiles')
        const galleryEl = document.getElementById('projectGalleryImages')
        const files = Array.from((inputEl && inputEl.files) || [])
        if (!files.length) {
          alert('请先选择图集文件')
          return
        }
        if (!galleryEl) {
          alert('图集输入框不存在')
          return
        }
        const existing = String(galleryEl.value || '')
          .split('\n')
          .map((x) => x.trim())
          .filter(Boolean)
        const uploaded = []
        try {
          for (let i = 0; i < files.length; i += 1) {
            setStatus(`图集上传中 (${i + 1}/${files.length})...`)
            const ret = await apiUploadAdminImage(files[i], 'project')
            const url = String(ret.url || ret.absolute_url || '').trim()
            if (url) {
              uploaded.push(url)
            }
          }
          const merged = Array.from(new Set(existing.concat(uploaded))).slice(0, 12)
          galleryEl.value = merged.join('\n')
          if (inputEl) {
            inputEl.value = ''
          }
          renderProjectUploadHints()
          setStatus(`图集上传成功（新增 ${uploaded.length} 张）`)
        } catch (err) {
          alert(err.message || '图集上传失败')
          setStatus('图集上传失败')
        }
      }

      function formatProjectSpecsLines(specs) {
        const list = Array.isArray(specs) ? specs : []
        return list
          .map((it) => {
            const label = String((it && it.label) || '').trim()
            const value = String((it && it.value) || '').trim()
            if (!label && !value) {
              return ''
            }
            return `${label}|${value}`
          })
          .filter(Boolean)
          .join('\n')
      }

      function parseProjectSpecsLines(text) {
        const lines = String(text || '')
          .split('\n')
          .map((x) => x.trim())
          .filter(Boolean)
        const list = []
        for (let i = 0; i < lines.length; i += 1) {
          const line = lines[i]
          const parts = line.split('|')
          if (parts.length < 2) {
            throw new Error(`参数信息第 ${i + 1} 行格式错误，请使用 参数名|参数值`)
          }
          const label = String(parts[0] || '').trim()
          const value = String(parts.slice(1).join('|') || '').trim()
          if (!label || !value) {
            throw new Error(`参数信息第 ${i + 1} 行不能为空`)
          }
          list.push({ label, value })
        }
        return list
      }

      function formatProjectSkuLines(skuList) {
        const list = Array.isArray(skuList) ? skuList : []
        return list
          .map((sku) => {
            const id = String((sku && sku.id) || '').trim()
            const name = String((sku && sku.name) || '').trim()
            const price = Number((sku && sku.price) || 0)
            const deposit = Number((sku && sku.deposit) || 0)
            const stock = Number((sku && sku.stock) || 0)
            const perks = Array.isArray(sku && sku.perks)
              ? sku.perks
                  .map((x) => String(x || '').trim())
                  .filter(Boolean)
                  .join('/')
              : ''
            if (!id && !name) {
              return ''
            }
            return `${id}|${name}|${price}|${deposit}|${stock}|${perks}`
          })
          .filter(Boolean)
          .join('\n')
      }

      function parseProjectSkuLines(text) {
        const lines = String(text || '')
          .split('\n')
          .map((x) => x.trim())
          .filter(Boolean)
        const list = []
        for (let i = 0; i < lines.length; i += 1) {
          const line = lines[i]
          const parts = line.split('|')
          if (parts.length < 6) {
            throw new Error(`版本与权益第 ${i + 1} 行格式错误，请使用 sku_id|名称|价格|定金|库存|权益1/权益2`)
          }
          const id = String(parts[0] || '').trim()
          const name = String(parts[1] || '').trim()
          const price = Number(parts[2] || 0)
          const deposit = Number(parts[3] || 0)
          const stock = Number(parts[4] || 0)
          const perks = String(parts.slice(5).join('|') || '')
            .split('/')
            .map((x) => x.trim())
            .filter(Boolean)
          if (!id || !name) {
            throw new Error(`版本与权益第 ${i + 1} 行需填写 sku_id 和名称`)
          }
          if (!Number.isFinite(price) || price < 0) {
            throw new Error(`版本与权益第 ${i + 1} 行价格不合法`)
          }
          if (!Number.isFinite(deposit) || deposit < 0) {
            throw new Error(`版本与权益第 ${i + 1} 行定金不合法`)
          }
          if (!Number.isFinite(stock) || stock < 0) {
            throw new Error(`版本与权益第 ${i + 1} 行库存不合法`)
          }
          list.push({
            id,
            name,
            price: Math.round(price),
            deposit: Math.round(deposit),
            stock: Math.round(stock),
            perks
          })
        }
        return list
      }

      function collectProjectFilters() {
        return {
          keyword: (document.getElementById('projectKeyword') || {}).value || '',
          sale_mode: (document.getElementById('projectModeFilter') || {}).value || '',
          is_current: (document.getElementById('projectCurrentFilter') || {}).value || ''
        }
      }

      function normalizeProjectDesignerOptions(items) {
        const list = Array.isArray(items) ? items : []
        const mapped = list
          .map((it) => {
            const openid = String(it.openid || '').trim()
            if (!openid) {
              return null
            }
            return {
              openid,
              displayName: String(it.display_name || it.nickname || '').trim() || openid,
              status: String(it.status || '').trim() || 'active',
              statusText: String(it.status_text || '').trim() || String(it.status || '').trim() || '-',
              defaultShareRatio: Number(it.default_share_ratio || 0.15)
            }
          })
          .filter(Boolean)
        const uniqMap = new Map()
        mapped.forEach((it) => {
          if (!uniqMap.has(it.openid)) {
            uniqMap.set(it.openid, it)
          }
        })
        return Array.from(uniqMap.values())
      }

      function renderProjectDesignerOptions(items) {
        const selectEl = document.getElementById('projectDesignerOpenid')
        if (!selectEl) {
          return
        }
        const normalized = normalizeProjectDesignerOptions(items)
        if (normalized.length) {
          projectDesignerOptions = normalized.slice()
        }
        const currentValue = String(selectEl.value || '').trim()
        const options = ['<option value="">请选择绑定设计师（创建时必填）</option>']
          .concat(
            projectDesignerOptions.map(
              (it) =>
                `<option value="${it.openid}">${it.displayName} · ${it.openid}${it.status !== 'active' ? ` [${it.statusText}]` : ''}</option>`
            )
          )
          .join('')
        selectEl.innerHTML = options
        if (currentValue && projectDesignerOptions.some((x) => x.openid === currentValue)) {
          selectEl.value = currentValue
        }
      }

      function handleProjectDesignerChange() {
        const selectEl = document.getElementById('projectDesignerOpenid')
        const shareEl = document.getElementById('projectDesignerShare')
        if (!selectEl || !shareEl) {
          return
        }
        const selected = projectDesignerOptions.find((x) => x.openid === String(selectEl.value || '').trim()) || null
        if (!selected) {
          return
        }
        const nextRatio = Number(selected.defaultShareRatio || 0.15)
        if (nextRatio > 0 && nextRatio <= 1) {
          shareEl.value = String(nextRatio)
        }
      }

      function resetProjectForm() {
        currentProjectEditId = ''
        const idEl = document.getElementById('projectWorkId')
        const nameEl = document.getElementById('projectName')
        const subtitleEl = document.getElementById('projectSubtitle')
        const modeEl = document.getElementById('projectSaleMode')
        const goalEl = document.getElementById('projectGoal')
        const deadlineEl = document.getElementById('projectDeadline')
        const coverEl = document.getElementById('projectCoverImage')
        const galleryEl = document.getElementById('projectGalleryImages')
        const storyEl = document.getElementById('projectStory')
        const highlightsEl = document.getElementById('projectHighlightsText')
        const specsEl = document.getElementById('projectSpecsText')
        const skuEl = document.getElementById('projectSkuText')
        const coverFileEl = document.getElementById('projectCoverFile')
        const galleryFileEl = document.getElementById('projectGalleryFiles')
        const designerOpenidEl = document.getElementById('projectDesignerOpenid')
        const designerShareEl = document.getElementById('projectDesignerShare')
        const currentEl = document.getElementById('projectSetCurrent')
        if (idEl) {
          idEl.value = ''
          idEl.disabled = false
        }
        if (nameEl) nameEl.value = ''
        if (subtitleEl) subtitleEl.value = ''
        if (modeEl) modeEl.value = 'preorder'
        if (goalEl) goalEl.value = '0'
        if (deadlineEl) deadlineEl.value = ''
        if (coverEl) coverEl.value = ''
        if (galleryEl) galleryEl.value = ''
        if (storyEl) storyEl.value = ''
        if (highlightsEl) highlightsEl.value = ''
        if (specsEl) specsEl.value = ''
        if (skuEl) skuEl.value = ''
        if (coverFileEl) coverFileEl.value = ''
        if (galleryFileEl) galleryFileEl.value = ''
        if (designerOpenidEl) {
          designerOpenidEl.value = ''
          renderProjectDesignerOptions(projectDesignerOptions)
        }
        if (designerShareEl) designerShareEl.value = '0.15'
        if (currentEl) currentEl.checked = false
        const saveBtn = document.getElementById('projectSaveBtn')
        if (saveBtn) {
          saveBtn.innerText = '创建项目'
        }
        renderProjectUploadHints()
      }

      function applyProjectToForm(item) {
        if (!item) return
        currentProjectEditId = String(item.work_id || '')
        const idEl = document.getElementById('projectWorkId')
        const nameEl = document.getElementById('projectName')
        const subtitleEl = document.getElementById('projectSubtitle')
        const modeEl = document.getElementById('projectSaleMode')
        const goalEl = document.getElementById('projectGoal')
        const deadlineEl = document.getElementById('projectDeadline')
        const coverEl = document.getElementById('projectCoverImage')
        const galleryEl = document.getElementById('projectGalleryImages')
        const storyEl = document.getElementById('projectStory')
        const highlightsEl = document.getElementById('projectHighlightsText')
        const specsEl = document.getElementById('projectSpecsText')
        const skuEl = document.getElementById('projectSkuText')
        const designerOpenidEl = document.getElementById('projectDesignerOpenid')
        const designerShareEl = document.getElementById('projectDesignerShare')
        const currentEl = document.getElementById('projectSetCurrent')
        if (idEl) {
          idEl.value = item.work_id || ''
          idEl.disabled = true
        }
        if (nameEl) nameEl.value = item.name || ''
        if (subtitleEl) subtitleEl.value = item.subtitle || ''
        if (modeEl) modeEl.value = item.sale_mode || 'preorder'
        if (goalEl) goalEl.value = Number(item.crowdfunding_goal_amount || 0)
        if (deadlineEl) deadlineEl.value = item.crowdfunding_deadline || ''
        if (coverEl) coverEl.value = item.cover_image || ''
        if (galleryEl) galleryEl.value = (item.gallery_images || []).join('\n')
        if (storyEl) storyEl.value = item.story || ''
        if (highlightsEl) highlightsEl.value = (item.highlights || []).join('\n')
        if (specsEl) specsEl.value = formatProjectSpecsLines(item.specs || [])
        if (skuEl) skuEl.value = formatProjectSkuLines(item.sku_list || [])
        const firstDesigner = (item.designers || [])[0] || {}
        if (designerOpenidEl) {
          if (firstDesigner.openid && !projectDesignerOptions.some((x) => x.openid === firstDesigner.openid)) {
            projectDesignerOptions = projectDesignerOptions.concat([
              {
                openid: String(firstDesigner.openid),
                displayName: String(firstDesigner.display_name || firstDesigner.openid || ''),
                status: 'active',
                statusText: '已绑定',
                defaultShareRatio: Number(firstDesigner.share_ratio || 0.15)
              }
            ])
          }
          renderProjectDesignerOptions(projectDesignerOptions)
          designerOpenidEl.value = firstDesigner.openid || ''
        }
        if (designerShareEl) designerShareEl.value = String(firstDesigner.share_ratio || 0.15)
        if (currentEl) currentEl.checked = Boolean(item.is_current)
        const saveBtn = document.getElementById('projectSaveBtn')
        if (saveBtn) {
          saveBtn.innerText = `保存项目（${item.work_id || ''}）`
        }
        renderProjectUploadHints()
      }

      function renderProjects(items, summary) {
        const rowsEl = document.getElementById('projectRows')
        const sumEl = document.getElementById('projectSummary')
        if (!rowsEl || !sumEl) {
          return
        }
        const sum = summary || {}
        sumEl.innerText = `项目 ${sum.total || 0} 个｜预售 ${sum.preorder_count || 0}｜众筹 ${sum.crowdfunding_count || 0}｜当前 ${sum.current_work_name || '-'} (${sum.current_work_id || '-'})`
        rowsEl.innerHTML = (items || [])
          .map((it) => {
            const modeText = it.sale_mode_text || (it.sale_mode === 'crowdfunding' ? '众筹' : '预售')
            const designerText = (it.designers || [])
              .map((d) => `${d.display_name || d.openid || '设计师'}(${d.share_percent || 0}%)`)
              .join(' / ')
            const crowdText =
              it.sale_mode === 'crowdfunding'
                ? `目标￥${it.crowdfunding_goal_amount || 0}｜已筹￥${((it.funding || {}).funded_amount || 0)}｜进度 ${((it.funding || {}).progress_percent || 0)}%`
                : `预售成交：${((it.preorder_stats || {}).paid_orders || 0)} 单 / ￥${((it.preorder_stats || {}).paid_amount || 0)}`
            return `<tr>
              <td>${it.is_current ? '<span class="tag">当前项目</span>' : '-'}</td>
              <td>${it.work_id || '-'}</td>
              <td>${it.name || '-'}<br/><span class="muted">${it.subtitle || '-'}</span><br/>${
                it.cover_image ? `<a class="feedback-img-link" href="${it.cover_image}" target="_blank">主图</a>` : '<span class="muted">未配置主图</span>'
              }<br/><span class="muted">设计师：${designerText || '-'}</span></td>
              <td>${modeText}</td>
              <td>${crowdText}<br/><span class="muted">状态：${it.crowdfunding_status_text || '-'}</span></td>
              <td>${it.updated_at || '-'}</td>
              <td>
                <button class="btn-small" onclick="editProject('${encodeURIComponent(it.work_id || '')}')">编辑</button>
                <button class="btn-small" onclick="setCurrentProject('${encodeURIComponent(it.work_id || '')}')">设为当前</button>
              </td>
            </tr>`
          })
          .join('')
      }

      async function loadProjects() {
        const f = collectProjectFilters()
        const params = new URLSearchParams()
        params.set('limit', '300')
        if (String(f.keyword || '').trim()) {
          params.set('keyword', String(f.keyword || '').trim())
        }
        if (String(f.sale_mode || '').trim()) {
          params.set('sale_mode', String(f.sale_mode || '').trim())
        }
        if (String(f.is_current || '').trim() !== '') {
          params.set('is_current', String(f.is_current || '').trim())
        }
        const ret = await apiGet(`/api/admin/projects?${params.toString()}`)
        renderProjects(ret.items || [], ret.summary || {})
      }

      async function loadProjectDesignerOptions() {
        const selectEl = document.getElementById('projectDesignerOpenid')
        if (!selectEl) {
          return
        }
        try {
          const ret = await apiGet('/api/admin/projects/designers/options?limit=500&active_only=1')
          renderProjectDesignerOptions(ret.items || [])
        } catch (err) {
          // keep current selector options if loading fails
        }
      }

      function renderProjectReservations(items) {
        const rowsEl = document.getElementById('projectReservationRows')
        const sumEl = document.getElementById('projectReservationSummary')
        if (!rowsEl || !sumEl) {
          return
        }
        const list = items || []
        const countByWork = {}
        list.forEach((it) => {
          const key = String(it.work_id || '')
          if (!key) return
          countByWork[key] = (countByWork[key] || 0) + 1
        })
        const summaryParts = Object.keys(countByWork)
          .sort()
          .map((k) => `${k}: ${countByWork[k]} 人`)
        sumEl.innerText = `预约总数 ${list.length} ｜ ${summaryParts.join(' ｜ ') || '无预约'}`
        rowsEl.innerHTML = list
          .map(
            (it) => `<tr>
              <td>${it.work_id || '-'}</td>
              <td>${it.openid || '-'}</td>
              <td>${it.nickname || '-'}</td>
              <td>${it.created_at || '-'}</td>
            </tr>`
          )
          .join('')
      }

      async function loadProjectReservations() {
        const filterWorkId = String((document.getElementById('projectReservationWorkId') || {}).value || '').trim()
        const ret = await apiGet('/api/admin/reservations?limit=2000')
        const list = (ret.items || []).filter((x) => {
          if (!filterWorkId) {
            return true
          }
          return String(x.work_id || '') === filterWorkId
        })
        renderProjectReservations(list)
      }

      async function editProject(encodedWorkId) {
        const workId = decodeURIComponent(String(encodedWorkId || ''))
        if (!workId) return
        try {
          const ret = await apiGet(`/api/admin/projects?keyword=${encodeURIComponent(workId)}&limit=20`)
          const target = (ret.items || []).find((x) => String(x.work_id || '') === workId)
          if (!target) {
            alert('未找到该项目')
            return
          }
          applyProjectToForm(target)
        } catch (err) {
          alert(err.message || '加载项目失败')
        }
      }

      async function saveProject() {
        const idEl = document.getElementById('projectWorkId')
        const nameEl = document.getElementById('projectName')
        const subtitleEl = document.getElementById('projectSubtitle')
        const modeEl = document.getElementById('projectSaleMode')
        const goalEl = document.getElementById('projectGoal')
        const deadlineEl = document.getElementById('projectDeadline')
        const coverEl = document.getElementById('projectCoverImage')
        const galleryEl = document.getElementById('projectGalleryImages')
        const storyEl = document.getElementById('projectStory')
        const highlightsEl = document.getElementById('projectHighlightsText')
        const specsEl = document.getElementById('projectSpecsText')
        const skuEl = document.getElementById('projectSkuText')
        const designerOpenidEl = document.getElementById('projectDesignerOpenid')
        const designerShareEl = document.getElementById('projectDesignerShare')
        const currentEl = document.getElementById('projectSetCurrent')
        const galleryLines = String((galleryEl || {}).value || '')
          .split('\n')
          .map((x) => x.trim())
          .filter(Boolean)
        const coverValue = String((coverEl || {}).value || '').trim()
        const storyValue = String((storyEl || {}).value || '').trim()
        const highlightsText = String((highlightsEl || {}).value || '').trim()
        const specsText = String((specsEl || {}).value || '').trim()
        const skuText = String((skuEl || {}).value || '').trim()
        const designerOpenid = String((designerOpenidEl || {}).value || '').trim()
        const designerShareValue = Number((designerShareEl || {}).value || '0.15')
        const payload = {
          name: String((nameEl || {}).value || '').trim(),
          subtitle: String((subtitleEl || {}).value || '').trim(),
          sale_mode: String((modeEl || {}).value || 'preorder').trim(),
          crowdfunding_goal_amount: Number((goalEl || {}).value || '0'),
          crowdfunding_deadline: String((deadlineEl || {}).value || '').trim(),
          is_current: Boolean((currentEl || {}).checked)
        }
        if (coverValue) {
          payload.cover_image = coverValue
        }
        if (galleryLines.length) {
          payload.gallery_images = galleryLines
        }
        if (storyValue) {
          payload.story = storyValue
        }
        if (highlightsText) {
          payload.highlights = highlightsText
            .split('\n')
            .map((x) => x.trim())
            .filter(Boolean)
            .slice(0, 20)
        }
        try {
          if (specsText) {
            payload.specs = parseProjectSpecsLines(specsText)
          }
          if (skuText) {
            payload.sku_list = parseProjectSkuLines(skuText)
          }
        } catch (parseErr) {
          alert(parseErr.message || '参数信息格式错误')
          return
        }
        if (designerOpenid) {
          if (!designerShareValue || designerShareValue <= 0 || designerShareValue > 1) {
            alert('设计师分成比例需在 0-1 之间')
            return
          }
          payload.designer_openid = designerOpenid
          payload.designer_share_ratio = designerShareValue
        }
        if (!payload.name) {
          alert('项目名称不能为空')
          return
        }
        if (!currentProjectEditId && !designerOpenid) {
          alert('创建项目请先选择绑定设计师')
          return
        }
        if (payload.sale_mode === 'crowdfunding' && payload.crowdfunding_goal_amount <= 0) {
          alert('众筹模式下目标金额必须大于0')
          return
        }
        const workId = String((idEl || {}).value || '').trim()
        try {
          if (!currentProjectEditId) {
            if (!workId) {
              alert('请先填写项目ID')
              return
            }
            await apiPost('/api/admin/projects', {
              work_id: workId,
              ...payload
            })
            setStatus(`项目 ${workId} 已创建`)
          } else {
            await apiPut(`/api/admin/projects/${encodeURIComponent(currentProjectEditId)}`, payload)
            setStatus(`项目 ${currentProjectEditId} 已更新`)
          }
          await Promise.all([loadProjects(), loadProjectReservations(), loadDashboard(), loadCurrentWork()])
          resetProjectForm()
        } catch (err) {
          alert(err.message || '保存项目失败')
        }
      }

      async function setCurrentProject(encodedWorkId) {
        const workId = decodeURIComponent(String(encodedWorkId || ''))
        if (!workId) return
        if (!confirm(`确认将项目 ${workId} 设为当前在售项目吗？`)) {
          return
        }
        try {
          await apiPost(`/api/admin/projects/${encodeURIComponent(workId)}/set-current`, {})
          await Promise.all([loadProjects(), loadProjectReservations(), loadDashboard(), loadCurrentWork()])
          setStatus(`当前项目已切换为 ${workId}`)
        } catch (err) {
          alert(err.message || '切换当前项目失败')
        }
      }

      async function initiateCrowdfundingRefunds() {
        const limit = Number(document.getElementById('refundLimitInput').value || '50')
        const reason = document.getElementById('refundReasonInput').value.trim()
        if (!confirm(`确认提交退款？将尝试处理最多 ${limit} 笔待退款众筹订单。`)) {
          return
        }
        try {
          const ret = await apiPost('/api/admin/refunds/crowdfunding/initiate', {
            limit,
            reason
          })
          await Promise.all([loadDashboard(), loadOrders()])
          setStatus(`退款提交完成：成功 ${ret.success || 0} 笔，失败 ${ret.failed || 0} 笔`)
          if ((ret.failed || 0) > 0) {
            const failedItems = (ret.items || []).filter((it) => !it.ok).slice(0, 3)
            const msg = failedItems.map((it) => `${it.order_id || '-'}: ${it.error || '失败'}`).join('\n')
            alert(`部分退款提交失败（仅展示前3条）：\n${msg}`)
          }
        } catch (err) {
          alert(err.message)
        }
      }

      async function reviewSubmission(id, status) {
        try {
          const note = status === 'rejected' ? prompt('请输入驳回说明') || '' : ''
          await apiPost(`/api/admin/submissions/${id}/review`, { status, note })
          await loadDashboard()
          await loadSubmissions()
        } catch (err) {
          alert(err.message)
        }
      }

      async function activateDesignerFromSubmission(id) {
        if (!confirm(`确认将投稿 ${id} 对应用户开通为设计师吗？`)) {
          return
        }
        try {
          const ret = await apiPost(`/api/admin/submissions/${id}/activate-designer`, {})
          await Promise.all([loadDashboard(), loadSubmissions()])
          const profile = ret.profile || {}
          const name = profile.display_name || '设计师'
          if (ret.created) {
            setStatus(`已开通设计师：${name}`)
          } else {
            setStatus(`该用户已是设计师，已刷新状态：${name}`)
          }
        } catch (err) {
          alert(err.message)
        }
      }

      async function assignDesigner() {
        if ((document.getElementById('adminRole').value || '').toLowerCase() === 'finance') {
          alert('财务角色无权执行分成绑定，请切换运营角色')
          return
        }
        const openid = document.getElementById('assignOpenid').value.trim()
        const workId = document.getElementById('assignWorkId').value.trim()
        const share = Number(document.getElementById('assignShare').value.trim() || '0.15')
        if (!openid || !workId) {
          alert('请填写 openid 和作品ID')
          return
        }
        if (!share || share <= 0 || share > 1) {
          alert('分成比例需在 0-1 之间')
          return
        }
        try {
          await apiPost('/api/admin/designers/assign', {
            openid,
            work_id: workId,
            share_ratio: share
          })
          await Promise.all([loadDesigners(), loadCommissions(), loadDashboard()])
          setStatus('分成绑定成功')
        } catch (err) {
          alert(err.message)
        }
      }

      async function settleCommission(recordId, status) {
        try {
          const note = status === 'settled' ? prompt('可选：填写结算备注') || '' : ''
          await apiPost(`/api/admin/commissions/${recordId}/settle`, {
            settlement_status: status,
            settlement_note: note
          })
          await Promise.all([loadCommissions(), loadDashboard()])
        } catch (err) {
          alert(err.message)
        }
      }

      async function batchSettleSelected(status) {
        const ids = Array.from(selectedCommissionIds)
        if (!ids.length) {
          alert('请先勾选分成记录')
          return
        }
        const note = status === 'settled' ? prompt('可选：填写结算备注') || '' : ''
        try {
          const ret = await apiPost('/api/admin/commissions/batch-settle', {
            settlement_status: status,
            settlement_note: note,
            record_ids: ids
          })
          selectedCommissionIds.clear()
          await Promise.all([loadCommissions(), loadDashboard()])
          setStatus(`批量处理完成：${ret.affected_count || 0} 条`)
        } catch (err) {
          alert(err.message)
        }
      }

      async function batchSettleByFilter(status) {
        const fromStatus = document.getElementById('commissionStatusFilter').value
        if (!fromStatus) {
          alert('请先选择筛选状态（未结算/已结算）')
          return
        }
        if (!confirm(`确认将当前筛选（${fromStatus}）批量更新为 ${status} 吗？`)) {
          return
        }
        const note = status === 'settled' ? prompt('可选：填写结算备注') || '' : ''
        try {
          const ret = await apiPost('/api/admin/commissions/batch-settle', {
            settlement_status: status,
            settlement_note: note,
            from_status: fromStatus,
            limit: 2000
          })
          selectedCommissionIds.clear()
          await Promise.all([loadCommissions(), loadDashboard()])
          setStatus(`按筛选批量处理完成：${ret.affected_count || 0} 条`)
        } catch (err) {
          alert(err.message)
        }
      }

      async function loadDashboard() {
        const ret = await apiGet('/api/admin/dashboard')
        renderKpis(ret.dashboard || {})
      }

      function collectOrderFilters() {
        return {
          keyword: document.getElementById('orderKeyword').value.trim(),
          sale_mode: document.getElementById('orderSaleMode').value,
          pay_status: document.getElementById('orderPayStatus').value,
          refund_status: document.getElementById('orderRefundStatus').value,
          order_status: document.getElementById('orderStatusFilter').value
        }
      }

      function resetOrderFilters() {
        document.getElementById('orderKeyword').value = ''
        document.getElementById('orderSaleMode').value = ''
        document.getElementById('orderPayStatus').value = ''
        document.getElementById('orderRefundStatus').value = ''
        document.getElementById('orderStatusFilter').value = ''
        loadOrders()
      }

      async function loadOrders() {
        const f = collectOrderFilters()
        const params = new URLSearchParams()
        params.set('limit', '200')
        if (f.keyword) {
          params.set('keyword', f.keyword)
        }
        if (f.sale_mode) {
          params.set('sale_mode', f.sale_mode)
        }
        if (f.pay_status) {
          params.set('pay_status', f.pay_status)
        }
        if (f.refund_status) {
          params.set('refund_status', f.refund_status)
        }
        if (f.order_status) {
          params.set('order_status', f.order_status)
        }
        const ret = await apiGet(`/api/admin/orders?${params.toString()}`)
        renderOrders(ret.items || [], ret.summary || {})
        refreshOrderExportLink()
      }

      async function loadReservations() {
        const ret = await apiGet('/api/admin/reservations?limit=200')
        renderReservations(ret.items || [])
      }

      async function loadSubmissions() {
        const ret = await apiGet('/api/admin/submissions?limit=100')
        renderSubmissions(ret.items || [])
      }

      async function loadDesigners() {
        const ret = await apiGet('/api/admin/designers?limit=100')
        renderDesigners(ret.items || [])
        renderProjectDesignerOptions(ret.items || [])
      }

      async function loadUsers() {
        const keyword = document.getElementById('userKeyword').value.trim()
        const params = new URLSearchParams()
        params.set('limit', '300')
        if (keyword) {
          params.set('keyword', keyword)
        }
        const ret = await apiGet(`/api/admin/users?${params.toString()}`)
        renderUsers(ret.items || [], ret.summary || {})
      }

      async function loadCommissions() {
        const status = document.getElementById('commissionStatusFilter').value
        const qs = status ? `?status=${encodeURIComponent(status)}&limit=300` : '?limit=300'
        const ret = await apiGet(`/api/admin/commissions${qs}`)
        renderCommissions(ret.items || [])
        refreshExportLink()
      }

      async function loadCurrentWork() {
        try {
          await loadWorkConfig()
        } catch (err) {
          console.warn(err)
        }
      }

      function collectLoadTasksByRole() {
        const allowed = resolveAllowedModules()
        const tasks = []
        const can = (moduleName) => allowed.includes(moduleName) && currentAdminModule === moduleName
        if (can('overview')) {
          tasks.push(loadDashboard(), loadCurrentWork())
        }
        if (can('project')) {
          tasks.push(loadDashboard(), loadProjects(), loadProjectReservations(), loadProjectDesignerOptions())
        }
        if (can('order')) {
          tasks.push(loadOrders(), loadReservations())
        }
        if (can('submission')) {
          tasks.push(loadSubmissions())
        }
        if (can('designer')) {
          tasks.push(loadDesigners(), loadCommissions())
        }
        if (can('user')) {
          tasks.push(loadUsers())
        }
        if (can('feedback')) {
          tasks.push(loadFeedbackTemplates(), loadFeedbacks())
        }
        if (can('log')) {
          tasks.push(loadActionLogs())
        }
        if (can('setting')) {
          tasks.push(loadAdminSettings())
        }
        return tasks
      }

      async function loadAll() {
        try {
          setStatus('加载中...')
          if (getStoredAdminSession()) {
            await loadAdminAuthMe(true)
          } else {
            syncAdminIdentityToDom()
          }
          if (currentAdminModule === 'log') {
            restoreActionLogPrefs()
            applyActionLogSortFromUrl()
            syncActionLogSortIndicators()
          }
          await Promise.all(collectLoadTasksByRole())
          applyAdminModule()
          setStatus('已刷新')
        } catch (err) {
          setStatus('加载失败: ' + err.message)
        }
      }

      window.addEventListener('popstate', () => {
        currentAdminModule = inferAdminModuleFromPath()
        applyRoleModuleVisibility()
      })

      async function bootstrapAdminPage() {
        currentAdminModule = inferAdminModuleFromPath()
        setAdminModule(currentAdminModule, false)
        if (getStoredAdminSession()) {
          await loadAdminAuthMe(true)
        } else {
          syncAdminIdentityToDom()
        }
        await loadAll()
      }

      bootstrapAdminPage()
