const { request } = require('./request')
const { API_BASE_URL } = require('../config/env')
const { getSessionToken } = require('./auth')

function resolveUrl(path) {
  if (!path) {
    return ''
  }
  if (/^https?:\/\//.test(path)) {
    return path
  }
  return `${API_BASE_URL}${path}`
}

function getCurrentWork() {
  return request({ url: '/api/work/current' })
}

function getWorkUpdates(workId, limit = 20) {
  return request({ url: `/api/work/${encodeURIComponent(workId)}/updates?limit=${limit}` })
}

function getWorkComments(workId, limit = 50) {
  return request({ url: `/api/work/${encodeURIComponent(workId)}/comments?limit=${Number(limit || 50)}` })
}

function createWorkComment(workId, payload) {
  return request({
    url: `/api/work/${encodeURIComponent(workId)}/comments`,
    method: 'POST',
    data: payload || {}
  })
}

function reserveWork(workId) {
  return request({
    url: '/api/reservations',
    method: 'POST',
    data: { work_id: workId }
  })
}

function getMySummary() {
  return request({ url: '/api/me/summary' })
}

function getMyProfile() {
  return request({ url: '/api/me/profile' })
}

function updateMyProfile(payload) {
  return request({
    url: '/api/me/profile',
    method: 'PUT',
    data: payload || {}
  })
}

function getMyFeedback(limit = 50) {
  return request({ url: `/api/me/feedback?limit=${Number(limit || 50)}` })
}

function submitFeedback(payload) {
  return request({
    url: '/api/me/feedback',
    method: 'POST',
    data: payload || {}
  })
}

function getMyOrders(params = {}) {
  const query = []
  const limit = Number(params.limit || 20)
  const offset = Number(params.offset || 0)
  query.push(`limit=${encodeURIComponent(limit)}`)
  query.push(`offset=${encodeURIComponent(offset)}`)
  if (params.status) {
    query.push(`status=${encodeURIComponent(params.status)}`)
  }
  if (params.sale_mode) {
    query.push(`sale_mode=${encodeURIComponent(params.sale_mode)}`)
  }
  if (params.period_days) {
    query.push(`period_days=${encodeURIComponent(Number(params.period_days))}`)
  }
  return request({ url: `/api/me/orders?${query.join('&')}` })
}

function createPreorder(payload) {
  return request({
    url: '/api/orders/preorder',
    method: 'POST',
    data: payload
  })
}

function confirmPayment(payload) {
  return request({
    url: '/api/payments/confirm',
    method: 'POST',
    data: payload
  })
}

function submitDesignerApplication(payload) {
  return request({
    url: '/api/submissions',
    method: 'POST',
    data: payload
  })
}

function enrollDesigner(payload) {
  return request({
    url: '/api/designer/enroll',
    method: 'POST',
    data: payload
  })
}

function getDesignerDashboard() {
  return request({ url: '/api/designer/me/dashboard' })
}

function getDesignerProfile(designerId) {
  const id = Number(designerId || 0)
  return request({ url: `/api/designers/${encodeURIComponent(id)}` })
}

function getDesignerOrders(limit = 100) {
  return request({ url: `/api/designer/me/orders?limit=${limit}` })
}

function getDesignerUpdates(limit = 50) {
  return request({ url: `/api/designer/me/updates?limit=${limit}` })
}

function createDesignerUpdate(payload) {
  return request({
    url: '/api/designer/me/updates',
    method: 'POST',
    data: payload
  })
}

function getDesignerProjects(limit = 100) {
  return request({ url: `/api/designer/me/projects?limit=${Number(limit || 100)}` })
}

function updateDesignerProject(workId, payload) {
  return request({
    url: `/api/designer/me/projects/${encodeURIComponent(workId)}`,
    method: 'PUT',
    data: payload || {}
  })
}

function getDesignerComments(params = {}) {
  const query = []
  const limit = Number(params.limit || 100)
  query.push(`limit=${encodeURIComponent(limit)}`)
  if (params.work_id) {
    query.push(`work_id=${encodeURIComponent(params.work_id)}`)
  }
  return request({ url: `/api/designer/me/comments?${query.join('&')}` })
}

function replyDesignerComment(commentId, payload) {
  return request({
    url: `/api/designer/me/comments/${encodeURIComponent(commentId)}/reply`,
    method: 'POST',
    data: payload || {}
  })
}

function updateDesignerProfile(payload) {
  return request({
    url: '/api/designer/me/profile',
    method: 'PUT',
    data: payload || {}
  })
}

function uploadImage(filePath) {
  return new Promise((resolve, reject) => {
    const token = getSessionToken()
    wx.uploadFile({
      url: `${API_BASE_URL}/api/uploads/image`,
      filePath,
      name: 'file',
      header: {
        'X-Session-Token': token
      },
      success: (res) => {
        try {
          const payload = JSON.parse(res.data || '{}')
          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve({
              url: payload.url,
              absoluteUrl: resolveUrl(payload.url)
            })
            return
          }
          reject(new Error(payload.detail || '上传失败'))
        } catch (error) {
          reject(new Error('上传返回格式错误'))
        }
      },
      fail: (err) => {
        reject(new Error(err.errMsg || '上传失败'))
      }
    })
  })
}

function getAdminDashboard(adminToken) {
  return request({
    url: '/api/admin/dashboard',
    header: { 'X-Admin-Token': adminToken }
  })
}

module.exports = {
  getCurrentWork,
  getWorkUpdates,
  getWorkComments,
  createWorkComment,
  reserveWork,
  getMySummary,
  getMyProfile,
  updateMyProfile,
  getMyFeedback,
  submitFeedback,
  getMyOrders,
  createPreorder,
  confirmPayment,
  submitDesignerApplication,
  enrollDesigner,
  getDesignerDashboard,
  getDesignerProfile,
  getDesignerOrders,
  getDesignerUpdates,
  createDesignerUpdate,
  getDesignerProjects,
  updateDesignerProject,
  getDesignerComments,
  replyDesignerComment,
  updateDesignerProfile,
  uploadImage,
  resolveUrl,
  getAdminDashboard
}
