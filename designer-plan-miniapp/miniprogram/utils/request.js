const { API_BASE_URL } = require('../config/env')

function request({ url, method = 'GET', data = {}, header = {}, skipAuth = false }) {
  return new Promise((resolve, reject) => {
    const finalHeader = { 'content-type': 'application/json', ...header }
    if (!skipAuth) {
      const token = wx.getStorageSync('kwc_session_token')
      if (token) {
        finalHeader['X-Session-Token'] = token
      }
    }

    wx.request({
      url: `${API_BASE_URL}${url}`,
      method,
      data,
      header: finalHeader,
      success: (res) => {
        const payload = res.data || {}
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(payload)
          return
        }
        if (res.statusCode === 401) {
          wx.removeStorageSync('kwc_session_token')
        }
        reject(new Error(payload.detail || payload.message || '请求失败'))
      },
      fail: (err) => {
        reject(new Error(err.errMsg || '网络异常'))
      }
    })
  })
}

module.exports = {
  request
}
