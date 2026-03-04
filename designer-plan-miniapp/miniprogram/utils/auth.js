const { API_BASE_URL } = require('../config/env')

let loginPromise = null

function getSessionToken() {
  return wx.getStorageSync('kwc_session_token') || ''
}

function setSessionToken(token) {
  wx.setStorageSync('kwc_session_token', token)
}

function getCurrentUser() {
  return wx.getStorageSync('kwc_user') || null
}

function setCurrentUser(user) {
  wx.setStorageSync('kwc_user', user || null)
}

function clearSession() {
  wx.removeStorageSync('kwc_session_token')
  wx.removeStorageSync('kwc_user')
}

function login(force = false, nickname = '') {
  if (!force) {
    const token = getSessionToken()
    if (token) {
      return Promise.resolve({ token, user: getCurrentUser() })
    }
  }

  if (loginPromise) {
    return loginPromise
  }

  loginPromise = new Promise((resolve, reject) => {
    wx.login({
      success: (loginRes) => {
        const code = (loginRes && loginRes.code) || ''
        if (!code) {
          reject(new Error('wx.login 未返回 code'))
          loginPromise = null
          return
        }

        wx.request({
          url: `${API_BASE_URL}/api/auth/login`,
          method: 'POST',
          data: { code, nickname: (nickname || '').trim() },
          header: { 'content-type': 'application/json' },
          success: (res) => {
            const payload = res.data || {}
            if (res.statusCode >= 200 && res.statusCode < 300 && payload.session_token) {
              setSessionToken(payload.session_token)
              setCurrentUser(payload.user || null)
              resolve({
                token: payload.session_token,
                user: payload.user || null,
                isNewUser: Boolean(payload.is_new_user)
              })
            } else {
              reject(new Error(payload.detail || '登录失败'))
            }
            loginPromise = null
          },
          fail: (err) => {
            reject(new Error(err.errMsg || '登录失败'))
            loginPromise = null
          }
        })
      },
      fail: (err) => {
        reject(new Error(err.errMsg || '调用 wx.login 失败'))
        loginPromise = null
      }
    })
  })

  return loginPromise
}

function ensureLogin() {
  const token = getSessionToken()
  if (token) {
    return Promise.resolve({ token, user: getCurrentUser() })
  }
  return login(false)
}

module.exports = {
  login,
  ensureLogin,
  getSessionToken,
  getCurrentUser,
  setCurrentUser,
  clearSession
}
