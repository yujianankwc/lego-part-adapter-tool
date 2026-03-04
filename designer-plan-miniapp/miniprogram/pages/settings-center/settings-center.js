const { clearSession, login } = require('../../utils/auth')

const SETTINGS_KEY = 'kwc_miniapp_settings'

Page({
  data: {
    settings: {
      orderNotify: true,
      activityNotify: true
    }
  },

  onShow() {
    this.loadSettings()
  },

  loadSettings() {
    const raw = wx.getStorageSync(SETTINGS_KEY) || {}
    this.setData({
      settings: {
        orderNotify: raw.orderNotify !== false,
        activityNotify: raw.activityNotify !== false
      }
    })
  },

  saveSettings() {
    wx.setStorageSync(SETTINGS_KEY, this.data.settings)
  },

  handleOrderNotifyChange(event) {
    this.setData(
      { 'settings.orderNotify': Boolean(event.detail.value) },
      () => this.saveSettings()
    )
  },

  handleActivityNotifyChange(event) {
    this.setData(
      { 'settings.activityNotify': Boolean(event.detail.value) },
      () => this.saveSettings()
    )
  },

  handleClearCache() {
    wx.showModal({
      title: '清理缓存',
      content: '确认清理页面缓存和筛选记录？',
      success: (res) => {
        if (!res.confirm) {
          return
        }
        const keep = wx.getStorageSync('kwc_session_token')
        const user = wx.getStorageSync('kwc_user')
        wx.clearStorageSync()
        if (keep) {
          wx.setStorageSync('kwc_session_token', keep)
        }
        if (user) {
          wx.setStorageSync('kwc_user', user)
        }
        this.saveSettings()
        wx.showToast({ title: '已清理', icon: 'success' })
      }
    })
  },

  async handleRefreshLogin() {
    try {
      wx.showLoading({ title: '刷新中' })
      await login(true)
      wx.hideLoading()
      wx.showToast({ title: '登录已刷新', icon: 'success' })
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '刷新失败', icon: 'none' })
    }
  },

  handleLogout() {
    wx.showModal({
      title: '退出登录',
      content: '退出后将清除登录状态，可随时重新微信登录。',
      success: (res) => {
        if (!res.confirm) {
          return
        }
        clearSession()
        wx.showToast({ title: '已退出', icon: 'success' })
      }
    })
  }
})
