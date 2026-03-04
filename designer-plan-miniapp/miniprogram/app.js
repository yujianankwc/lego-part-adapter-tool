const { ensureLogin } = require('./utils/auth')

App({
  globalData: {
    loginReady: null
  },

  onLaunch() {
    this.globalData.loginReady = ensureLogin().catch((err) => {
      console.warn('登录初始化失败', err)
      return null
    })
  }
})
