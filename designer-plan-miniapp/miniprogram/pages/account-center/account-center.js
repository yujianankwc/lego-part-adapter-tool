const api = require('../../utils/api')
const { ensureLogin, login, setCurrentUser, getCurrentUser } = require('../../utils/auth')

Page({
  data: {
    profile: {
      user_id: 0,
      openid: '',
      nickname: '',
      created_at: '',
      updated_at: '',
      registered: false
    },
    nicknameInput: ''
  },

  async onShow() {
    await this.loadProfile()
  },

  async loadProfile() {
    try {
      await ensureLogin()
      const ret = await api.getMyProfile()
      const profile = ret.profile || {}
      this.setData({
        profile,
        nicknameInput: profile.nickname || ''
      })
    } catch (error) {
      const user = getCurrentUser() || {}
      this.setData({
        profile: {
          user_id: Number(user.user_id || 0),
          openid: user.openid || '',
          nickname: user.nickname || '',
          created_at: '',
          updated_at: '',
          registered: Boolean((user.nickname || '').trim())
        },
        nicknameInput: user.nickname || ''
      })
      wx.showToast({ title: error.message || '账号信息加载失败', icon: 'none' })
    }
  },

  handleNicknameInput(event) {
    this.setData({ nicknameInput: (event.detail.value || '').trim() })
  },

  async handleWechatLogin() {
    try {
      wx.showLoading({ title: '登录中' })
      const ret = await login(true, this.data.nicknameInput || '')
      wx.hideLoading()
      if (ret && ret.user) {
        setCurrentUser(ret.user)
      }
      wx.showToast({
        title: ret && ret.isNewUser ? '登录成功，请完成注册' : '登录成功',
        icon: 'success'
      })
      await this.loadProfile()
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '登录失败', icon: 'none' })
    }
  },

  async handleSaveProfile() {
    const nickname = (this.data.nicknameInput || '').trim()
    if (!nickname) {
      wx.showToast({ title: '请输入昵称', icon: 'none' })
      return
    }
    try {
      wx.showLoading({ title: '保存中' })
      const ret = await api.updateMyProfile({ nickname })
      wx.hideLoading()
      const profile = ret.profile || {}
      const current = getCurrentUser() || {}
      setCurrentUser({
        ...current,
        user_id: profile.user_id || current.user_id,
        openid: profile.openid || current.openid,
        nickname: profile.nickname || nickname
      })
      this.setData({
        profile,
        nicknameInput: profile.nickname || nickname
      })
      wx.showToast({ title: profile.registered ? '资料已保存' : '注册完成', icon: 'success' })
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '保存失败', icon: 'none' })
    }
  }
})
