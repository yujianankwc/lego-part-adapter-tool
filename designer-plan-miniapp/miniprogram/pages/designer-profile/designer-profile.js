const api = require('../../utils/api')

Page({
  data: {
    designerId: 0,
    loading: false,
    profile: null,
    assignments: []
  },

  onLoad(options) {
    const designerId = Number((options && options.designerId) || 0)
    if (!designerId) {
      wx.showToast({ title: '设计师参数缺失', icon: 'none' })
      return
    }
    this.setData({ designerId })
    this.loadProfile()
  },

  async loadProfile() {
    if (!this.data.designerId) {
      return
    }
    this.setData({ loading: true })
    try {
      const ret = await api.getDesignerProfile(this.data.designerId)
      const profile = ret.profile || null
      this.setData({
        profile,
        assignments: ret.assignments || []
      })
      if (profile && profile.display_name) {
        wx.setNavigationBarTitle({ title: profile.display_name })
      }
    } catch (error) {
      wx.showToast({ title: error.message || '加载失败', icon: 'none' })
    } finally {
      this.setData({ loading: false })
      wx.stopPullDownRefresh()
    }
  },

  onPullDownRefresh() {
    this.loadProfile()
  }
})
