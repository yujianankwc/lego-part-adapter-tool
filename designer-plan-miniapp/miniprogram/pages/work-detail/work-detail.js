const { work } = require('../../utils/mock')
const api = require('../../utils/api')
const { ensureLogin } = require('../../utils/auth')
const { normalizeWork, isCrowdfunding } = require('../../utils/work')

Page({
  data: {
    work,
    reserved: false,
    isCrowdfundingMode: false,
    actionText: '立即预售',
    actionDisabled: false,
    actionHint: '',
    modeTag: '预售',
    updates: [],
    comments: [],
    commentInput: '',
    commentSubmitting: false,
    previewGallery: [],
    visualPanels: [
      { title: '主视角', desc: '鹤翼展开，形态上扬，视觉焦点集中于“破茧”姿态。' },
      { title: '侧视角', desc: '梅枝与山体形成穿插层次，强调东方园景的空间关系。' },
      { title: '俯视角', desc: '环形构图完整，平台边框稳定，适合作为桌面雕塑摆件。' }
    ]
  },

  async onShow() {
    await Promise.all([this.loadWork(), this.loadSummary()])
  },

  async loadWork() {
    try {
      const ret = await api.getCurrentWork()
      const nextWork = normalizeWork(ret.work)
      this.applyWorkData(nextWork)
      await this.loadWorkUpdates(nextWork.id)
      await this.loadWorkComments(nextWork.id)
    } catch (error) {
      const fallback = normalizeWork(work)
      this.applyWorkData(fallback)
      await this.loadWorkUpdates(fallback.id)
      await this.loadWorkComments(fallback.id)
    }
  },

  applyWorkData(nextWork) {
    const crowdfunding = isCrowdfunding(nextWork)
    const crowdfundingStatus = nextWork.crowdfundingStatus || 'active'
    const actionDisabled = crowdfunding && crowdfundingStatus !== 'active'
    let actionText = crowdfunding ? '立即支持众筹' : '立即预售'
    if (crowdfunding && crowdfundingStatus === 'producing') {
      actionText = '已达标生产中'
    } else if (crowdfunding && crowdfundingStatus === 'failed') {
      actionText = '众筹失败已退款'
    }
    const previewGallery = Array.isArray(nextWork.galleryImages) ? nextWork.galleryImages : []
    this.setData({
      work: nextWork,
      previewGallery,
      isCrowdfundingMode: crowdfunding,
      actionText,
      actionDisabled,
      actionHint: crowdfunding ? nextWork.crowdfundingStatusText || '' : '',
      modeTag: crowdfunding ? '众筹' : '预售'
    })
  },

  async loadSummary() {
    try {
      await ensureLogin()
      const summary = await api.getMySummary()
      this.setData({ reserved: Boolean(summary.reserved) })
    } catch (error) {
      wx.showToast({ title: error.message || '加载失败', icon: 'none' })
    }
  },

  async loadWorkUpdates(workId) {
    try {
      const ret = await api.getWorkUpdates(workId || this.data.work.id, 20)
      this.setData({ updates: ret.items || [] })
    } catch (error) {
      this.setData({ updates: [] })
    }
  },

  async loadWorkComments(workId) {
    try {
      const ret = await api.getWorkComments(workId || this.data.work.id, 100)
      this.setData({ comments: ret.items || [] })
    } catch (error) {
      this.setData({ comments: [] })
    }
  },

  handleCommentInput(event) {
    this.setData({ commentInput: event.detail.value || '' })
  },

  async handleSubmitComment() {
    if (this.data.commentSubmitting) {
      return
    }
    const content = String(this.data.commentInput || '').trim()
    if (!content) {
      wx.showToast({ title: '请输入评论内容', icon: 'none' })
      return
    }
    if (content.length > 300) {
      wx.showToast({ title: '评论不能超过300字', icon: 'none' })
      return
    }
    this.setData({ commentSubmitting: true })
    wx.showLoading({ title: '提交中' })
    try {
      await ensureLogin()
      await api.createWorkComment(this.data.work.id, { content })
      wx.hideLoading()
      wx.showToast({ title: '评论已发布', icon: 'success' })
      this.setData({ commentInput: '' })
      await this.loadWorkComments(this.data.work.id)
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '评论失败', icon: 'none' })
    } finally {
      this.setData({ commentSubmitting: false })
    }
  },

  async handleReserve() {
    if (this.data.reserved) {
      wx.showToast({ title: '你已预约', icon: 'none' })
      return
    }

    try {
      await ensureLogin()
      await api.reserveWork(this.data.work.id)
      this.setData({ reserved: true })
      wx.showToast({ title: '预约成功', icon: 'success' })
    } catch (error) {
      wx.showToast({ title: error.message || '预约失败', icon: 'none' })
    }
  },

  handlePreorder() {
    if (this.data.actionDisabled) {
      wx.showToast({ title: this.data.actionHint || '当前阶段不可下单', icon: 'none' })
      return
    }
    wx.switchTab({
      url: '/pages/preorder/preorder'
    })
  },

  handleOpenDesignerProfile(event) {
    const designerId = Number(event.currentTarget.dataset.designerId || 0)
    if (!designerId) {
      wx.showToast({ title: '设计师信息缺失', icon: 'none' })
      return
    }
    wx.navigateTo({
      url: `/pages/designer-profile/designer-profile?designerId=${designerId}`
    })
  }
})
