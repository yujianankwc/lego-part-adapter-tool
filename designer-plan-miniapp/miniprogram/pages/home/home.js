const { work, program } = require('../../utils/mock')
const api = require('../../utils/api')
const { ensureLogin } = require('../../utils/auth')
const { normalizeWork, isCrowdfunding } = require('../../utils/work')

Page({
  data: {
    work,
    program,
    reserved: false,
    workSaleModeText: '预售',
    isCrowdfundingMode: false,
    heroImage: '',
    galleryPreview: [],
    countdown: {
      days: 7,
      hours: 20,
      minutes: 0
    }
  },

  async onShow() {
    await Promise.all([this.loadSummary(), this.loadWork()])
  },

  async loadWork() {
    try {
      const ret = await api.getCurrentWork()
      const nextWork = normalizeWork(ret.work)
      this.applyWorkData(nextWork)
    } catch (error) {
      const fallback = normalizeWork(work)
      this.applyWorkData(fallback)
    }
  },

  applyWorkData(nextWork) {
    const crowdfunding = isCrowdfunding(nextWork)
    const countdown = this.buildCountdown(nextWork.crowdfundingDeadline)
    const gallery = Array.isArray(nextWork.galleryImages) ? nextWork.galleryImages : []
    this.setData({
      work: nextWork,
      isCrowdfundingMode: crowdfunding,
      workSaleModeText: crowdfunding ? '众筹' : '预售',
      heroImage: nextWork.coverImage || gallery[0] || '',
      galleryPreview: gallery.slice(0, 4),
      countdown
    })
  },

  buildCountdown(deadline) {
    const raw = String(deadline || '').trim()
    if (!raw) {
      return { days: 7, hours: 20, minutes: 0 }
    }
    const parsed = raw.includes(' ') ? raw.replace(' ', 'T') : `${raw}T23:59:59`
    const target = new Date(parsed)
    const now = new Date()
    const diffMs = target.getTime() - now.getTime()
    if (!Number.isFinite(diffMs) || diffMs <= 0) {
      return { days: 0, hours: 0, minutes: 0 }
    }
    const totalMinutes = Math.floor(diffMs / 60000)
    const days = Math.floor(totalMinutes / (24 * 60))
    const hours = Math.floor((totalMinutes % (24 * 60)) / 60)
    const minutes = totalMinutes % 60
    return { days, hours, minutes }
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

  handleViewWork() {
    wx.navigateTo({
      url: '/pages/work-detail/work-detail'
    })
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

  handleGoApply() {
    wx.switchTab({
      url: '/pages/designer-apply/designer-apply'
    })
  }
})
