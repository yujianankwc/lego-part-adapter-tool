const api = require('../../utils/api')
const { ensureLogin, getCurrentUser } = require('../../utils/auth')

Page({
  data: {
    reserved: false,
    orderOverview: {
      totalOrders: 0,
      paidOrders: 0,
      pendingOrders: 0,
      refundingOrders: 0,
      refundedOrders: 0,
      paidAmount: 0,
      refundAmount: 0,
      netAmount: 0,
      preorderOrders: 0,
      crowdfundingOrders: 0
    },
    submissions: [],
    designer: {
      isDesigner: false,
      profile: null,
      qualification: {
        hasApplied: false,
        canEnroll: false,
        totalSubmissions: 0,
        approvedSubmissions: 0,
        pendingSubmissions: 0,
        rejectedSubmissions: 0,
        latestStatus: '',
        latestStatusText: '',
        message: '请先提交设计师投稿申请'
      }
    },
    accountUser: {
      nickname: '',
      openid: ''
    }
  },

  async onShow() {
    await this.loadSummary()
  },

  async loadSummary() {
    try {
      await ensureLogin()
      const summary = await api.getMySummary()

      const submissions = (summary.submissions || []).map((item) => ({
        id: item.submission_id,
        workName: item.work_name,
        category: item.category,
        estimatedPieces: item.estimated_pieces,
        status: item.status || '',
        statusText: item.status_text || '',
        createdAt: item.created_at
      }))

      const fallbackQualification = this.buildDesignerQualification(submissions)
      const rawQualification = (summary.designer && summary.designer.qualification) || {}
      const designerQualification = {
        hasApplied:
          rawQualification.has_applied != null ? Boolean(rawQualification.has_applied) : fallbackQualification.hasApplied,
        canEnroll:
          rawQualification.can_enroll != null ? Boolean(rawQualification.can_enroll) : fallbackQualification.canEnroll,
        totalSubmissions:
          rawQualification.total_submissions != null
            ? Number(rawQualification.total_submissions)
            : fallbackQualification.totalSubmissions,
        approvedSubmissions:
          rawQualification.approved_submissions != null
            ? Number(rawQualification.approved_submissions)
            : fallbackQualification.approvedSubmissions,
        pendingSubmissions:
          rawQualification.pending_submissions != null
            ? Number(rawQualification.pending_submissions)
            : fallbackQualification.pendingSubmissions,
        rejectedSubmissions:
          rawQualification.rejected_submissions != null
            ? Number(rawQualification.rejected_submissions)
            : fallbackQualification.rejectedSubmissions,
        latestStatus: String(rawQualification.latest_status || fallbackQualification.latestStatus || ''),
        latestStatusText: String(rawQualification.latest_status_text || fallbackQualification.latestStatusText || ''),
        message: String(rawQualification.message || fallbackQualification.message || '')
      }

      const designer = {
        isDesigner: Boolean(summary.designer && summary.designer.is_designer),
        profile: summary.designer ? summary.designer.profile : null,
        qualification: designerQualification
      }

      const rawOverview = summary.order_overview || {}
      const orderOverview = {
        totalOrders: Number(rawOverview.total_orders || 0),
        paidOrders: Number(rawOverview.paid_orders || 0),
        pendingOrders: Number(rawOverview.pending_orders || 0),
        refundingOrders: Number(rawOverview.refunding_orders || 0),
        refundedOrders: Number(rawOverview.refunded_orders || 0),
        paidAmount: Number(rawOverview.paid_amount || 0),
        refundAmount: Number(rawOverview.refund_amount || 0),
        netAmount: Number(rawOverview.net_amount || Number(rawOverview.paid_amount || 0) - Number(rawOverview.refund_amount || 0)),
        preorderOrders: Number(rawOverview.preorder_orders || 0),
        crowdfundingOrders: Number(rawOverview.crowdfunding_orders || 0)
      }

      this.setData({
        reserved: Boolean(summary.reserved),
        orderOverview,
        submissions,
        designer,
        accountUser: {
          nickname: (getCurrentUser() || {}).nickname || '',
          openid: (getCurrentUser() || {}).openid || ''
        }
      })
    } catch (error) {
      wx.showToast({ title: error.message || '加载失败', icon: 'none' })
    }
  },

  buildDesignerQualification(submissions) {
    const list = submissions || []
    const totalSubmissions = list.length
    const approvedSubmissions = list.filter((item) => item.status === 'approved').length
    const pendingSubmissions = list.filter((item) => item.status === 'pending').length
    const rejectedSubmissions = list.filter((item) => item.status === 'rejected').length
    const hasApplied = totalSubmissions > 0
    const canEnroll = approvedSubmissions > 0
    const latest = list[0] || {}
    let message = '已满足开通设计师条件'
    if (!hasApplied) {
      message = '请先提交设计师投稿申请'
    } else if (!canEnroll) {
      message = '你的投稿尚未审核通过，请等待审核结果'
    }
    return {
      hasApplied,
      canEnroll,
      totalSubmissions,
      approvedSubmissions,
      pendingSubmissions,
      rejectedSubmissions,
      latestStatus: latest.status || '',
      latestStatusText: latest.statusText || '',
      message
    }
  },

  async handleActivateDesigner() {
    if (this.data.designer.isDesigner) {
      this.handleGoDesignerCenter()
      return
    }
    if (!this.data.designer.qualification.canEnroll) {
      wx.showToast({ title: this.data.designer.qualification.message || '暂不满足开通条件', icon: 'none' })
      return
    }
    try {
      await ensureLogin()
      wx.showLoading({ title: '开通中' })
      await api.enrollDesigner({})
      wx.hideLoading()
      wx.showToast({ title: '设计师入口已开通', icon: 'success' })
      await this.loadSummary()
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '开通失败', icon: 'none' })
    }
  },

  handleContact() {
    wx.showModal({
      title: '联系项目组',
      content: '请添加企业微信：KWC-DESIGNER',
      showCancel: false
    })
  },

  handleGoDesignerApply() {
    wx.navigateTo({
      url: '/pages/designer-apply/designer-apply'
    })
  },

  handleGoDesignerCenter() {
    if (!this.data.designer.isDesigner) {
      wx.showToast({ title: '请先完成设计师认证', icon: 'none' })
      return
    }
    wx.navigateTo({
      url: '/pages/designer-center/designer-center'
    })
  },

  handleGoAccountCenter() {
    wx.navigateTo({ url: '/pages/account-center/account-center' })
  },

  handleGoHelpCenter() {
    wx.navigateTo({ url: '/pages/help-center/help-center' })
  },

  handleGoFeedbackCenter() {
    wx.navigateTo({ url: '/pages/feedback-center/feedback-center' })
  },

  handleGoSettingsCenter() {
    wx.navigateTo({ url: '/pages/settings-center/settings-center' })
  },

  handleGoMyOrders() {
    wx.navigateTo({ url: '/pages/my-orders/my-orders' })
  }
})
