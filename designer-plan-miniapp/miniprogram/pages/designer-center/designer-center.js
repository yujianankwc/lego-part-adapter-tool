const api = require('../../utils/api')
const { ensureLogin } = require('../../utils/auth')

Page({
  data: {
    loading: false,
    profile: null,
    assignments: [],
    sales: {
      paid_orders_count: 0,
      paid_units: 0,
      total_sales_amount: 0,
      estimated_commission_amount: 0,
      pending_commission_amount: 0,
      settled_commission_amount: 0,
      by_work: []
    },
    orders: [],
    updates: [],
    projects: [],
    comments: [],
    draft: {
      workId: '',
      title: '',
      content: ''
    },
    projectDraft: {
      workId: '',
      name: '',
      subtitle: '',
      coverImage: '',
      story: '',
      galleryText: '',
      highlightsText: ''
    },
    profileDraft: {
      displayName: '',
      bio: '',
      avatarUrl: ''
    },
    commentReplyDraft: {},
    submitting: false,
    projectSaving: false,
    profileSaving: false,
    replyingCommentId: ''
  },

  async onShow() {
    await this.loadData()
  },

  async loadData() {
    this.setData({ loading: true })
    try {
      await ensureLogin()
      const dashboard = await api.getDesignerDashboard()
      const orderRet = await api.getDesignerOrders(50)
      const updatesRet = await api.getDesignerUpdates(20)
      const projectsRet = await api.getDesignerProjects(100)

      const assignments = dashboard.assignments || []
      const projects = projectsRet.items || []
      let workId = this.data.draft.workId || this.data.projectDraft.workId
      if (!workId && assignments.length > 0) {
        workId = assignments[0].work_id
      }
      if (!workId && projects.length > 0) {
        workId = projects[0].work_id
      }
      const currentProject = projects.find((x) => x.work_id === workId) || projects[0] || null
      const finalWorkId = currentProject ? currentProject.work_id : workId
      const commentsRet = finalWorkId ? await api.getDesignerComments({ work_id: finalWorkId, limit: 100 }) : { items: [] }

      this.setData({
        profile: dashboard.profile,
        assignments,
        sales: dashboard.sales || this.data.sales,
        orders: orderRet.items || [],
        updates: updatesRet.items || [],
        projects,
        comments: commentsRet.items || [],
        commentReplyDraft: {},
        draft: {
          ...this.data.draft,
          workId: finalWorkId
        },
        projectDraft: this.buildProjectDraft(currentProject, finalWorkId),
        profileDraft: {
          displayName: String((dashboard.profile || {}).display_name || ''),
          bio: String((dashboard.profile || {}).bio || ''),
          avatarUrl: String((dashboard.profile || {}).avatar_url || '')
        }
      })
    } catch (error) {
      if (this.isDesignerAccessDenied(error)) {
        this.handleDesignerAccessDenied(error)
        return
      }
      wx.showToast({ title: error.message || '加载失败', icon: 'none' })
    } finally {
      this.setData({ loading: false })
    }
  },

  isDesignerAccessDenied(error) {
    const msg = String((error && error.message) || '').toLowerCase()
    return msg.includes('未开通设计师') || msg.includes('403') || msg.includes('forbidden')
  },

  handleDesignerAccessDenied(error) {
    const detail = String((error && error.message) || '').trim() || '当前账号未开通设计师权限'
    wx.showModal({
      title: '暂未开通',
      content: `${detail}，请先在“我的”页面完成认证流程。`,
      showCancel: false,
      success: () => {
        wx.switchTab({ url: '/pages/profile/profile' })
      }
    })
  },

  buildProjectDraft(project, fallbackWorkId = '') {
    const target = project || {}
    const gallery = Array.isArray(target.gallery_images) ? target.gallery_images : []
    const highlights = Array.isArray(target.highlights) ? target.highlights : []
    return {
      workId: target.work_id || fallbackWorkId || '',
      name: target.name || '',
      subtitle: target.subtitle || '',
      coverImage: target.cover_image || '',
      story: target.story || '',
      galleryText: gallery.join('\n'),
      highlightsText: highlights.join('\n')
    }
  },

  async loadDesignerComments(workId) {
    const safeWorkId = String(workId || '').trim()
    if (!safeWorkId) {
      this.setData({ comments: [], commentReplyDraft: {} })
      return
    }
    try {
      const ret = await api.getDesignerComments({ work_id: safeWorkId, limit: 100 })
      this.setData({
        comments: ret.items || [],
        commentReplyDraft: {}
      })
    } catch (error) {
      this.setData({ comments: [], commentReplyDraft: {} })
    }
  },

  handleWorkChange(event) {
    const idx = Number(event.detail.value)
    const current = this.data.assignments[idx]
    if (!current) {
      return
    }
    const targetProject = (this.data.projects || []).find((x) => x.work_id === current.work_id) || null
    this.setData({
      'draft.workId': current.work_id,
      projectDraft: this.buildProjectDraft(targetProject, current.work_id)
    })
    this.loadDesignerComments(current.work_id)
  },

  handleInput(event) {
    const { field } = event.currentTarget.dataset
    this.setData({
      [`draft.${field}`]: event.detail.value
    })
  },

  handleProjectInput(event) {
    const { field } = event.currentTarget.dataset
    this.setData({
      [`projectDraft.${field}`]: event.detail.value
    })
  },

  handleProfileInput(event) {
    const { field } = event.currentTarget.dataset
    this.setData({
      [`profileDraft.${field}`]: event.detail.value
    })
  },

  async handleSaveProfile() {
    if (this.data.profileSaving) {
      return
    }
    const displayName = String(this.data.profileDraft.displayName || '').trim()
    const bio = String(this.data.profileDraft.bio || '').trim()
    const avatarUrl = String(this.data.profileDraft.avatarUrl || '').trim()
    if (!displayName) {
      wx.showToast({ title: '请填写设计师名称', icon: 'none' })
      return
    }
    this.setData({ profileSaving: true })
    wx.showLoading({ title: '保存中' })
    try {
      await api.updateDesignerProfile({
        display_name: displayName,
        bio,
        avatar_url: avatarUrl
      })
      wx.hideLoading()
      wx.showToast({ title: '个人介绍已更新', icon: 'success' })
      await this.loadData()
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '保存失败', icon: 'none' })
    } finally {
      this.setData({ profileSaving: false })
    }
  },

  async handleChooseAvatar() {
    try {
      const chooseRes = await new Promise((resolve, reject) => {
        wx.chooseImage({
          count: 1,
          sizeType: ['compressed'],
          sourceType: ['album', 'camera'],
          success: resolve,
          fail: reject
        })
      })
      const tempPath = ((chooseRes || {}).tempFilePaths || [])[0]
      if (!tempPath) {
        return
      }
      wx.showLoading({ title: '上传中' })
      const ret = await api.uploadImage(tempPath)
      wx.hideLoading()
      this.setData({
        'profileDraft.avatarUrl': String(ret.url || ret.absoluteUrl || '').trim()
      })
      wx.showToast({ title: '头像已上传', icon: 'success' })
    } catch (error) {
      wx.hideLoading()
      const msg = String((error && error.errMsg) || (error && error.message) || '')
      if (msg.includes('cancel')) {
        return
      }
      wx.showToast({ title: msg || '头像上传失败', icon: 'none' })
    }
  },

  async handleSaveProject() {
    if (this.data.projectSaving) {
      return
    }
    const workId = String(this.data.projectDraft.workId || '').trim()
    if (!workId) {
      wx.showToast({ title: '请先选择作品', icon: 'none' })
      return
    }
    const name = String(this.data.projectDraft.name || '').trim()
    const subtitle = String(this.data.projectDraft.subtitle || '').trim()
    const coverImage = String(this.data.projectDraft.coverImage || '').trim()
    const story = String(this.data.projectDraft.story || '').trim()
    const galleryImages = String(this.data.projectDraft.galleryText || '')
      .split('\n')
      .map((x) => x.trim())
      .filter(Boolean)
      .slice(0, 12)
    const highlights = String(this.data.projectDraft.highlightsText || '')
      .split('\n')
      .map((x) => x.trim())
      .filter(Boolean)
      .slice(0, 12)
    if (!name) {
      wx.showToast({ title: '项目名称不能为空', icon: 'none' })
      return
    }
    if (!story) {
      wx.showToast({ title: '项目故事不能为空', icon: 'none' })
      return
    }

    this.setData({ projectSaving: true })
    wx.showLoading({ title: '保存中' })
    try {
      await api.updateDesignerProject(workId, {
        name,
        subtitle,
        cover_image: coverImage,
        story,
        gallery_images: galleryImages,
        highlights
      })
      wx.hideLoading()
      wx.showToast({ title: '项目信息已更新', icon: 'success' })
      await this.loadData()
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '保存失败', icon: 'none' })
    } finally {
      this.setData({ projectSaving: false })
    }
  },

  async handlePublishUpdate() {
    if (this.data.submitting) {
      return
    }

    const workId = (this.data.draft.workId || '').trim()
    const title = (this.data.draft.title || '').trim()
    const content = (this.data.draft.content || '').trim()
    if (!workId || !title || !content) {
      wx.showToast({ title: '请完整填写作品、标题、内容', icon: 'none' })
      return
    }

    this.setData({ submitting: true })
    wx.showLoading({ title: '发布中' })

    try {
      await api.createDesignerUpdate({ work_id: workId, title, content })
      wx.hideLoading()
      wx.showToast({ title: '发布成功', icon: 'success' })
      this.setData({
        draft: {
          workId,
          title: '',
          content: ''
        }
      })
      await this.loadData()
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '发布失败', icon: 'none' })
    } finally {
      this.setData({ submitting: false })
    }
  },

  handleReplyInput(event) {
    const commentId = event.currentTarget.dataset.commentId
    if (!commentId) {
      return
    }
    this.setData({
      [`commentReplyDraft.${commentId}`]: event.detail.value || ''
    })
  },

  async handleReplyComment(event) {
    const commentId = event.currentTarget.dataset.commentId
    if (!commentId || this.data.replyingCommentId) {
      return
    }
    const replyContent = String((this.data.commentReplyDraft || {})[commentId] || '').trim()
    if (!replyContent) {
      wx.showToast({ title: '请输入回复内容', icon: 'none' })
      return
    }
    this.setData({ replyingCommentId: commentId })
    wx.showLoading({ title: '回复中' })
    try {
      await api.replyDesignerComment(commentId, { reply_content: replyContent })
      wx.hideLoading()
      wx.showToast({ title: '回复成功', icon: 'success' })
      await this.loadDesignerComments(this.data.projectDraft.workId || this.data.draft.workId)
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '回复失败', icon: 'none' })
    } finally {
      this.setData({ replyingCommentId: '' })
    }
  }
})
