const api = require('../../utils/api')
const { ensureLogin } = require('../../utils/auth')

Page({
  data: {
    categories: [
      { key: 'general', label: '一般咨询' },
      { key: 'bug', label: '问题反馈' },
      { key: 'feature', label: '功能建议' },
      { key: 'service', label: '服务投诉' }
    ],
    priorities: [
      { key: 'low', label: '低' },
      { key: 'normal', label: '普通' },
      { key: 'high', label: '高' },
      { key: 'urgent', label: '紧急' }
    ],
    categoryIndex: 0,
    priorityIndex: 1,
    contact: '',
    content: '',
    imageFiles: [],
    imageUrls: [],
    items: [],
    loading: false
  },

  async onShow() {
    await this.loadFeedbacks()
  },

  async loadFeedbacks() {
    this.setData({ loading: true })
    try {
      await ensureLogin()
      const ret = await api.getMyFeedback(50)
      const items = (ret.items || []).map((it) => ({
        ...it,
        image_urls: (it.image_urls || []).map((url) => api.resolveUrl(url))
      }))
      this.setData({ items })
    } catch (error) {
      wx.showToast({ title: error.message || '加载失败', icon: 'none' })
    } finally {
      this.setData({ loading: false })
    }
  },

  handleCategoryChange(event) {
    this.setData({ categoryIndex: Number(event.detail.value || 0) })
  },

  handlePriorityChange(event) {
    this.setData({ priorityIndex: Number(event.detail.value || 1) })
  },

  handleContactInput(event) {
    this.setData({ contact: (event.detail.value || '').trim() })
  },

  handleContentInput(event) {
    this.setData({ content: event.detail.value || '' })
  },

  handleRemoveImage(event) {
    const index = Number(event.currentTarget.dataset.index || -1)
    if (index < 0) {
      return
    }
    const files = (this.data.imageFiles || []).slice()
    const urls = (this.data.imageUrls || []).slice()
    files.splice(index, 1)
    urls.splice(index, 1)
    this.setData({ imageFiles: files, imageUrls: urls })
  },

  async handleChooseImages() {
    const left = 3 - (this.data.imageFiles || []).length
    if (left <= 0) {
      wx.showToast({ title: '最多上传3张图片', icon: 'none' })
      return
    }
    try {
      const choose = await new Promise((resolve, reject) => {
        wx.chooseMedia({
          count: left,
          mediaType: ['image'],
          sizeType: ['compressed'],
          sourceType: ['album', 'camera'],
          success: resolve,
          fail: reject
        })
      })
      const files = (choose.tempFiles || []).map((x) => x.tempFilePath).filter(Boolean)
      if (!files.length) {
        return
      }
      wx.showLoading({ title: '上传中' })
      await ensureLogin()
      const uploaded = []
      for (let i = 0; i < files.length; i += 1) {
        const ret = await api.uploadImage(files[i])
        uploaded.push(ret.url || '')
      }
      wx.hideLoading()
      this.setData({
        imageFiles: (this.data.imageFiles || []).concat(files),
        imageUrls: (this.data.imageUrls || []).concat(uploaded)
      })
    } catch (error) {
      wx.hideLoading()
      if (String(error.errMsg || '').includes('cancel')) {
        return
      }
      wx.showToast({ title: error.message || '上传失败', icon: 'none' })
    }
  },

  async handleSubmit() {
    const content = (this.data.content || '').trim()
    if (!content || content.length < 5) {
      wx.showToast({ title: '反馈内容至少 5 个字', icon: 'none' })
      return
    }
    const selected = this.data.categories[this.data.categoryIndex] || this.data.categories[0]
    const selectedPriority = this.data.priorities[this.data.priorityIndex] || this.data.priorities[1]
    try {
      wx.showLoading({ title: '提交中' })
      await ensureLogin()
      await api.submitFeedback({
        category: selected.key,
        priority: selectedPriority.key,
        content,
        contact: this.data.contact || '',
        image_urls: this.data.imageUrls || []
      })
      wx.hideLoading()
      wx.showToast({ title: '提交成功', icon: 'success' })
      this.setData({ content: '', imageFiles: [], imageUrls: [] })
      await this.loadFeedbacks()
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '提交失败', icon: 'none' })
    }
  }
})
