const { program } = require('../../utils/mock')
const api = require('../../utils/api')
const { ensureLogin } = require('../../utils/auth')

const CATEGORY_OPTIONS = ['东方题材', '机械科幻', '建筑场景', '人仔主题', '其他']

Page({
  data: {
    program,
    categoryOptions: CATEGORY_OPTIONS,
    categoryIndex: 0,
    form: {
      designerName: '',
      contact: '',
      workName: '',
      intro: '',
      estimatedPieces: ''
    },
    imageList: [],
    submitting: false,
    uploading: false
  },

  handleCategoryChange(event) {
    this.setData({ categoryIndex: Number(event.detail.value) })
  },

  handleInput(event) {
    const { field } = event.currentTarget.dataset
    this.setData({
      [`form.${field}`]: event.detail.value
    })
  },

  async handleChooseImage() {
    const remain = 6 - this.data.imageList.length
    if (remain <= 0) {
      wx.showToast({ title: '最多上传 6 张', icon: 'none' })
      return
    }

    wx.chooseMedia({
      count: remain,
      mediaType: ['image'],
      sourceType: ['album', 'camera'],
      success: async (res) => {
        const files = (res.tempFiles || []).map((item) => item.tempFilePath)
        if (!files.length) {
          return
        }

        this.setData({ uploading: true })
        wx.showLoading({ title: '上传中' })

        try {
          await ensureLogin()
          const uploaded = []
          for (let i = 0; i < files.length; i += 1) {
            const filePath = files[i]
            const ret = await api.uploadImage(filePath)
            uploaded.push(ret.absoluteUrl)
          }

          this.setData({ imageList: this.data.imageList.concat(uploaded).slice(0, 6) })
          wx.hideLoading()
          wx.showToast({ title: '上传成功', icon: 'success' })
        } catch (error) {
          wx.hideLoading()
          wx.showToast({ title: error.message || '上传失败', icon: 'none' })
        } finally {
          this.setData({ uploading: false })
        }
      }
    })
  },

  handleDeleteImage(event) {
    const index = Number(event.currentTarget.dataset.index)
    const next = this.data.imageList.filter((_, i) => i !== index)
    this.setData({ imageList: next })
  },

  validateForm() {
    const { designerName, contact, workName, intro, estimatedPieces } = this.data.form

    if (!designerName.trim() || !contact.trim() || !workName.trim() || !intro.trim()) {
      return '请完整填写必填信息'
    }

    if (intro.trim().length < 30) {
      return '作品简介至少 30 字'
    }

    if (!/^\d+$/.test(estimatedPieces.trim())) {
      return '预计零件数请填写整数'
    }

    if (this.data.imageList.length === 0) {
      return '请至少上传 1 张作品图'
    }

    return ''
  },

  async handleSubmit() {
    if (this.data.submitting || this.data.uploading) {
      return
    }

    const errorMessage = this.validateForm()
    if (errorMessage) {
      wx.showToast({ title: errorMessage, icon: 'none' })
      return
    }

    this.setData({ submitting: true })
    wx.showLoading({ title: '提交中' })

    try {
      await ensureLogin()
      await api.submitDesignerApplication({
        designer_name: this.data.form.designerName.trim(),
        contact: this.data.form.contact.trim(),
        work_name: this.data.form.workName.trim(),
        category: this.data.categoryOptions[this.data.categoryIndex],
        intro: this.data.form.intro.trim(),
        estimated_pieces: Number(this.data.form.estimatedPieces),
        image_urls: this.data.imageList
      })

      wx.hideLoading()
      wx.showModal({
        title: '投稿已提交',
        content: '我们会在 3-5 个工作日内反馈审核结果。',
        showCancel: false,
        success: () => {
          this.setData({
            categoryIndex: 0,
            form: {
              designerName: '',
              contact: '',
              workName: '',
              intro: '',
              estimatedPieces: ''
            },
            imageList: []
          })
        }
      })
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '提交失败', icon: 'none' })
    } finally {
      this.setData({ submitting: false })
    }
  }
})
