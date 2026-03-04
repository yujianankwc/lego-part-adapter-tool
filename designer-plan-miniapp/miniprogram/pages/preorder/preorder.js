const { work } = require('../../utils/mock')
const api = require('../../utils/api')
const { ensureLogin } = require('../../utils/auth')
const { normalizeWork, isCrowdfunding } = require('../../utils/work')

Page({
  data: {
    work,
    selectedSkuId: 'standard',
    quantity: 1,
    agree: false,
    reserved: false,
    submitting: false,
    paying: false,
    isCrowdfundingMode: false,
    pageTitle: '预售下单',
    agreementText: '我已阅读并同意预售规则与发货周期说明',
    submitButtonText: '提交预售订单',
    isOrderDisabled: false,
    disableReason: ''
  },

  async onShow() {
    await Promise.all([this.loadWork(), this.loadSummary()])
  },

  async loadWork() {
    try {
      const ret = await api.getCurrentWork()
      this.applyWorkData(normalizeWork(ret.work))
    } catch (error) {
      this.applyWorkData(normalizeWork(work))
    }
  },

  applyWorkData(nextWork) {
    const crowdfunding = isCrowdfunding(nextWork)
    const crowdfundingStatus = nextWork.crowdfundingStatus || 'active'
    const isOrderDisabled = crowdfunding && crowdfundingStatus !== 'active'
    let disableReason = ''
    if (crowdfundingStatus === 'producing') {
      disableReason = '众筹已达标并进入生产阶段，当前不再接受支持。'
    } else if (crowdfundingStatus === 'failed') {
      disableReason = '众筹已结束且未达标，系统已自动退款。'
    }
    const firstSkuId = nextWork.skuList && nextWork.skuList.length ? nextWork.skuList[0].id : 'standard'
    let submitButtonText = crowdfunding ? '提交众筹支持' : '提交预售订单'
    if (isOrderDisabled && crowdfundingStatus === 'producing') {
      submitButtonText = '已达标生产中'
    } else if (isOrderDisabled && crowdfundingStatus === 'failed') {
      submitButtonText = '众筹失败已退款'
    }
    this.setData({
      work: nextWork,
      selectedSkuId: firstSkuId,
      isCrowdfundingMode: crowdfunding,
      pageTitle: crowdfunding ? '众筹支持' : '预售下单',
      agreementText: crowdfunding ? '我已阅读并同意众筹规则与交付说明' : '我已阅读并同意预售规则与发货周期说明',
      submitButtonText,
      isOrderDisabled,
      disableReason
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

  getSelectedSku() {
    return this.data.work.skuList.find((sku) => sku.id === this.data.selectedSkuId)
  },

  handleSelectSku(event) {
    this.setData({
      selectedSkuId: event.currentTarget.dataset.skuId
    })
  },

  handleAddQty() {
    this.setData({ quantity: this.data.quantity + 1 })
  },

  handleReduceQty() {
    const next = this.data.quantity - 1
    this.setData({ quantity: next < 1 ? 1 : next })
  },

  handleAgreeChange(event) {
    this.setData({ agree: event.detail.value.length > 0 })
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

  normalizePaymentInstruction(payment) {
    const payload = payment || {}
    const modeRaw = String(payload.mode || 'mock').trim().toLowerCase()
    const mode = modeRaw || 'mock'
    const params =
      payload.params ||
      payload.jsapi_params ||
      payload.wechat_params ||
      payload.wechatJsapiParams ||
      {}
    if (mode === 'mock') {
      return {
        mode: 'mock',
        mockToken: String(payload.mock_token || ''),
        confirmSource: 'mock'
      }
    }
    if (mode === 'jsapi' || mode === 'wechat' || mode === 'wechat_jsapi') {
      return {
        mode: 'wechat_jsapi',
        params,
        confirmSource: 'client_jsapi'
      }
    }
    return {
      mode: 'unknown',
      params: {},
      confirmSource: ''
    }
  },

  async invokeWechatPay(params) {
    const requiredKeys = ['timeStamp', 'nonceStr', 'package', 'signType', 'paySign']
    const missing = requiredKeys.filter((key) => !params || !params[key])
    if (missing.length) {
      throw new Error('微信支付参数不完整，请联系管理员检查支付配置')
    }
    await new Promise((resolve, reject) => {
      wx.requestPayment({
        ...params,
        success: () => resolve(),
        fail: (err) => {
          const msg = String((err && err.errMsg) || '')
          if (msg.includes('cancel')) {
            reject(new Error('支付已取消'))
            return
          }
          reject(new Error(msg || '支付未完成'))
        }
      })
    })
  },

  async runPayment(orderId, payment) {
    const instruction = this.normalizePaymentInstruction(payment)
    if (instruction.mode === 'mock') {
      await api.confirmPayment({
        order_id: orderId,
        mock_token: instruction.mockToken
      })
      return
    }

    if (instruction.mode === 'wechat_jsapi') {
      await this.invokeWechatPay(instruction.params || {})
      await api.confirmPayment({
        order_id: orderId,
        source: instruction.confirmSource
      })
      return
    }

    throw new Error('当前支付模式未配置')
  },

  async handleSubmitOrder() {
    if (this.data.submitting) {
      return
    }

    if (this.data.isOrderDisabled) {
      wx.showToast({ title: this.data.disableReason || '当前阶段不可下单', icon: 'none' })
      return
    }

    if (!this.data.agree) {
      wx.showToast({ title: '请先勾选预售协议', icon: 'none' })
      return
    }

    const sku = this.getSelectedSku()
    if (!sku) {
      wx.showToast({ title: '请选择版本', icon: 'none' })
      return
    }

    this.setData({ submitting: true, paying: false })
    wx.showLoading({ title: '正在创建订单' })

    try {
      await ensureLogin()
      const result = await api.createPreorder({
        sku_id: sku.id,
        quantity: this.data.quantity
      })

      this.setData({ paying: true })
      wx.showLoading({ title: '正在拉起支付' })
      await this.runPayment(result.order.order_id, result.payment)
      wx.hideLoading()

      const paidAmount = Number(result.order.paid_amount || 0)
      const actionWord = this.data.isCrowdfundingMode ? '支持成功' : '下单成功'
      const payText = this.data.isCrowdfundingMode ? '支持金额' : '定金合计'

      wx.showModal({
        title: actionWord,
        content: `你已锁定 ${sku.name}，${payText} ￥${paidAmount}`,
        showCancel: false,
        success: () => {
          wx.navigateTo({
            url: '/pages/my-orders/my-orders'
          })
        }
      })
    } catch (error) {
      wx.hideLoading()
      wx.showToast({ title: error.message || '下单失败', icon: 'none' })
    } finally {
      this.setData({ submitting: false, paying: false })
    }
  }
})
