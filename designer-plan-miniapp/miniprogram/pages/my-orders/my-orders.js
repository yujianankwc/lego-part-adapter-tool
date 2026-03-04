const api = require('../../utils/api')
const { ensureLogin } = require('../../utils/auth')

Page({
  data: {
    orderTab: 'all',
    orderTabs: [
      { key: 'all', label: '全部' },
      { key: 'pending', label: '待支付' },
      { key: 'paid', label: '已支付' },
      { key: 'refunding', label: '退款中' },
      { key: 'refunded', label: '已退款' }
    ],
    periodOptions: [
      { days: 0, label: '全部时间' },
      { days: 30, label: '近30天' },
      { days: 90, label: '近90天' }
    ],
    periodDays: 0,
    orderSaleMode: '',
    orders: [],
    ordersOffset: 0,
    ordersLimit: 20,
    ordersHasMore: false,
    ordersLoading: false
  },

  async onShow() {
    await this.reloadOrders()
  },

  mapOrderItem(item) {
    const refundStatus = item.refund_status || 'none'
    let statusKey = 'pending'
    if (
      item.pay_status === 'refunded' ||
      refundStatus === 'refunded' ||
      item.order_status === 'crowdfunding_refunded'
    ) {
      statusKey = 'refunded'
    } else if (
      refundStatus === 'pending_submit' ||
      refundStatus === 'processing' ||
      item.order_status === 'crowdfunding_refunding'
    ) {
      statusKey = 'refunding'
    } else if (item.pay_status === 'paid') {
      statusKey = 'paid'
    }
    return {
      orderId: item.order_id,
      workName: item.work_name,
      skuName: item.sku_name,
      saleMode: item.sale_mode || 'preorder',
      saleModeText: item.sale_mode === 'crowdfunding' ? '众筹' : '预售',
      quantity: item.quantity,
      totalAmount: Number(item.total_amount || 0),
      paidAmount: Number(item.paid_amount || 0),
      refundAmount: Number(item.refund_amount || 0),
      payStatus: item.pay_status || '',
      refundStatus,
      refundReason: item.refund_reason || '',
      refundedAt: item.refunded_at || '',
      statusKey,
      status: item.order_status_text,
      createdAt: item.created_at,
      paidAt: item.paid_at || ''
    }
  },

  async reloadOrders() {
    this.setData({
      orders: [],
      ordersOffset: 0,
      ordersHasMore: true
    })
    await this.loadMoreOrders()
  },

  async loadMoreOrders() {
    if (this.data.ordersLoading || !this.data.ordersHasMore) {
      return
    }
    this.setData({ ordersLoading: true })
    try {
      await ensureLogin()
      const ret = await api.getMyOrders({
        limit: this.data.ordersLimit,
        offset: this.data.ordersOffset,
        status: this.data.orderTab === 'all' ? '' : this.data.orderTab,
        sale_mode: this.data.orderSaleMode,
        period_days: this.data.periodDays
      })
      const newItems = (ret.items || []).map((item) => this.mapOrderItem(item))
      this.setData({
        orders: this.data.orders.concat(newItems),
        ordersOffset: Number(ret.offset || this.data.ordersOffset) + newItems.length,
        ordersHasMore: Boolean(ret.has_more)
      })
    } catch (error) {
      wx.showToast({ title: error.message || '订单加载失败', icon: 'none' })
    } finally {
      this.setData({ ordersLoading: false })
    }
  },

  async handleSwitchOrderTab(event) {
    const tab = event.currentTarget.dataset.tab || 'all'
    if (tab === this.data.orderTab) {
      return
    }
    this.setData({ orderTab: tab })
    await this.reloadOrders()
  },

  async handleSwitchPeriod(event) {
    const days = Number(event.currentTarget.dataset.days || 0)
    if (days === this.data.periodDays) {
      return
    }
    this.setData({ periodDays: days })
    await this.reloadOrders()
  },

  async handleLoadMoreOrders() {
    await this.loadMoreOrders()
  }
})
