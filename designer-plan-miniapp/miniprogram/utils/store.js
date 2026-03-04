function getReserved() {
  return Boolean(wx.getStorageSync('kwc_reserved'))
}

function setReserved(flag) {
  wx.setStorageSync('kwc_reserved', Boolean(flag))
}

function getOrders() {
  const value = wx.getStorageSync('kwc_orders')
  return Array.isArray(value) ? value : []
}

function addOrder(order) {
  const orders = getOrders()
  wx.setStorageSync('kwc_orders', [order].concat(orders))
}

function getSubmissions() {
  const value = wx.getStorageSync('kwc_submissions')
  return Array.isArray(value) ? value : []
}

function addSubmission(submission) {
  const submissions = getSubmissions()
  wx.setStorageSync('kwc_submissions', [submission].concat(submissions))
}

module.exports = {
  getReserved,
  setReserved,
  getOrders,
  addOrder,
  getSubmissions,
  addSubmission
}
