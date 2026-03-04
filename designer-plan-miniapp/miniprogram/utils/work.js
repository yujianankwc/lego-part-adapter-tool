const { work: mockWork } = require('./mock')
const { API_BASE_URL } = require('../config/env')

function normalizeSaleMode(mode) {
  const value = String(mode || 'preorder').toLowerCase()
  return value === 'crowdfunding' ? 'crowdfunding' : 'preorder'
}

function normalizeCrowdfundingStatus(status) {
  const value = String(status || 'active').toLowerCase()
  if (value === 'producing' || value === 'failed') {
    return value
  }
  return 'active'
}

function normalizeFunding(rawFunding, goalAmount) {
  const goal = Number(goalAmount || (rawFunding && rawFunding.goal_amount) || 0) || 0
  const funded = Number((rawFunding && rawFunding.funded_amount) || 0) || 0
  const supporters = Number((rawFunding && rawFunding.supporters_count) || 0) || 0
  let progress = Number((rawFunding && rawFunding.progress_percent) || 0) || 0
  if (!progress && goal > 0) {
    progress = Math.min(100, (funded * 100) / goal)
  }
  return {
    supportersCount: supporters,
    fundedAmount: funded,
    goalAmount: goal,
    progressPercent: Number(progress.toFixed(2))
  }
}

function normalizeMediaUrl(input) {
  const value = String(input || '').trim()
  if (!value) {
    return ''
  }
  if (/^https?:\/\//i.test(value)) {
    return value
  }
  const base = String(API_BASE_URL || '').replace(/\/$/, '')
  if (!base) {
    return value
  }
  return `${base}${value.startsWith('/') ? '' : '/'}${value}`
}

function normalizeGallery(rawGallery, coverImage) {
  let list = []
  if (Array.isArray(rawGallery)) {
    list = rawGallery
  } else if (typeof rawGallery === 'string') {
    list = rawGallery
      .split('\n')
      .map((x) => x.trim())
      .filter(Boolean)
  }
  const mapped = list.map((x) => normalizeMediaUrl(x)).filter(Boolean)
  if (!mapped.length && coverImage) {
    mapped.push(coverImage)
  }
  return mapped.slice(0, 12)
}

function normalizeDesigners(rawDesigners) {
  if (!Array.isArray(rawDesigners)) {
    return []
  }
  return rawDesigners
    .map((item) => ({
      designerId: Number((item && (item.designer_id || item.designerId)) || 0),
      displayName: String((item && (item.display_name || item.displayName)) || '').trim(),
      bio: String((item && item.bio) || '').trim(),
      avatarUrl: normalizeMediaUrl((item && (item.avatar_url || item.avatarUrl)) || ''),
      sharePercent: Number((item && (item.share_percent || item.sharePercent)) || 0) || 0
    }))
    .filter((item) => item.designerId > 0)
}

function normalizeWork(rawWork) {
  if (!rawWork || typeof rawWork !== 'object') {
    return { ...mockWork }
  }

  const saleMode = normalizeSaleMode(rawWork.sale_mode || rawWork.saleMode)
  const crowdfundingStatus = normalizeCrowdfundingStatus(rawWork.crowdfunding_status || rawWork.crowdfundingStatus)
  const crowdfundingStatusText =
    rawWork.crowdfunding_status_text ||
    (crowdfundingStatus === 'producing'
      ? '众筹达标，生产中'
      : crowdfundingStatus === 'failed'
      ? '众筹失败，退款中'
      : '众筹进行中')
  const goalAmount = Number(rawWork.crowdfunding_goal_amount || rawWork.crowdfundingGoalAmount || 0) || 0
  const coverImage = normalizeMediaUrl(rawWork.cover_image || rawWork.coverImage || mockWork.coverImage)
  const galleryImages = normalizeGallery(rawWork.gallery_images || rawWork.galleryImages || [], coverImage)

  return {
    id: rawWork.work_id || rawWork.id || mockWork.id,
    name: rawWork.name || mockWork.name,
    subtitle: rawWork.subtitle || mockWork.subtitle,
    saleMode,
    crowdfundingGoalAmount: goalAmount,
    crowdfundingDeadline: rawWork.crowdfunding_deadline || rawWork.crowdfundingDeadline || '',
    crowdfundingStatus,
    crowdfundingStatusText,
    funding: normalizeFunding(rawWork.funding, goalAmount),
    coverImage,
    galleryImages,
    story: rawWork.story || mockWork.story,
    designers: normalizeDesigners(rawWork.designers || rawWork.designer_list || mockWork.designers),
    specs: Array.isArray(rawWork.specs) ? rawWork.specs : mockWork.specs,
    highlights: Array.isArray(rawWork.highlights) ? rawWork.highlights : mockWork.highlights,
    skuList: Array.isArray(rawWork.sku_list)
      ? rawWork.sku_list
      : Array.isArray(rawWork.skuList)
      ? rawWork.skuList
      : mockWork.skuList
  }
}

function isCrowdfunding(work) {
  return normalizeSaleMode(work && work.saleMode) === 'crowdfunding'
}

module.exports = {
  normalizeWork,
  isCrowdfunding,
  normalizeSaleMode,
  normalizeCrowdfundingStatus
}
