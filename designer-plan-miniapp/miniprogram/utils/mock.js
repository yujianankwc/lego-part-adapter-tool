const work = {
  id: 'POJIANZHE-001',
  name: '破茧者',
  subtitle: '中国原创积木作品 · 首发款',
  saleMode: 'preorder',
  crowdfundingGoalAmount: 50000,
  crowdfundingDeadline: '',
  crowdfundingStatus: 'active',
  crowdfundingStatusText: '众筹进行中',
  coverImage: 'https://picsum.photos/seed/pojianzhe-cover/1200/720',
  galleryImages: [
    'https://picsum.photos/seed/pojianzhe-g1/1200/900',
    'https://picsum.photos/seed/pojianzhe-g2/1200/900',
    'https://picsum.photos/seed/pojianzhe-g3/1200/900'
  ],
  funding: {
    supportersCount: 0,
    fundedAmount: 0,
    goalAmount: 50000,
    progressPercent: 0
  },
  story:
    '以鹤为骨、梅为脉、山石为势，通过可拼搭结构表达“破茧而立”的成长意象。作品强调桌面雕塑感与观赏面完整度。',
  designers: [
    {
      designerId: 1,
      displayName: '原创设计师',
      bio: '热爱东方美学与积木结构表达，关注可拼搭性与观赏性的平衡。',
      avatarUrl: 'https://picsum.photos/seed/designer-avatar-1/160/160',
      sharePercent: 15
    }
  ],
  specs: [
    { label: '零件数', value: '约 980 pcs' },
    { label: '成品尺寸', value: '约 26 x 26 x 33 cm' },
    { label: '拼搭难度', value: '中高阶' },
    { label: '拼搭时长', value: '6-8 小时' },
    { label: '发货节奏', value: '预售后 15 天内' }
  ],
  highlights: [
    '环形构图+鹤翼展开，主视角冲击力强',
    '梅枝与山体形成前后层次，适合静态陈列',
    '结构模块化，便于后续IP化扩展'
  ],
  skuList: [
    {
      id: 'standard',
      name: '标准版',
      price: 499,
      deposit: 99,
      stock: 120,
      perks: ['基础彩盒', '电子说明书', '售后补件支持']
    },
    {
      id: 'collector',
      name: '收藏版',
      price: 699,
      deposit: 149,
      stock: 60,
      perks: ['限定编号卡', '设计师签名卡', '独立包装套封']
    }
  ]
}

const program = {
  title: '酷玩潮 × 高砖 原创设计师计划',
  intro:
    '面向原创积木创作者，提供打样支持、供应链转化、品牌共创和发售运营，帮助作品从MOC走向商品化。',
  support: ['产品化评审', '供应链对接', '视觉包装与宣发', '上架销售与复盘'],
  process: ['提交作品', '初审反馈', '打样优化', '签约上架', '联合发售']
}

module.exports = {
  work,
  program
}
