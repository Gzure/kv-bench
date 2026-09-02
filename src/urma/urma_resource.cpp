/*
 * kv-bench URMA 资源层实现（对齐 yuanrong urma_resource.cpp 精简）。
 */
#include "urma_resource.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>

/* bonding user_ctl 定义（BONDP_USER_CTL_BONDING / bondp_set_bonding_mode_in_t 等）在
 * urma_ubagg.h 中；引入后 #ifdef BONDP_USER_CTL_BONDING 生效（对齐参考 ChangeBondingBalanceMode） */
#include "urma_ubagg.h"

namespace kv_bench {

namespace {

/* 0=RM 1=RC 2=UM 3=RS（对齐 SDK 示例 args_to_trans_mode） */
urma_transport_mode_t ResolveTransMode(uint32_t mode) {
  switch (mode) {
  case 1:
  case 3:
    return URMA_TM_RC;
  case 2:
    return URMA_TM_UM;
  default:
    return URMA_TM_RM;
  }
}

} // namespace

/* ---------------- UrmaContext ---------------- */

bool UrmaContext::Create(urma_device_t *dev, uint32_t eidIndex,
                         std::unique_ptr<UrmaContext> &ctx) {
  if (dev == nullptr) {
    return false;
  }
  urma_context_t *raw = urma_create_context(dev, eidIndex);
  if (raw == nullptr) {
    fprintf(stderr, "Failed to urma_create_context, errno = %d\n", errno);
    return false;
  }
  ctx.reset(new UrmaContext(raw));
  return true;
}

UrmaContext::~UrmaContext() {
  if (raw_ != nullptr) {
    (void)urma_delete_context(raw_);
    raw_ = nullptr;
  }
}

/* ---------------- UrmaJfce ---------------- */

bool UrmaJfce::Create(urma_context_t *ctx, std::unique_ptr<UrmaJfce> &jfce) {
  if (ctx == nullptr) {
    return false;
  }
  urma_jfce_t *raw = urma_create_jfce(ctx);
  if (raw == nullptr) {
    fprintf(stderr, "Failed to urma_create_jfce, errno = %d\n", errno);
    return false;
  }
  jfce.reset(new UrmaJfce(raw));
  return true;
}

UrmaJfce::~UrmaJfce() {
  if (raw_ != nullptr) {
    (void)urma_delete_jfce(raw_);
    raw_ = nullptr;
  }
}

/* ---------------- UrmaJfc ---------------- */

bool UrmaJfc::Create(urma_context_t *ctx, const urma_device_attr_t &devAttr,
                     urma_jfce_t *jfce, std::unique_ptr<UrmaJfc> &jfc) {
  if (ctx == nullptr) {
    return false;
  }
  urma_jfc_cfg_t cfg;
  (void)memset(&cfg, 0, sizeof(cfg));
  cfg.depth = devAttr.dev_cap.max_jfc_depth;
  cfg.jfce = jfce; /* 事件模式必须绑定 jfce（对齐 urma_sample.c jfc_cfg.jfce） */
  cfg.user_ctx = (uint64_t)NULL;
  urma_jfc_t *raw = urma_create_jfc(ctx, &cfg);
  if (raw == nullptr) {
    fprintf(stderr, "Failed to urma_create_jfc, errno = %d\n", errno);
    return false;
  }
  jfc.reset(new UrmaJfc(raw));
  return true;
}

UrmaJfc::~UrmaJfc() {
  if (raw_ != nullptr) {
    (void)urma_delete_jfc(raw_);
    raw_ = nullptr;
  }
}

bool UrmaJfc::Rearm() const {
  return raw_ != nullptr && urma_rearm_jfc(raw_, false) == URMA_SUCCESS;
}

/* ---------------- UrmaJfr ---------------- */

bool UrmaJfr::Create(urma_context_t *ctx, uint32_t depth,
                     const urma_token_t &token, urma_jfc_t *jfc,
                     uint32_t transMode, std::shared_ptr<UrmaJfr> &jfr) {
  if (ctx == nullptr || jfc == nullptr) {
    return false;
  }
  urma_jfr_cfg_t cfg;
  (void)memset(&cfg, 0, sizeof(cfg));
  cfg.depth = depth;
  cfg.flag.bs.tag_matching = URMA_NO_TAG_MATCHING;
  cfg.flag.bs.order_type = transMode == 3 ? 1 : 0;
  cfg.trans_mode = ResolveTransMode(transMode);
  cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
  cfg.jfc = jfc;
  cfg.token_value = token;
  cfg.id = 0;
  cfg.max_sge = 1;
  cfg.user_ctx = (uint64_t)NULL;
  urma_jfr_t *raw = urma_create_jfr(ctx, &cfg);
  if (raw == nullptr) {
    fprintf(stderr, "Failed to urma_create_jfr, errno = %d\n", errno);
    return false;
  }
  jfr.reset(new UrmaJfr(raw));
  return true;
}

UrmaJfr::~UrmaJfr() {
  if (raw_ != nullptr) {
    (void)urma_delete_jfr(raw_);
    raw_ = nullptr;
  }
}

/* ---------------- UrmaJetty ---------------- */

bool UrmaJetty::Create(UrmaResource &res, bool isSend,
                       std::shared_ptr<UrmaJetty> &out) {
  urma_context_t *ctx = res.Ctx();
  urma_jfc_t *jfc = res.Jfc();
  if (ctx == nullptr || jfc == nullptr) {
    return false;
  }

  /* send: 共享 context 级 JFR；recv: 独立 JFR（对齐 yuanrong UrmaJetty::Create
   * L344） */
  std::shared_ptr<UrmaJfr> jfr;
  if (isSend) {
    if (res.sharedJettyJfr_ == nullptr) {
      if (!UrmaJfr::Create(ctx, kSharedJfrDepth, res.Token(), jfc,
                           res.TransMode(), res.sharedJettyJfr_)) {
        return false;
      }
    }
    jfr = res.sharedJettyJfr_;
  } else {
    if (!UrmaJfr::Create(ctx, kUrmaJettySize, res.Token(), jfc, res.TransMode(),
                         jfr)) {
      return false;
    }
  }

  urma_jfs_cfg_t jfsCfg;
  (void)memset(&jfsCfg, 0, sizeof(jfsCfg));
  jfsCfg.depth = kUrmaJettySize;
  jfsCfg.flag.bs.order_type = res.TransMode() == 3 ? 1 : 0;
  jfsCfg.flag.bs.multi_path = 1;
  jfsCfg.trans_mode = ResolveTransMode(res.TransMode());
  jfsCfg.priority = 4;
  jfsCfg.max_sge = kUrmaMaxSge;
  jfsCfg.max_inline_data = 0;
  jfsCfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
  jfsCfg.err_timeout = 0;
  jfsCfg.jfc = jfc;
  jfsCfg.user_ctx = (uint64_t)NULL;

  urma_jetty_cfg_t jettyCfg;
  (void)memset(&jettyCfg, 0, sizeof(jettyCfg));
  jettyCfg.flag.bs.share_jfr = 1;
  jettyCfg.jfs_cfg = jfsCfg;
  jettyCfg.shared.jfr = jfr->Raw();
  jettyCfg.shared.jfc = jfc;
  jettyCfg.user_ctx = (uint64_t)NULL;

  urma_jetty_t *raw = urma_create_jetty(ctx, &jettyCfg);
  if (raw == nullptr) {
    fprintf(stderr, "Failed to urma_create_jetty, errno = %d\n", errno);
    return false;
  }
  out.reset(new UrmaJetty(raw, std::move(jfr)));
  return true;
}

UrmaJetty::~UrmaJetty() {
  if (raw_ != nullptr) {
    (void)urma_delete_jetty(raw_);
    raw_ = nullptr;
  }
}

/* ---------------- UrmaTargetJetty ---------------- */

bool UrmaTargetJetty::Import(urma_context_t *ctx,
                             const urma_rjetty_t *remoteJetty,
                             const urma_token_t &token,
                             std::unique_ptr<UrmaTargetJetty> &out) {
  if (ctx == nullptr || remoteJetty == nullptr) {
    return false;
  }
  urma_rjetty_t *mutableJetty = const_cast<urma_rjetty_t *>(remoteJetty);
  urma_token_t *mutableToken = const_cast<urma_token_t *>(&token);
  urma_target_jetty_t *raw = urma_import_jetty(ctx, mutableJetty, mutableToken);
  if (raw == nullptr) {
    fprintf(stderr, "Failed to urma_import_jetty, errno = %d\n", errno);
    return false;
  }
  out.reset(new UrmaTargetJetty(raw));
  return true;
}

UrmaTargetJetty::~UrmaTargetJetty() {
  if (raw_ != nullptr) {
    (void)urma_unimport_jetty(raw_);
    raw_ = nullptr;
  }
}

/* ---------------- UrmaLocalSegment ---------------- */

bool UrmaLocalSegment::Register(urma_context_t *ctx, uint64_t va, uint64_t len,
                                const urma_token_t &token, bool cacheable,
                                std::shared_ptr<UrmaLocalSegment> &out) {
  if (ctx == nullptr) {
    return false;
  }
  urma_reg_seg_flag_t flag;
  (void)memset(&flag, 0, sizeof(flag));
  flag.bs.token_policy = URMA_TOKEN_NONE;
  flag.bs.cacheable = cacheable ? URMA_CACHEABLE : URMA_NON_CACHEABLE;
  flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
  flag.bs.token_id_valid = 0;
  flag.bs.reserved = 0;

  urma_seg_cfg_t cfg;
  (void)memset(&cfg, 0, sizeof(cfg));
  cfg.va = va;
  cfg.len = len;
  cfg.token_id = nullptr;
  cfg.token_value = token;
  cfg.flag = flag;
  cfg.user_ctx = (uintptr_t)NULL;
  cfg.iova = 0;

  urma_target_seg_t *raw = urma_register_seg(ctx, &cfg);
  if (raw == nullptr) {
    fprintf(stderr,
            "Failed to urma_register_seg (va=0x%llx len=%llu), errno = %d\n",
            (unsigned long long)va, (unsigned long long)len, errno);
    return false;
  }
  out.reset(new UrmaLocalSegment(raw));
  return true;
}

UrmaLocalSegment::~UrmaLocalSegment() {
  if (raw_ != nullptr) {
    (void)urma_unregister_seg(raw_);
    raw_ = nullptr;
  }
}

uint64_t UrmaLocalSegment::Va() const {
  return raw_ != nullptr ? raw_->seg.ubva.va : 0;
}

uint64_t UrmaLocalSegment::Len() const {
  return raw_ != nullptr ? raw_->seg.len : 0;
}

/* ---------------- UrmaRemoteSegment ---------------- */

bool UrmaRemoteSegment::Import(urma_context_t *ctx, const urma_seg_t &seg,
                               const urma_token_t &token, bool cacheable,
                               std::shared_ptr<UrmaRemoteSegment> &out) {
  if (ctx == nullptr) {
    return false;
  }
  urma_import_seg_flag_t flag;
  (void)memset(&flag, 0, sizeof(flag));
  flag.bs.cacheable = cacheable ? URMA_CACHEABLE : URMA_NON_CACHEABLE;
  flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
  flag.bs.mapping = URMA_SEG_NOMAP;
  flag.bs.reserved = 0;

  urma_seg_t remote = seg;
  urma_token_t *mutableToken = const_cast<urma_token_t *>(&token);
  urma_target_seg_t *raw = urma_import_seg(ctx, &remote, mutableToken, 0, flag);
  if (raw == nullptr) {
    fprintf(stderr, "Failed to urma_import_seg (va=0x%llx), errno = %d\n",
            (unsigned long long)seg.ubva.va, errno);
    return false;
  }
  out.reset(new UrmaRemoteSegment(raw));
  return true;
}

UrmaRemoteSegment::~UrmaRemoteSegment() {
  if (raw_ != nullptr) {
    (void)urma_unimport_seg(raw_);
    raw_ = nullptr;
  }
}

uint64_t UrmaRemoteSegment::Va() const {
  return raw_ != nullptr ? raw_->seg.ubva.va : 0;
}

/* ---------------- UrmaResource ---------------- */

UrmaResource::~UrmaResource() { Clear(); }

bool UrmaResource::Init(urma_device_t *device, uint32_t eidIndex,
                        bool cacheable, uint32_t jettyCount, uint32_t minJettys,
                        bool eventMode, uint32_t transMode) {
  Clear();
  if (device == nullptr) {
    return false;
  }
  cacheable_ = cacheable;
  transMode_ = transMode;
  if (urma_query_device(device, &devAttr_) != URMA_SUCCESS) {
    fprintf(stderr, "Failed to urma_query_device\n");
    return false;
  }
  maxWriteSize_ = devAttr_.dev_cap.max_write_size;

  if (!UrmaContext::Create(device, eidIndex, context_)) {
    return false;
  }
  token_.token = 0xACFE; /* 对齐 yuanrong 默认 token */

  /* bonding 设备：设置 bonding balance 模式（对齐参考 UrmaContext::ChangeBondingBalanceMode）。
   * 必须在创建任何队列（jfce/jfc/jetty）之前调用，否则驱动可能因设备忙返回 EAGAIN(11)。
   * 默认编译关闭（-DKV_BENCH_BONDP_CTL=ON 开启）：部分平台/UMDK 版本的
   * urma_user_ctl 不可用或 bondp 相关库崩溃，默认不开更安全。 */
  if (strncmp(device->name, "bonding", strlen("bonding")) == 0) {
#ifdef KV_BENCH_BONDP_CTL
    bondp_set_bonding_mode_in_t mode;
    (void)memset(&mode, 0, sizeof(mode));
    mode.bonding_mode = BONDP_BONDING_MODE_BALANCE;
    mode.bonding_level = BONDP_BONDING_LEVEL_PORT;
    urma_user_ctl_in_t in;
    (void)memset(&in, 0, sizeof(in));
    in.addr = reinterpret_cast<uint64_t>(&mode);
    in.len = sizeof(mode);
    in.opcode = BONDP_USER_CTL_SET_BONDING_MODE;
    urma_user_ctl_out_t out;
    (void)memset(&out, 0, sizeof(out));
    const urma_status_t rc = urma_user_ctl(context_->Raw(), &in, &out);
    printf("bonding device %s: set balance mode rc=%d\n", device->name, rc);
#else
    fprintf(stderr,
            "Warning: bonding device %s: balance mode disabled at compile time "
            "(-DKV_BENCH_BONDP_CTL=ON to enable)\n",
            device->name);
#endif
  }

  if (!UrmaJfce::Create(context_->Raw(), jfce_)) {
    return false;
  }
  if (!UrmaJfc::Create(context_->Raw(), devAttr_,
                       eventMode ? jfce_->Raw() : nullptr, jfc_)) {
    return false;
  }

  uint32_t count = jettyCount > minJettys ? jettyCount : minJettys;
  if (count < 1) {
    count = 1;
  }
  if (!PreFillSendJettyPool(count)) {
    return false;
  }
  {
    std::shared_ptr<UrmaJetty> recv;
    if (!GetOrCreateSharedRecvJetty(recv)) {
      return false;
    }
  }
  printf("urma resource init ok: jettys=%u max_write_size=%llu\n",
         SendJettyCount(), (unsigned long long)maxWriteSize_);
  return true;
}

void UrmaResource::Clear() {
  localSeg_.reset();
  sendJettyPool_.Clear();
  sharedRecvJetty_.reset();
  sharedJettyJfr_.reset();
  jfc_.reset();
  jfce_.reset();
  context_.reset();
}

urma_context_t *UrmaResource::Ctx() const {
  return context_ != nullptr ? context_->Raw() : nullptr;
}

urma_jfce_t *UrmaResource::Jfce() const {
  return jfce_ != nullptr ? jfce_->Raw() : nullptr;
}

urma_jfc_t *UrmaResource::Jfc() const {
  return jfc_ != nullptr ? jfc_->Raw() : nullptr;
}

uint64_t UrmaResource::MaxWriteSize() const { return maxWriteSize_; }

bool UrmaResource::RegisterSegment(uint64_t va, uint64_t len,
                                   std::shared_ptr<UrmaLocalSegment> &out) {
  if (Ctx() == nullptr) {
    return false;
  }
  if (!UrmaLocalSegment::Register(Ctx(), va, len, token_, cacheable_,
                                  localSeg_)) {
    return false;
  }
  out = localSeg_;
  return true;
}

bool UrmaResource::CreateSendJetty(std::shared_ptr<UrmaJetty> &out) {
  return UrmaJetty::Create(*this, true, out);
}

bool UrmaResource::GetOrCreateSharedRecvJetty(
    std::shared_ptr<UrmaJetty> &out) {
  if (sharedRecvJetty_ == nullptr) {
    /* RECV jetty 持独立 JFR（对齐 yuanrong UrmaJetty::Create recv 路径） */
    if (!UrmaJetty::Create(*this, false, sharedRecvJetty_)) {
      return false;
    }
    printf("created shared recv jetty id %u\n",
           sharedRecvJetty_->JettyId());
  }
  out = sharedRecvJetty_;
  return true;
}

uint32_t UrmaResource::SendJettyCount() const {
  return static_cast<uint32_t>(sendJettyPool_.GetStats().poolSize);
}

std::shared_ptr<UrmaJetty> UrmaResource::JettyAt(uint32_t index) const {
  return sendJettyPool_.At(index);
}

SendJettyPool::Stats UrmaResource::SendJettyPoolStats() const {
  return sendJettyPool_.GetStats();
}

bool UrmaResource::AcquireSendJetty(std::shared_ptr<UrmaJetty> &out) {
  return sendJettyPool_.PopIdle(out);
}

void UrmaResource::ReleaseSendJetty(const std::shared_ptr<UrmaJetty> &jetty) {
  sendJettyPool_.Release(jetty);
}

bool UrmaResource::ImportTargetJetty(const urma_rjetty_t *remoteJetty,
                                     std::unique_ptr<UrmaTargetJetty> &out) {
  return UrmaTargetJetty::Import(Ctx(), remoteJetty, token_, out);
}

bool UrmaResource::ImportSegment(const urma_seg_t &seg,
                                 std::shared_ptr<UrmaRemoteSegment> &out) {
  return UrmaRemoteSegment::Import(Ctx(), seg, token_, cacheable_, out);
}

urma_eid_t UrmaResource::Eid() const {
  return context_ != nullptr ? context_->Eid() : urma_eid_t{};
}

uint32_t UrmaResource::Uasid() const {
  return context_ != nullptr ? context_->Uasid() : 0;
}

uint32_t UrmaResource::PublishedJettyId() const {
  return sharedRecvJetty_ != nullptr ? sharedRecvJetty_->JettyId() : 0;
}

bool UrmaResource::PreFillSendJettyPool(uint32_t count) {
  for (uint32_t i = 0; i < count; i++) {
    std::shared_ptr<UrmaJetty> jetty;
    if (!CreateSendJetty(jetty)) {
      fprintf(stderr, "Failed to create send jetty %u/%u\n", i, count);
      return false;
    }
    sendJettyPool_.Add(std::move(jetty));
  }
  return true;
}

} // namespace kv_bench
