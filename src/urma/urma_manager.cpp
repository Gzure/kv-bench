/*
 * kv-bench URMA 管理层实现（对齐 yuanrong urma_manager.cpp 精简）。
 */
#include "urma_manager.h"

#include <cstdio>
#include <cstring>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "urma_opcode.h"
#include "urma_ubagg.h"

namespace kv_bench {

namespace {

constexpr int kMaxPollCr = 32;
constexpr uint64_t kPollSleepNs =
    1000; /* 1us，对齐参考 UrmaManager::PollJfcWait 退避 */

uint64_t NowNs() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

void SleepNs(uint64_t ns) {
  struct timespec ts = {.tv_sec = 0, .tv_nsec = (long)ns};
  nanosleep(&ts, nullptr);
}

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

/* 控制面交换的 POD（替代 protobuf） */
struct WireInfo {
  urma_eid_t eid;
  uint32_t uasid;
  uint64_t seg_va;
  uint64_t seg_len;
  uint32_t seg_flag;
  uint32_t seg_token_id;
  urma_jetty_id_t jetty_id;
  uint32_t threads;
  uint32_t op_code;
  uint32_t dual_mode;
  uint32_t value_size;
  uint32_t dst_chip;
  uint32_t reserved[2];
} __attribute__((packed));

int SockSyncData(int sockfd, int size, char *localData, char *remoteData) {
  int rc = (int)write(sockfd, localData, (size_t)size);
  if (rc < size) {
    fprintf(stderr, "Failed writing data during exchange, errno: %s.\n",
            strerror(errno));
    return -1;
  }
  int total = 0;
  while (total < size) {
    int n = (int)read(sockfd, remoteData + total, (size_t)(size - total));
    if (n > 0) {
      total += n;
    } else {
      fprintf(stderr, "Failed reading data during exchange, errno: %s.\n",
              strerror(errno));
      return -1;
    }
  }
  return 0;
}

/* ---------------- PostJettyRw（对齐 yuanrong UrmaManager::PostJettyRw，WR
 * 构造） ---------------- */

// Build a BONDP WRITE work request. For WRITE, src SGEs describe local memory
// and dst SGEs describe the remote (imported) memory, matching YuanRong
// PostJettyRw's SGE direction.
static urma_jfs_wr_t *BuildBondpWr(bondp_jfs_wr_t *bondpWr,
                                   urma_target_jetty_t *targetJetty,
                                   urma_sge_t *srcSges, uint32_t numSges,
                                   urma_sge_t *dstSges, uint64_t userContext,
                                   uint32_t useDriverExt, uint32_t srcChipId,
                                   uint32_t dstChipId, uint32_t fence) {
  auto *wr = &bondpWr->base;
  wr->opcode = URMA_OPC_WRITE;
  wr->flag.bs.complete_enable = 1;
  wr->flag.bs.inline_flag = 0;
  wr->flag.bs.fence = fence != 0;
  wr->flag.bs.has_drv_ext = useDriverExt != 0;
  wr->tjetty = targetJetty;
  wr->user_ctx = userContext;
  wr->rw.src = {.sge = srcSges, .num_sge = numSges};
  wr->rw.dst = {.sge = dstSges, .num_sge = numSges};
  wr->next = nullptr;
  bondpWr->src_chip_id = srcChipId;
  bondpWr->dst_chip_id = dstChipId;
  return wr;
}

// 单 SGE WRITE（数据）
static urma_status_t
PostJettyRw(urma_jetty_t *jetty, urma_target_jetty_t *targetJetty,
            urma_target_seg_t *localSeg, urma_target_seg_t *remoteSeg,
            uint64_t localAddress, uint64_t remoteAddress, uint32_t length,
            uint64_t userContext, uint32_t useDriverExt, uint32_t srcChipId,
            uint32_t dstChipId, urma_jfs_wr_t **badWr) {
  if (jetty == nullptr || targetJetty == nullptr || localSeg == nullptr ||
      remoteSeg == nullptr || length == 0) {
    return URMA_EINVAL;
  }
  urma_sge_t localSge = {.addr = localAddress,
                         .len = length,
                         .tseg = localSeg,
                         .user_tseg = nullptr};
  urma_sge_t remoteSge = {.addr = remoteAddress,
                          .len = length,
                          .tseg = remoteSeg,
                          .user_tseg = nullptr};
  bondp_jfs_wr_t bondpWr{};
  urma_jfs_wr_t *wr =
      BuildBondpWr(&bondpWr, targetJetty, &localSge, 1, &remoteSge, userContext,
                   useDriverExt, srcChipId, dstChipId, 0);
  return urma_post_jetty_send_wr(jetty, wr, badWr);
}

// 单 WQE 2-sge：数据 + 8B 完成标志（接收方同一条 WR 内看到数据与标志）
static urma_status_t
PostJettyRwWithFlag(urma_jetty_t *jetty, urma_target_jetty_t *targetJetty,
                    urma_target_seg_t *localSeg, urma_target_seg_t *remoteSeg,
                    uint64_t localAddress, uint64_t remoteAddress,
                    uint32_t length, uint64_t flagLocalAddress,
                    uint64_t flagRemoteAddress, uint64_t userContext,
                    uint32_t useDriverExt, uint32_t srcChipId,
                    uint32_t dstChipId, urma_jfs_wr_t **badWr) {
  if (jetty == nullptr || targetJetty == nullptr || localSeg == nullptr ||
      remoteSeg == nullptr || length == 0) {
    return URMA_EINVAL;
  }
  urma_sge_t srcSges[2] = {{.addr = localAddress,
                            .len = length,
                            .tseg = localSeg,
                            .user_tseg = nullptr},
                           {.addr = flagLocalAddress,
                            .len = 8,
                            .tseg = localSeg,
                            .user_tseg = nullptr}};
  urma_sge_t dstSges[2] = {{.addr = remoteAddress,
                            .len = length,
                            .tseg = remoteSeg,
                            .user_tseg = nullptr},
                           {.addr = flagRemoteAddress,
                            .len = 8,
                            .tseg = remoteSeg,
                            .user_tseg = nullptr}};
  bondp_jfs_wr_t bondpWr{};
  urma_jfs_wr_t *wr =
      BuildBondpWr(&bondpWr, targetJetty, srcSges, 2, dstSges, userContext,
                   useDriverExt, srcChipId, dstChipId, 0);
  return urma_post_jetty_send_wr(jetty, wr, badWr);
}

// 数据 WR + fence 标志 WR（平台不保证单 WQE 内 sge 顺序时使用）
static urma_status_t
PostJettyRwFencedFlag(urma_jetty_t *jetty, urma_target_jetty_t *targetJetty,
                      urma_target_seg_t *localSeg, urma_target_seg_t *remoteSeg,
                      uint64_t localAddress, uint64_t remoteAddress,
                      uint32_t length, uint64_t flagLocalAddress,
                      uint64_t flagRemoteAddress, uint64_t userContext,
                      uint32_t useDriverExt, uint32_t srcChipId,
                      uint32_t dstChipId, urma_jfs_wr_t **badWr) {
  if (jetty == nullptr || targetJetty == nullptr || localSeg == nullptr ||
      remoteSeg == nullptr || length == 0) {
    return URMA_EINVAL;
  }
  // 1) data WR (no fence)
  urma_sge_t dataLocalSge = {.addr = localAddress,
                             .len = length,
                             .tseg = localSeg,
                             .user_tseg = nullptr};
  urma_sge_t dataRemoteSge = {.addr = remoteAddress,
                              .len = length,
                              .tseg = remoteSeg,
                              .user_tseg = nullptr};
  bondp_jfs_wr_t dataWr{};
  urma_jfs_wr_t *wr =
      BuildBondpWr(&dataWr, targetJetty, &dataLocalSge, 1, &dataRemoteSge,
                   userContext, useDriverExt, srcChipId, dstChipId, 0);
  urma_status_t ret = urma_post_jetty_send_wr(jetty, wr, badWr);
  if (ret != URMA_SUCCESS) {
    return ret;
  }
  // 2) 8-byte flag WR with fence (ordered after the data WR completes)
  urma_sge_t flagLocalSge = {.addr = flagLocalAddress,
                             .len = 8,
                             .tseg = localSeg,
                             .user_tseg = nullptr};
  urma_sge_t flagRemoteSge = {.addr = flagRemoteAddress,
                              .len = 8,
                              .tseg = remoteSeg,
                              .user_tseg = nullptr};
  bondp_jfs_wr_t flagWr{};
  wr = BuildBondpWr(&flagWr, targetJetty, &flagLocalSge, 1, &flagRemoteSge,
                    userContext, useDriverExt, srcChipId, dstChipId, 1);
  return urma_post_jetty_send_wr(jetty, wr, badWr);
}

} // namespace

/* ---------------- UrmaConnection ---------------- */

uint64_t UrmaConnection::RemoteSegVa() const {
  return remoteSeg != nullptr ? remoteSeg->Va() : 0;
}

/* ---------------- UrmaManager ---------------- */

UrmaManager::~UrmaManager() { Stop(); }

int UrmaManager::GetEidIndex(urma_device_t *dev) {
  uint32_t eidCnt = 0;
  urma_eid_info_t *eidList = urma_get_eid_list(dev, &eidCnt);
  if (eidList == nullptr || eidCnt == 0) {
    if (eidList != nullptr) {
      urma_free_eid_list(eidList);
    }
    return -1;
  }
  int index = eidList[0].eid_index;
  urma_free_eid_list(eidList);
  return index;
}

bool UrmaManager::InitUrmaLib() {
  urma_init_attr_t attr;
  (void)memset(&attr, 0, sizeof(attr));
  attr.uasid = 0;
  if (urma_init(&attr) != URMA_SUCCESS) {
    fprintf(stderr, "Failed to urma_init\n");
    return false;
  }
  return true;
}

bool UrmaManager::Init(const std::string &devName, bool cacheable,
                       uint32_t jettyCount, uint32_t threadsMin, bool eventMode,
                       uint32_t transMode) {
  if (initialized_) {
    return true;
  }
  eventMode_ = eventMode;
  if (!InitUrmaLib()) {
    return false;
  }
  urma_device_t *dev =
      urma_get_device_by_name(const_cast<char *>(devName.c_str()));
  if (dev == nullptr) {
    fprintf(stderr, "urma_get_device_by_name failed: %s\n", devName.c_str());
    return false;
  }
  int eidIndex = GetEidIndex(dev);
  if (eidIndex < 0) {
    fprintf(stderr, "Failed to get eid index\n");
    return false;
  }
  if (!resource_.Init(dev, (uint32_t)eidIndex, cacheable, jettyCount,
                      threadsMin, transMode)) {
    return false;
  }
  if (eventMode_ && !resource_.RearmJfc()) {
    fprintf(stderr, "Failed to rearm jfc\n");
    return false;
  }
  stop_ = false;
  initialized_ = true;
  return true;
}

void UrmaManager::Stop() {
  if (initialized_) {
    stop_ = true;
    if (pollStarted_) {
      if (pollThread_.joinable()) {
        pollThread_.join();
      }
      pollStarted_ = false;
    }
    resource_.Clear();
    initialized_ = false;
    (void)urma_uninit();
  }
}

bool UrmaManager::RegisterBuffer(void *va, uint64_t len) {
  std::shared_ptr<UrmaLocalSegment> seg;
  if (!resource_.RegisterSegment((uint64_t)va, len, seg)) {
    return false;
  }
  buf_ = va;
  bufLen_ = len;
  localSeg_ = seg;
  return true;
}

urma_target_seg_t *UrmaManager::LocalSeg() const {
  return localSeg_ != nullptr ? localSeg_->Raw() : nullptr;
}

/* ---------------- 事件槽 ---------------- */

bool UrmaManager::RegisterWorker(uint32_t &workerId) {
  bool first = false;
  {
    std::lock_guard<std::mutex> lock(workerMutex_);
    workerSlots_.push_back(std::make_unique<EventSlots>(kEventSlotsPerWorker));
    workerId = static_cast<uint32_t>(workerSlots_.size() - 1);
    first = (workerSlots_.size() == 1);
  }
  if (first) {
    EnsurePollThread();
  }
  return true;
}

void UrmaManager::UnregisterWorker(uint32_t workerId) {
  /* 事件槽保留到 Stop（打流线程全部结束后统一清理）；此处仅占位 */
  (void)workerId;
}

uint32_t UrmaManager::WorkerCount() const {
  return static_cast<uint32_t>(workerSlots_.size());
}

uint64_t UrmaManager::PostEvent(uint32_t workerId, uint64_t &seq) {
  if (workerId >= workerSlots_.size() || workerSlots_[workerId] == nullptr) {
    return 0;
  }
  EventSlots &slots = *workerSlots_[workerId];
  seq = slots.localSeq.fetch_add(1) & kRidMask;
  uint64_t *slot = &slots.slots[seq % kEventSlotsPerWorker];
  __atomic_store_n(slot, seq << 2, __ATOMIC_RELEASE);
  return ((uint64_t)workerId << kRidShift) | seq;
}

bool UrmaManager::WaitEvent(uint32_t workerId, uint64_t seq, int timeoutMs) {
  if (workerId >= workerSlots_.size() || workerSlots_[workerId] == nullptr) {
    return false;
  }
  uint64_t *slot = &workerSlots_[workerId]->slots[seq % kEventSlotsPerWorker];
  uint64_t doneOk = (seq << 2) | 1;
  uint64_t doneFail = (seq << 2) | 2;
  uint64_t deadline = NowNs() + (uint64_t)timeoutMs * 1000000ULL;
  bool ok = false;
  for (;;) {
    uint64_t v = __atomic_load_n(slot, __ATOMIC_ACQUIRE);
    if (v == doneOk) {
      ok = true;
      break;
    }
    if (v == doneFail) {
      break;
    }
    if (NowNs() >= deadline) {
      break;
    }
    SleepNs(kPollSleepNs);
  }
  __atomic_store_n(slot, 0, __ATOMIC_RELEASE); /* 超时/完成后复位，允许槽复用 */
  return ok;
}

void UrmaManager::AbortEvent(uint32_t workerId, uint64_t seq) {
  if (workerId >= workerSlots_.size() || workerSlots_[workerId] == nullptr) {
    return;
  }
  uint64_t *slot = &workerSlots_[workerId]->slots[seq % kEventSlotsPerWorker];
  __atomic_store_n(slot, 0, __ATOMIC_RELEASE);
}

void UrmaManager::CompleteEvent(uint32_t workerId, uint64_t userCtx, bool ok) {
  uint64_t seq = userCtx & kRidMask;
  if ((userCtx >> kRidShift) != workerId) {
    return;
  }
  std::lock_guard<std::mutex> lock(workerMutex_);
  if (workerId >= workerSlots_.size() || workerSlots_[workerId] == nullptr) {
    return;
  }
  uint64_t *slot = &workerSlots_[workerId]->slots[seq % kEventSlotsPerWorker];
  uint64_t expect = seq << 2;
  uint64_t want = (seq << 2) | (ok ? 1 : 2);
  __atomic_compare_exchange_n(slot, &expect, want, false, __ATOMIC_ACQ_REL,
                              __ATOMIC_ACQUIRE);
}

/* ---------------- 轮询线程 ---------------- */

bool UrmaManager::EnsurePollThread() {
  if (pollStarted_) {
    return true;
  }
  try {
    pollThread_ = std::thread(&UrmaManager::PollThreadMain, this);
    pollStarted_ = true;
    return true;
  } catch (...) {
    fprintf(stderr, "Failed to start poll thread\n");
    return false;
  }
}

void UrmaManager::PollThreadMain() {
  urma_cr_t crs[kMaxPollCr];
  while (!stop_) {
    int cnt;
    if (eventMode_) {
      urma_jfc_t *evJfc = nullptr;
      cnt = urma_wait_jfc(resource_.Jfce(), 1, 100, &evJfc);
      if (cnt <= 0) {
        continue;
      }
      if (resource_.Jfc() != evJfc) {
        continue;
      }
      cnt = urma_poll_jfc(resource_.Jfc(), kMaxPollCr, crs);
      if (cnt > 0) {
        uint32_t ack = (uint32_t)cnt;
        urma_ack_jfc(&evJfc, &ack, 1);
        (void)urma_rearm_jfc(resource_.Jfc(), false);
      }
    } else {
      cnt = urma_poll_jfc(resource_.Jfc(), kMaxPollCr, crs);
    }
    if (cnt > 0) {
      for (int i = 0; i < cnt; i++) {
        uint64_t userCtx = crs[i].user_ctx;
        uint32_t wid = (uint32_t)(userCtx >> kRidShift);
        CompleteEvent(wid, userCtx, crs[i].status == URMA_CR_SUCCESS);
      }
    } else if (cnt == 0) {
      SleepNs(kPollSleepNs);
    } else {
      fprintf(stderr, "Failed to poll jfc, ret=%d\n", cnt);
      SleepNs(1000 * kPollSleepNs);
    }
  }
}

/* ---------------- send lane（每轮从池取一条 jetty） ---------------- */

bool UrmaManager::AcquireSendLane(std::shared_ptr<UrmaJetty> &jetty) {
  return resource_.AcquireSendJetty(jetty);
}

void UrmaManager::ReleaseSendLane(const std::shared_ptr<UrmaJetty> &jetty) {
  resource_.ReleaseSendJetty(jetty);
}

/* ---------------- 握手 ---------------- */

bool UrmaManager::Exchange(int sockfd, const HandshakeParams &params,
                           std::shared_ptr<UrmaConnection> &conn) {
  if (localSeg_ == nullptr || resource_.Ctx() == nullptr) {
    fprintf(stderr, "Exchange before RegisterBuffer/Init\n");
    return false;
  }
  urma_target_seg_t *local = localSeg_->Raw();

  WireInfo localWire;
  (void)memset(&localWire, 0, sizeof(localWire));
  localWire.eid = resource_.Eid();
  localWire.uasid = resource_.Uasid();
  localWire.seg_va = local->seg.ubva.va;
  localWire.seg_len = local->seg.len;
  localWire.seg_flag = local->seg.attr.value;
  localWire.seg_token_id = local->seg.token_id;
  {
    std::shared_ptr<UrmaJetty> jetty0 = resource_.JettyAt(0);
    if (jetty0 != nullptr) {
      localWire.jetty_id =
          jetty0->Raw()->jetty_id; /* 发布 jetty[0] 供对端 import */
    }
  }
  localWire.threads = params.threads;
  localWire.op_code = params.opCode;
  localWire.dual_mode = params.dualMode;
  localWire.value_size = params.valueSize;
  localWire.dst_chip = params.dstChip;

  WireInfo remoteWire;
  (void)memset(&remoteWire, 0, sizeof(remoteWire));
  if (SockSyncData(sockfd, (int)sizeof(WireInfo), (char *)&localWire,
                   (char *)&remoteWire) != 0) {
    return false;
  }

  printf("exchange: remote seg va=0x%lx len=%lu jetty=%u dst_chip=%u "
         "threads=%u op=%u\n",
         (uint64_t)remoteWire.seg_va, (uint64_t)remoteWire.seg_len,
         remoteWire.jetty_id.id, remoteWire.dst_chip, remoteWire.threads,
         remoteWire.op_code);

  auto newConn = std::make_shared<UrmaConnection>();
  newConn->peer.eid = remoteWire.eid;
  newConn->peer.uasid = remoteWire.uasid;
  newConn->peer.jettyId = remoteWire.jetty_id.id;
  newConn->peer.dstChip = remoteWire.dst_chip;
  newConn->peer.threads = remoteWire.threads;
  newConn->peer.opCode = remoteWire.op_code;
  newConn->peer.dualMode = remoteWire.dual_mode;
  newConn->peer.valueSize = remoteWire.value_size;
  newConn->peer.seg.ubva.eid = remoteWire.eid;
  newConn->peer.seg.ubva.uasid = remoteWire.uasid;
  newConn->peer.seg.ubva.va = remoteWire.seg_va;
  newConn->peer.seg.len = remoteWire.seg_len;
  newConn->peer.seg.attr.value = remoteWire.seg_flag;
  newConn->peer.seg.token_id = remoteWire.seg_token_id;

  /* import 对端 jetty（legacy handshake，对齐 yuanrong BuildRemoteJetty +
   * ImportTargetJetty） */
  urma_rjetty_t rjetty;
  (void)memset(&rjetty, 0, sizeof(rjetty));
  rjetty.jetty_id = remoteWire.jetty_id;
  rjetty.trans_mode = ResolveTransMode(params.transMode);
  rjetty.type = URMA_JETTY;
  rjetty.tp_type = URMA_RTP;
  rjetty.flag.bs.order_type = params.transMode == 3 ? 1 : 0;
  rjetty.flag.bs.share_tp = params.transMode == 3 ? 1 : 0;
  if (!resource_.ImportTargetJetty(&rjetty, newConn->targetJetty)) {
    return false;
  }
  if (!resource_.ImportSegment(newConn->peer.seg, newConn->remoteSeg)) {
    return false;
  }

  /* RC 模式: 绑定所有本地 jetty 到对端 target jetty（对齐 SDK 示例） */
  if (params.transMode == 1 || params.transMode == 3) {
    uint32_t n = resource_.SendJettyCount();
    for (uint32_t i = 0; i < n; i++) {
      std::shared_ptr<UrmaJetty> jetty = resource_.JettyAt(i);
      if (jetty != nullptr &&
          urma_bind_jetty(jetty->Raw(), newConn->targetJetty->Raw()) !=
              URMA_SUCCESS) {
        fprintf(stderr, "Failed to bind jetty to remote target jetty\n");
        return false;
      }
    }
    printf("bind %u jettys to remote jetty\n", n);
  }
  conn = std::move(newConn);
  return true;
}

bool UrmaManager::ExchangeAsClient(int sockfd, const HandshakeParams &params,
                                   std::shared_ptr<UrmaConnection> &conn) {
  return Exchange(sockfd, params, conn);
}

bool UrmaManager::ExchangeAsServer(int sockfd, const HandshakeParams &params,
                                   std::shared_ptr<UrmaConnection> &conn) {
  return Exchange(sockfd, params, conn);
}

/* ---------------- 数据面 ---------------- */

bool UrmaManager::PostWrite(const std::shared_ptr<UrmaJetty> &jetty,
                            UrmaConnection &conn, uint64_t localAddr,
                            uint64_t remoteAddr, uint32_t len, uint32_t srcChip,
                            uint32_t dstChip, uint64_t userCtx) {
  if (jetty == nullptr || localSeg_ == nullptr || conn.targetJetty == nullptr ||
      conn.remoteSeg == nullptr) {
    return false;
  }
  uint32_t drv = (srcChip != kInvalidChip && dstChip != kInvalidChip) ? 1 : 0;
  urma_jfs_wr_t *bad = nullptr;
  urma_status_t rc =
      PostJettyRw(jetty->Raw(), conn.targetJetty->Raw(), localSeg_->Raw(),
                  conn.remoteSeg->Raw(), localAddr, remoteAddr, len, userCtx,
                  drv, srcChip, dstChip, &bad);
  if (rc != URMA_SUCCESS) {
    fprintf(stderr, "[wr] post failed rc=%d\n", rc);
  }
  return rc == URMA_SUCCESS;
}

bool UrmaManager::PostWriteWithFlag(const std::shared_ptr<UrmaJetty> &jetty,
                                    UrmaConnection &conn, uint64_t localAddr,
                                    uint64_t remoteAddr, uint32_t len,
                                    uint64_t flagLocalAddr,
                                    uint64_t flagRemoteAddr, uint32_t srcChip,
                                    uint32_t dstChip, uint64_t userCtx) {
  if (jetty == nullptr || localSeg_ == nullptr || conn.targetJetty == nullptr ||
      conn.remoteSeg == nullptr) {
    return false;
  }
  uint32_t drv = (srcChip != kInvalidChip && dstChip != kInvalidChip) ? 1 : 0;
  urma_jfs_wr_t *bad = nullptr;
  urma_status_t rc = PostJettyRwWithFlag(
      jetty->Raw(), conn.targetJetty->Raw(), localSeg_->Raw(),
      conn.remoteSeg->Raw(), localAddr, remoteAddr, len, flagLocalAddr,
      flagRemoteAddr, userCtx, drv, srcChip, dstChip, &bad);
  if (rc != URMA_SUCCESS) {
    fprintf(stderr, "[wr+flag] post failed rc=%d\n", rc);
  }
  return rc == URMA_SUCCESS;
}

bool UrmaManager::PostWriteFencedFlag(const std::shared_ptr<UrmaJetty> &jetty,
                                      UrmaConnection &conn, uint64_t localAddr,
                                      uint64_t remoteAddr, uint32_t len,
                                      uint64_t flagLocalAddr,
                                      uint64_t flagRemoteAddr, uint32_t srcChip,
                                      uint32_t dstChip, uint64_t userCtx) {
  if (jetty == nullptr || localSeg_ == nullptr || conn.targetJetty == nullptr ||
      conn.remoteSeg == nullptr) {
    return false;
  }
  uint32_t drv = (srcChip != kInvalidChip && dstChip != kInvalidChip) ? 1 : 0;
  urma_jfs_wr_t *bad = nullptr;
  urma_status_t rc = PostJettyRwFencedFlag(
      jetty->Raw(), conn.targetJetty->Raw(), localSeg_->Raw(),
      conn.remoteSeg->Raw(), localAddr, remoteAddr, len, flagLocalAddr,
      flagRemoteAddr, userCtx, drv, srcChip, dstChip, &bad);
  if (rc != URMA_SUCCESS) {
    fprintf(stderr, "[wr+fence] post failed rc=%d\n", rc);
  }
  return rc == URMA_SUCCESS;
}

} // namespace kv_bench
