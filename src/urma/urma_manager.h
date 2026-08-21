/*
 * kv-bench URMA 管理层（对齐 yuanrong-datasystem
 * src/datasystem/common/rdma/urma_manager.h 精简）。
 *
 * 职责：URMA 库初始化/设备发现、bench 缓冲注册、TCP 控制面握手（交换
 * jetty/segment 元信息并 import 对端资源）、数据面 PostWrite 系列（含 bonding
 * chip 字段）、 完成模型（独立轮询线程 + 每打流线程事件槽）与清理。业务层只与
 * UrmaManager 交互。
 */
#ifndef KV_BENCH_URMA_MANAGER_H
#define KV_BENCH_URMA_MANAGER_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "urma_api.h"
#include "urma_resource.h"

namespace kv_bench {

constexpr uint32_t kEventSlotsPerWorker = 256;
constexpr uint32_t kRidShift = 40;
constexpr uint64_t kRidMask = (1ULL << kRidShift) - 1;

/* 握手参数（业务层传入，随控制面消息交换） */
struct HandshakeParams {
  uint32_t threads = 1;
  uint32_t opCode = 0;
  uint32_t valueSize = 0;
  uint32_t dstChip = kInvalidChip; /* 本进程数据落地目的 chip */
  uint32_t transMode = 0; /* 0=RM 1=RC 2=UM 3=RS（RC 时绑定本地 jetty） */
};

/* 对端元信息（握手后持有） */
struct UrmaPeerInfo {
  urma_eid_t eid{};
  uint32_t uasid = 0;
  uint32_t jettyId = 0;
  uint32_t dstChip = kInvalidChip;
  uint32_t threads = 1;  /* 对端打流线程数 */
  uint32_t opCode = 0;   /* 对端 op（write/get/mixed） */
  uint32_t valueSize = 0;
  urma_seg_t seg{};
};

/* 连接：对端 import 出的 target jetty + segment（对齐 yuanrong UrmaConnection）
 */
class UrmaConnection {
public:
  UrmaPeerInfo peer;
  std::unique_ptr<UrmaTargetJetty> targetJetty;
  std::shared_ptr<UrmaRemoteSegment> remoteSeg;

  uint64_t RemoteSegVa() const;
};

class UrmaManager {
public:
  UrmaManager() = default;
  ~UrmaManager();
  UrmaManager(const UrmaManager &) = delete;
  UrmaManager &operator=(const UrmaManager &) = delete;

  /* urma_init -> 设备发现(eid) -> UrmaResource::Init ->
   * (首个打流线程注册时起轮询线程) */
  bool Init(const std::string &devName, bool cacheable, uint32_t jettyCount,
            uint32_t threadsMin, bool eventMode = false,
            uint32_t transMode = 0);
  void Stop(); /* 停轮询线程 + 资源清理 + urma_uninit */

  UrmaResource &Resource() { return resource_; }
  const UrmaResource &Resource() const { return resource_; }

  /* bench 缓冲注册（单个本地段） */
  bool RegisterBuffer(void *va, uint64_t len);
  void *Buf() const { return buf_; }
  uint64_t BufLen() const { return bufLen_; }
  urma_target_seg_t *LocalSeg() const;

  /* 打流线程事件槽 */
  bool RegisterWorker(uint32_t &workerId);
  void UnregisterWorker(uint32_t workerId);
  uint32_t WorkerCount() const;
  uint64_t PostEvent(uint32_t workerId, uint64_t &seq); /* 返回 user_ctx */
  bool WaitEvent(uint32_t workerId, uint64_t seq, int timeoutMs);
  /* 非阻塞探测事件槽：1=成功完成（槽已复位），-1=失败完成，0=未完成 */
  int ProbeEvent(uint32_t workerId, uint64_t seq);
  void AbortEvent(uint32_t workerId, uint64_t seq);

  /* TCP 控制面握手：交换元信息 + import 对端 jetty/segment。
   * preferRtp=true 时用普通 RTP import（SDK 示例），跳过 bondp/CTP
   * （部分平台驱动对 bondp import 有问题时可绕行） */
  bool ExchangeAsClient(int sockfd, const HandshakeParams &params,
                        std::shared_ptr<UrmaConnection> &conn,
                        bool preferRtp = false);
  bool ExchangeAsServer(int sockfd, const HandshakeParams &params,
                        std::shared_ptr<UrmaConnection> &conn,
                        bool preferRtp = false);

  /* send lane：每轮（一次业务请求）从池取一条 jetty，用后归还（对齐 yuanrong
   * AcquireSendLane） */
  bool AcquireSendLane(std::shared_ptr<UrmaJetty> &jetty);
  void ReleaseSendLane(const std::shared_ptr<UrmaJetty> &jetty);

  /* 数据面（jetty 由 lane 提供; bonding chip 路由） */
  bool PostWrite(const std::shared_ptr<UrmaJetty> &jetty, UrmaConnection &conn,
                 uint64_t localAddr, uint64_t remoteAddr, uint32_t len,
                 uint32_t srcChip, uint32_t dstChip, uint64_t userCtx);
  /* READ：从对端段读 len 字节到本地（src/dst sge 对调，对齐参考 UrmaRead） */
  bool PostRead(const std::shared_ptr<UrmaJetty> &jetty, UrmaConnection &conn,
                uint64_t localAddr, uint64_t remoteAddr, uint32_t len,
                uint32_t srcChip, uint32_t dstChip, uint64_t userCtx);

private:
  struct EventSlots {
    std::vector<uint64_t> slots;
    std::atomic<uint64_t> localSeq{0};
    explicit EventSlots(uint32_t count) : slots(count, 0) {}
  };

  bool InitUrmaLib();
  bool EnsurePollThread();
  void PollThreadMain();
  void CompleteEvent(uint32_t workerId, uint64_t userCtx, bool ok);
  bool Exchange(int sockfd, const HandshakeParams &params,
                std::shared_ptr<UrmaConnection> &conn,
                bool preferRtp = false);
  static int GetEidIndex(urma_device_t *dev);

  UrmaResource resource_;
  bool initialized_{false};
  bool pollStarted_{false};
  bool stop_{false};
  bool eventMode_{false};
  std::thread pollThread_;

  void *buf_{nullptr};
  uint64_t bufLen_{0};
  std::shared_ptr<UrmaLocalSegment> localSeg_;

  std::vector<std::unique_ptr<EventSlots>> workerSlots_;
  mutable std::mutex workerMutex_;
};

} // namespace kv_bench

#endif /* KV_BENCH_URMA_MANAGER_H */
