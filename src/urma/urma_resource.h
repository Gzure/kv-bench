/*
 * kv-bench URMA 资源层（对齐 yuanrong-datasystem
 * src/datasystem/common/rdma/urma_resource.h 精简）。
 *
 * 职责：独占持有底层 liburma 句柄（context/jfce/jfc/jfr/jetty/segment），
 * 提供进程级 send Jetty 池与 import/register 能力。去掉 yuanrong 的故障生命周期
 * （PostGate 状态机/退休/补池/异步事件），bench 场景不做故障恢复。
 */
#ifndef KV_BENCH_URMA_RESOURCE_H
#define KV_BENCH_URMA_RESOURCE_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "urma_api.h"
#include "urma_send_lane.h"

namespace kv_bench {

constexpr uint32_t kUrmaJettySize = 256; /* 对齐 yuanrong JETTY_SIZE */
constexpr uint32_t kSharedJfrDepth = 32; /* 对齐 yuanrong SHARED_JFR_DEPTH */
constexpr uint32_t kUrmaMaxSge = 2; /* 本工具单 WR 最大 sge 数（数据+标志） */
constexpr uint32_t kInvalidChip = 0xFF;

/* ---------------- RAII 句柄包装 ---------------- */

class UrmaContext {
public:
  static bool Create(urma_device_t *dev, uint32_t eidIndex,
                     std::unique_ptr<UrmaContext> &ctx);
  ~UrmaContext();

  urma_context_t *Raw() const { return raw_; }
  urma_eid_t Eid() const { return raw_ != nullptr ? raw_->eid : urma_eid_t{}; }
  uint32_t Uasid() const { return raw_ != nullptr ? raw_->uasid : 0; }

private:
  explicit UrmaContext(urma_context_t *raw) : raw_(raw) {}
  urma_context_t *raw_;
};

class UrmaJfce {
public:
  static bool Create(urma_context_t *ctx, std::unique_ptr<UrmaJfce> &jfce);
  ~UrmaJfce();

  urma_jfce_t *Raw() const { return raw_; }

private:
  explicit UrmaJfce(urma_jfce_t *raw) : raw_(raw) {}
  urma_jfce_t *raw_;
};

class UrmaJfc {
public:
  static bool Create(urma_context_t *ctx, const urma_device_attr_t &devAttr,
                     std::unique_ptr<UrmaJfc> &jfc);
  ~UrmaJfc();

  bool Rearm() const;
  urma_jfc_t *Raw() const { return raw_; }

private:
  explicit UrmaJfc(urma_jfc_t *raw) : raw_(raw) {}
  urma_jfc_t *raw_;
};

class UrmaJfr {
public:
  static bool Create(urma_context_t *ctx, uint32_t depth,
                     const urma_token_t &token, urma_jfc_t *jfc,
                     uint32_t transMode, std::shared_ptr<UrmaJfr> &jfr);
  ~UrmaJfr();

  urma_jfr_t *Raw() const { return raw_; }

private:
  explicit UrmaJfr(urma_jfr_t *raw) : raw_(raw) {}
  urma_jfr_t *raw_;
};

/* Jetty：send jetty 共享 context 级 JFR；recv jetty 持有独立 JFR（对齐 yuanrong
 * UrmaJetty::Create） */
class UrmaJetty {
public:
  static bool Create(class UrmaResource &res, bool isSend,
                     std::shared_ptr<UrmaJetty> &out);
  ~UrmaJetty();

  urma_jetty_t *Raw() const { return raw_; }
  uint32_t JettyId() const { return raw_ != nullptr ? raw_->jetty_id.id : 0; }

private:
  UrmaJetty(urma_jetty_t *raw, std::shared_ptr<UrmaJfr> jfr)
      : raw_(raw), jfr_(std::move(jfr)) {}
  urma_jetty_t *raw_;
  std::shared_ptr<UrmaJfr> jfr_; /* 保持 JFR 存活 */
};

class UrmaTargetJetty {
public:
  static bool Import(urma_context_t *ctx, const urma_rjetty_t *remoteJetty,
                     const urma_token_t &token,
                     std::unique_ptr<UrmaTargetJetty> &out);
  ~UrmaTargetJetty();

  urma_target_jetty_t *Raw() const { return raw_; }

private:
  explicit UrmaTargetJetty(urma_target_jetty_t *raw) : raw_(raw) {}
  urma_target_jetty_t *raw_;
};

class UrmaLocalSegment {
public:
  static bool Register(urma_context_t *ctx, uint64_t va, uint64_t len,
                       const urma_token_t &token, bool cacheable,
                       std::shared_ptr<UrmaLocalSegment> &out);
  ~UrmaLocalSegment();

  urma_target_seg_t *Raw() const { return raw_; }
  uint64_t Va() const;
  uint64_t Len() const;

private:
  explicit UrmaLocalSegment(urma_target_seg_t *raw) : raw_(raw) {}
  urma_target_seg_t *raw_;
};

class UrmaRemoteSegment {
public:
  static bool Import(urma_context_t *ctx, const urma_seg_t &seg,
                     const urma_token_t &token, bool cacheable,
                     std::shared_ptr<UrmaRemoteSegment> &out);
  ~UrmaRemoteSegment();

  urma_target_seg_t *Raw() const { return raw_; }
  uint64_t Va() const;

private:
  explicit UrmaRemoteSegment(urma_target_seg_t *raw) : raw_(raw) {}
  urma_target_seg_t *raw_;
};

/* ---------------- 资源门面 ---------------- */

class UrmaResource {
public:
  UrmaResource() = default;
  ~UrmaResource();
  UrmaResource(const UrmaResource &) = delete;
  UrmaResource &operator=(const UrmaResource &) = delete;

  friend class UrmaJetty; /* UrmaJetty::Create 访问共享 JFR */

  /* 设备查询 + context/jfce/jfc/jfr + send jetty 池预填（对齐 yuanrong
   * UrmaResource::Init L724） */
  bool Init(urma_device_t *device, uint32_t eidIndex, bool cacheable,
            uint32_t jettyCount, uint32_t minJettys, uint32_t transMode = 0);
  void Clear();

  urma_context_t *Ctx() const;
  urma_jfce_t *Jfce() const;
  urma_jfc_t *Jfc() const;
  const urma_token_t &Token() const { return token_; }
  uint64_t MaxWriteSize() const;
  uint32_t TransMode() const { return transMode_; }

  /* 本地段注册（bench 单段） */
  bool RegisterSegment(uint64_t va, uint64_t len,
                       std::shared_ptr<UrmaLocalSegment> &out);

  /* jetty 创建与池（SendJettyPool 线程安全内建；每轮 PopIdle 取"新的"空闲
   * jetty） */
  bool CreateSendJetty(std::shared_ptr<UrmaJetty> &out);
  uint32_t SendJettyCount() const;
  std::shared_ptr<UrmaJetty> JettyAt(uint32_t index) const;
  SendJettyPool::Stats SendJettyPoolStats() const;
  bool AcquireSendJetty(std::shared_ptr<UrmaJetty> &out);
  void ReleaseSendJetty(const std::shared_ptr<UrmaJetty> &jetty);

  /* 对端 import */
  bool ImportTargetJetty(const urma_rjetty_t *remoteJetty,
                         std::unique_ptr<UrmaTargetJetty> &out);
  bool ImportSegment(const urma_seg_t &seg,
                     std::shared_ptr<UrmaRemoteSegment> &out);

  /* 握手发布信息 */
  urma_eid_t Eid() const;
  uint32_t Uasid() const;
  uint32_t PublishedJettyId() const; /* jetty[0] */

private:
  bool PreFillSendJettyPool(uint32_t count);

  std::unique_ptr<UrmaContext> context_;
  std::unique_ptr<UrmaJfce> jfce_;
  std::unique_ptr<UrmaJfc> jfc_;
  std::shared_ptr<UrmaJfr> sharedJettyJfr_;
  SendJettyPool sendJettyPool_;
  urma_token_t token_{};
  urma_device_attr_t devAttr_{};
  uint64_t maxWriteSize_{0};
  uint32_t transMode_{0};
  bool cacheable_{false};
  std::shared_ptr<UrmaLocalSegment> localSeg_;
};

} // namespace kv_bench

#endif /* KV_BENCH_URMA_RESOURCE_H */
