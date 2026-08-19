/*
 * kv-bench send Jetty 池（对齐 yuanrong-datasystem src/datasystem/common/rdma/urma_send_lane.h
 * 的 SendJettyPool 结构，线程安全内建于对象内部）。
 *
 * 与 yuanrong 差异：
 *  - 内部自带 std::mutex（yuanrong 由 UrmaResource 在外部持锁，这里收进对象本身）；
 *  - PopIdle 按游标轮转取空闲 jetty，保证"每轮都取到新的"（多线程下同时天然分散）。
 */
#ifndef KV_BENCH_URMA_SEND_LANE_H
#define KV_BENCH_URMA_SEND_LANE_H

#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

namespace kv_bench {

class UrmaJetty; /* 前向声明，池只持有 shared_ptr */

class SendJettyPool {
public:
    struct Stats {
        size_t poolSize = 0;
        size_t idleCount = 0;
        size_t inUseCount = 0;
    };

    SendJettyPool() = default;
    ~SendJettyPool() = default;
    SendJettyPool(const SendJettyPool &) = delete;
    SendJettyPool &operator=(const SendJettyPool &) = delete;

    void Clear()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        jettys_.clear();
        idleIndices_.clear();
        cursor_ = 0;
    }

    void Add(std::shared_ptr<UrmaJetty> jetty)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        idleIndices_.push_back(jettys_.size());
        jettys_.push_back(std::move(jetty));
    }

    /* 取一条空闲 jetty：从游标起找第一个空闲下标并移除，游标前移（每轮"新的"） */
    bool PopIdle(std::shared_ptr<UrmaJetty> &jetty)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const size_t n = jettys_.size();
        for (size_t i = 0; i < n; i++) {
            const size_t idx = (cursor_ + i) % n;
            auto it = std::find(idleIndices_.begin(), idleIndices_.end(), idx);
            if (it == idleIndices_.end()) {
                continue;
            }
            idleIndices_.erase(it);
            cursor_ = idx + 1;
            jetty = jettys_[idx];
            return true;
        }
        return false; /* 池耗尽 */
    }

    /* 归还：jetty 回到空闲集合 */
    void Release(const std::shared_ptr<UrmaJetty> &jetty)
    {
        if (jetty == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        for (size_t i = 0; i < jettys_.size(); i++) {
            if (jettys_[i] == jetty) {
                if (std::find(idleIndices_.begin(), idleIndices_.end(), i) == idleIndices_.end()) {
                    idleIndices_.push_back(i);
                }
                return;
            }
        }
    }

    /* 从池移除（对齐 yuanrong Remove：末尾交换 + 下标修正） */
    bool Remove(const std::shared_ptr<UrmaJetty> &jetty)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (size_t i = 0; i < jettys_.size(); i++) {
            if (jettys_[i] != jetty) {
                continue;
            }
            idleIndices_.erase(std::remove(idleIndices_.begin(), idleIndices_.end(), i), idleIndices_.end());
            const size_t lastIdx = jettys_.size() - 1;
            if (i != lastIdx) {
                jettys_[i] = std::move(jettys_[lastIdx]);
                std::replace(idleIndices_.begin(), idleIndices_.end(), lastIdx, i);
            }
            jettys_.pop_back();
            return true;
        }
        return false;
    }

    Stats GetStats() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        Stats stats;
        stats.poolSize = jettys_.size();
        stats.idleCount = idleIndices_.size();
        stats.inUseCount = stats.poolSize > stats.idleCount ? stats.poolSize - stats.idleCount : 0;
        return stats;
    }

    /* 按下标访问（发布 jetty[0] / RC 绑定用），越界返回 nullptr */
    std::shared_ptr<UrmaJetty> At(size_t index) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return index < jettys_.size() ? jettys_[index] : nullptr;
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::shared_ptr<UrmaJetty>> jettys_;
    std::vector<size_t> idleIndices_;
    size_t cursor_{0};
};

}  // namespace kv_bench

#endif /* KV_BENCH_URMA_SEND_LANE_H */
