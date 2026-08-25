/*
 * kv-bench send Jetty 池（对齐 yuanrong-datasystem src/datasystem/common/rdma/urma_send_lane.h
 * 的 SendJettyPool 结构，线程安全内建于对象内部）。
 *
 * 与 yuanrong 差异：
 *  - 内部自带 std::mutex（yuanrong 由 UrmaResource 在外部持锁，这里收进对象本身）；
 *  - PopIdle 按 FIFO 空闲队列轮转 jetty，保证"每轮都取到新的"（多线程下同时天然分散）。
 */
#ifndef KV_BENCH_URMA_SEND_LANE_H
#define KV_BENCH_URMA_SEND_LANE_H

#include <algorithm>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
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
        isIdle_.clear();
        jettyIndices_.clear();
    }

    void Add(std::shared_ptr<UrmaJetty> jetty)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const size_t index = jettys_.size();
        jettyIndices_[jetty.get()] = index;
        idleIndices_.push_back(index);
        isIdle_.push_back(true);
        jettys_.push_back(std::move(jetty));
    }

    /* FIFO 轮转取空闲 jetty：获取/归还均为 O(1)，每轮仍优先取最早归还的 jetty。 */
    bool PopIdle(std::shared_ptr<UrmaJetty> &jetty)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        while (!idleIndices_.empty()) {
            const size_t idx = idleIndices_.front();
            idleIndices_.pop_front();
            if (idx >= jettys_.size() || !isIdle_[idx]) {
                continue;
            }
            isIdle_[idx] = false;
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
        auto it = jettyIndices_.find(jetty.get());
        if (it == jettyIndices_.end() || isIdle_[it->second]) {
            return;
        }
        isIdle_[it->second] = true;
        idleIndices_.push_back(it->second);
    }

    /* 从池移除（对齐 yuanrong Remove：末尾交换 + 下标修正） */
    bool Remove(const std::shared_ptr<UrmaJetty> &jetty)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto jettyIt = jettyIndices_.find(jetty.get());
        if (jettyIt == jettyIndices_.end()) {
            return false;
        }

        const size_t index = jettyIt->second;
        const size_t lastIndex = jettys_.size() - 1;
        idleIndices_.erase(std::remove(idleIndices_.begin(), idleIndices_.end(), index), idleIndices_.end());
        jettyIndices_.erase(jettyIt);
        if (index != lastIndex) {
            jettys_[index] = std::move(jettys_[lastIndex]);
            isIdle_[index] = isIdle_[lastIndex];
            jettyIndices_[jettys_[index].get()] = index;
            std::replace(idleIndices_.begin(), idleIndices_.end(), lastIndex, index);
        }
        jettys_.pop_back();
        isIdle_.pop_back();
        return true;
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
    std::deque<size_t> idleIndices_;
    std::vector<bool> isIdle_;
    std::unordered_map<const UrmaJetty *, size_t> jettyIndices_;
};

}  // namespace kv_bench

#endif /* KV_BENCH_URMA_SEND_LANE_H */
