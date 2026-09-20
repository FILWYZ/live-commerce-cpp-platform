#pragma once

#include "cache/sharded_cache.h"
#include "common/error/status.h"
#include "kv/kv_store.h"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace live::storage { class LocalKVEngine; }

namespace live::business::product {

struct Product {
    std::string id;
    std::string name;
    std::int64_t price_cents{0};
    bool on_sale{false};
};

class ProductService {
public:
    explicit ProductService(cache::ShardedCache* cache = nullptr, kv::IKVStore* store = nullptr,
                            ::live::storage::LocalKVEngine* storage = nullptr)
        : cache_(cache), store_(store), storage_(storage) {}

    common::Status addProduct(Product product);
    common::Status restore();
    common::Status getProduct(const std::string& id, Product* product);
    common::Status setOnSale(const std::string& id, bool on_sale);

private:
    static std::string serialize(const Product& product);
    static bool deserialize(const std::string& value, Product* product);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Product> products_;
    cache::ShardedCache* cache_{nullptr};
    kv::IKVStore* store_{nullptr};
    ::live::storage::LocalKVEngine* storage_{nullptr};
};

}  // namespace live::business::product
