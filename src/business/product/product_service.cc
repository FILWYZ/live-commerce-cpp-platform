#include "business/product/product_service.h"

#include "storage/local_engine/local_kv_engine.h"

#include <sstream>
#include <utility>

namespace live::business::product {

common::Status ProductService::addProduct(Product product) {
    if (product.id.empty() || product.name.empty() || product.price_cents < 0) {
        return common::Status::InvalidArgument("invalid product");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (products_.find(product.id) != products_.end()) return common::Status::AlreadyExists("product exists");
    if (store_ != nullptr) {
        std::string existing;
        if (store_->get("product:" + product.id, &existing).ok()) {
            return common::Status::AlreadyExists("product exists");
        }
        if (const auto status = store_->set("product:" + product.id, serialize(product)); !status.ok()) return status;
    }
    if (storage_ != nullptr) {
        if (const auto status = storage_->set("products/" + product.id, serialize(product)); !status.ok()) {
            if (store_ != nullptr) (void)store_->del("product:" + product.id);
            return status;
        }
    }
    products_.emplace(product.id, std::move(product));
    return common::Status::Ok();
}

common::Status ProductService::restore() {
    if (storage_ == nullptr) return common::Status::Ok();
    std::lock_guard<std::mutex> lock(mutex_);
    products_.clear();
    for (const auto& [key, value] : storage_->scanPrefix("products/")) {
        Product product;
        if (!deserialize(value, &product) || key != "products/" + product.id || product.id.empty()) {
            return common::Status::Internal("invalid persisted product");
        }
        if (!products_.emplace(product.id, std::move(product)).second) {
            return common::Status::Internal("duplicate persisted product");
        }
    }
    return common::Status::Ok();
}

common::Status ProductService::getProduct(const std::string& id, Product* product) {
    if (id.empty() || product == nullptr) return common::Status::InvalidArgument("invalid product query");
    if (cache_ != nullptr) {
        std::string encoded;
        const auto cache_status = cache_->getOrLoad(id, [this, &id](std::string* loaded) {
            if (store_ != nullptr) {
                const auto store_status = store_->get("product:" + id, loaded);
                if (store_status.ok()) return store_status;
                if (store_status.code() != common::ErrorCode::kNotFound) return store_status;
            }
            std::lock_guard<std::mutex> lock(mutex_);
            const auto it = products_.find(id);
            if (it == products_.end()) return common::Status::NotFound("product not found");
            *loaded = serialize(it->second);
            if (store_ != nullptr) {
                const auto store_status = store_->set("product:" + id, *loaded, std::chrono::hours(24));
                if (!store_status.ok()) return store_status;
            }
            return common::Status::Ok();
        }, &encoded, std::chrono::seconds(30));
        if (!cache_status.ok()) return cache_status;
        if (!deserialize(encoded, product)) return common::Status::Internal("invalid cached product");
        return common::Status::Ok();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = products_.find(id);
    if (it == products_.end()) return common::Status::NotFound("product not found");
    *product = it->second;
    return common::Status::Ok();
}

common::Status ProductService::setOnSale(const std::string& id, bool on_sale) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(id);
    if (it == products_.end() && store_ != nullptr) {
        std::string encoded;
        if (store_->get("product:" + id, &encoded).ok()) {
            Product shared;
            if (!deserialize(encoded, &shared)) return common::Status::Internal("invalid shared product");
            products_[id] = shared;
            it = products_.find(id);
        }
    }
    if (it == products_.end()) return common::Status::NotFound("product not found");
    const bool previous = it->second.on_sale;
    it->second.on_sale = on_sale;
    if (storage_ != nullptr) {
        const auto storage_status = storage_->set("products/" + id, serialize(it->second));
        if (!storage_status.ok()) {
            it->second.on_sale = previous;
            return storage_status;
        }
    }
    if (store_ != nullptr) {
        const auto store_status = store_->set("product:" + id, serialize(it->second), std::chrono::hours(24));
        if (!store_status.ok()) {
            it->second.on_sale = previous;
            if (storage_ != nullptr) storage_->set("products/" + id, serialize(it->second));
            return store_status;
        }
    }
    if (cache_ != nullptr) cache_->erase(id);
    return common::Status::Ok();
}

std::string ProductService::serialize(const Product& product) {
    std::ostringstream out;
    out << product.id << '\t' << product.name << '\t' << product.price_cents << '\t' << product.on_sale;
    return out.str();
}

bool ProductService::deserialize(const std::string& value, Product* product) {
    std::istringstream input(value);
    int on_sale = 0;
    return static_cast<bool>(std::getline(input, product->id, '\t') &&
                             std::getline(input, product->name, '\t') &&
                             (input >> product->price_cents) && input.get() == '\t' && (input >> on_sale)) &&
           (product->on_sale = on_sale != 0, true);
}

}  // namespace live::business::product
