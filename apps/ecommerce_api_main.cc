#include "business/inventory/inventory_service.h"
#include "business/inventory/inventory_gateway.h"
#include "business/inventory/inventory_reconciler.h"
#include "business/order/order_service.h"
#include "business/product/product_service.h"
#include "business/promotion/promotion_service.h"
#include "business/flash_sale/flash_sale_order_service.h"
#include "business/flash_sale/flash_sale_order_consumer.h"
#include "business/flash_sale/local_flash_sale_order_repository.h"
#include "business/user/user_service.h"
#include "cache/sharded_cache.h"
#include "gateway/http/http_server.h"
#include "messaging/async_outbox_dispatcher.h"
#include "messaging/in_memory_broker.h"
#include "messaging/order_queue.h"
#include "messaging/outbox.h"
#include "network/reactor/event_loop.h"
#include "observability/metrics/metrics_registry.h"
#include "observability/logging/structured_logger.h"
#include "storage/local_engine/local_kv_engine.h"
#include "storage/local_engine/snapshot_worker.h"
#include "traffic/token_bucket.h"
#ifdef ECOMMERCE_HAS_REDIS
#include "kv/redis_kv_store.h"
#include "kv/redis_flash_sale_gate.h"
#include "kv/redis_inventory_gateway.h"
#include "kv/redis_order_queue.h"
#endif
#ifdef ECOMMERCE_HAS_KAFKA
#include "messaging/kafka_client.h"
#include "messaging/kafka_dlq_operator.h"
#endif
#ifdef ECOMMERCE_HAS_MYSQL
#include "storage/mysql/mysql_transaction_store.h"
#endif

#include <cstdint>
#include <charconv>
#include <algorithm>
#include <cctype>
#include <csignal>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <sstream>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

volatile std::sig_atomic_t shutdown_requested = 0;

void requestShutdown(int) {
    shutdown_requested = 1;
}

std::string jsonEscape(const std::string& value) {
    std::string output;
    for (const char c : value) {
        switch (c) {
            case '"': output += "\\\""; break;
            case '\\': output += "\\\\"; break;
            case '\b': output += "\\b"; break;
            case '\f': output += "\\f"; break;
            case '\n': output += "\\n"; break;
            case '\r': output += "\\r"; break;
            case '\t': output += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    const char* digits = "0123456789abcdef";
                    output += "\\u00";
                    output.push_back(digits[(static_cast<unsigned char>(c) >> 4) & 0x0f]);
                    output.push_back(digits[static_cast<unsigned char>(c) & 0x0f]);
                } else {
                    output.push_back(c);
                }
        }
    }
    return output;
}

int hexValue(char value) {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

bool percentDecode(std::string_view encoded, std::string* decoded) {
    if (decoded == nullptr) return false;
    decoded->clear();
    decoded->reserve(encoded.size());
    for (std::size_t i = 0; i < encoded.size(); ++i) {
        if (encoded[i] == '+') {
            decoded->push_back(' ');
        } else if (encoded[i] == '%') {
            if (i + 2 >= encoded.size()) return false;
            const int high = hexValue(encoded[i + 1]);
            const int low = hexValue(encoded[i + 2]);
            if (high < 0 || low < 0) return false;
            decoded->push_back(static_cast<char>((high << 4) | low));
            i += 2;
        } else {
            decoded->push_back(encoded[i]);
        }
    }
    return true;
}

std::string formValue(const std::string& body, const std::string& key) {
    std::size_t begin = 0;
    while (begin <= body.size()) {
        const auto end = body.find('&', begin);
        const auto part_end = end == std::string::npos ? body.size() : end;
        const auto equal = body.find('=', begin);
        if (equal != std::string::npos && equal < part_end) {
            std::string decoded_key;
            std::string decoded_value;
            if (percentDecode(std::string_view(body).substr(begin, equal - begin), &decoded_key) &&
                decoded_key == key && percentDecode(std::string_view(body).substr(equal + 1, part_end - equal - 1), &decoded_value)) {
                return decoded_value;
            }
        }
        if (end == std::string::npos) break;
        begin = end + 1;
    }
    return {};
}

std::string queryValue(const std::string& target, const std::string& key) {
    const auto query = target.find('?');
    if (query == std::string::npos) return {};
    return formValue(target.substr(query + 1), key);
}

live::gateway::http::HttpResponse jsonResponse(int status, std::string reason, std::string body) {
    return {status, std::move(reason), {{"Content-Type", "application/json"}}, std::move(body)};
}

bool parseInt64(const std::string& text, std::int64_t* value) {
    if (value == nullptr || text.empty()) return false;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), *value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

std::size_t envSize(const char* name, std::size_t fallback, std::size_t maximum) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') return fallback;
    std::uint64_t parsed = 0;
    const auto result = std::from_chars(value, value + std::char_traits<char>::length(value), parsed);
    if (result.ec != std::errc{} || parsed == 0 || parsed > maximum) return fallback;
    return static_cast<std::size_t>(parsed);
}

std::vector<std::string> splitCsv(const char* value) {
    std::vector<std::string> result;
    if (value == nullptr) return result;
    std::string current;
    std::istringstream input(value);
    while (std::getline(input, current, ',')) if (!current.empty()) result.push_back(current);
    return result;
}

live::common::Status authenticateRequest(const live::gateway::http::HttpRequest& request,
                                         live::business::user::UserService* users,
                                         live::business::user::User* user) {
    if (users == nullptr || user == nullptr) return live::common::Status::Internal("authentication is unavailable");
    const auto authorization = request.headers.find("authorization");
    if (authorization == request.headers.end() || authorization->second.size() <= 7) {
        return live::common::Status::Unauthenticated("Bearer token is required");
    }
    const std::string scheme = authorization->second.substr(0, 6);
    const std::string normalized_scheme = [&] {
        std::string value = scheme;
        for (char& c : value) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return value;
    }();
    if (normalized_scheme != "bearer" || authorization->second[6] != ' ') {
        return live::common::Status::Unauthenticated("Bearer token is required");
    }
    return users->authenticate(authorization->second.substr(7), user);
}

live::gateway::http::HttpResponse unauthorizedResponse(const live::common::Status& status) {
    return jsonResponse(401, "Unauthorized", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
}

live::gateway::http::HttpResponse forbiddenResponse(const std::string& message) {
    return jsonResponse(403, "Forbidden", "{\"error\":\"" + jsonEscape(message) + "\"}");
}

}  // namespace

int main(int argc, char** argv) {
    std::signal(SIGINT, requestShutdown);
    std::signal(SIGTERM, requestShutdown);
#ifdef SIGPIPE
    std::signal(SIGPIPE, SIG_IGN);
#endif
    std::uint16_t port = 8080;
    if (argc > 1) port = static_cast<std::uint16_t>(std::strtoul(argv[1], nullptr, 10));

    live::network::EventLoop loop;
    live::storage::LocalKVEngine domain_storage;
    const char* snapshot_path = std::getenv("ECOMMERCE_STORAGE_SNAPSHOT");
    const char* wal_path = std::getenv("ECOMMERCE_STORAGE_WAL");
    const auto storage_status = domain_storage.open(
        snapshot_path == nullptr ? "/tmp/ecommerce-domain.snapshot" : snapshot_path,
        wal_path == nullptr ? "/tmp/ecommerce-domain.wal" : wal_path);
    if (!storage_status.ok()) {
        std::cerr << "failed to open domain storage: " << storage_status.message() << '\n';
        return 1;
    }
    live::storage::SnapshotWorker snapshot_worker(
        &domain_storage, std::chrono::seconds(envSize("ECOMMERCE_SNAPSHOT_INTERVAL_SECONDS", 30, 3600)));
    if (!snapshot_worker.start().ok()) {
        std::cerr << "failed to start snapshot worker\n";
        return 1;
    }
    live::cache::ShardedCache cache(16, 1024);
#ifdef ECOMMERCE_HAS_REDIS
    std::unique_ptr<live::kv::RedisKVStore> redis;
    const char* redis_host = std::getenv("ECOMMERCE_REDIS_HOST");
    if (redis_host != nullptr) {
        redis = std::make_unique<live::kv::RedisKVStore>();
        const auto redis_status = redis->connect(redis_host, 6379, std::chrono::seconds(2),
                                                 envSize("ECOMMERCE_REDIS_POOL_SIZE", 8, 128));
        if (!redis_status.ok()) {
            std::cerr << "Redis configured but unavailable: " << redis_status.message() << '\n';
            return 1;
        }
    }
#endif
    live::kv::IKVStore* shared_store = nullptr;
#ifdef ECOMMERCE_HAS_REDIS
    shared_store = redis.get();
#endif
    live::business::product::ProductService products(&cache,
        shared_store
        , &domain_storage
    );
    if (!products.restore().ok()) {
        std::cerr << "failed to restore products\n";
        return 1;
    }
    live::business::inventory::InventoryService inventory(&domain_storage);
    if (!inventory.restore().ok()) {
        std::cerr << "failed to restore inventory\n";
        return 1;
    }
    if (const auto invariant_status = inventory.validateInvariants(); !invariant_status.ok()) {
        std::cerr << "inventory invariant check failed: " << invariant_status.message() << '\n';
        return 1;
    }
    live::messaging::InMemoryBroker broker;
#ifdef ECOMMERCE_HAS_KAFKA
    std::unique_ptr<live::messaging::KafkaProducer> kafka;
    const char* kafka_brokers = std::getenv("ECOMMERCE_KAFKA_BROKERS");
    if (kafka_brokers != nullptr) {
        kafka = std::make_unique<live::messaging::KafkaProducer>();
        const auto kafka_status = kafka->start(kafka_brokers, "ecommerce-api");
        if (!kafka_status.ok()) {
            std::cerr << "Kafka configured but unavailable: " << kafka_status.message() << '\n';
            return 1;
        }
    }
#endif
#ifdef ECOMMERCE_HAS_MYSQL
    std::unique_ptr<live::storage::mysql::MySqlTransactionStore> mysql_store;
    const char* mysql_host = std::getenv("ECOMMERCE_MYSQL_HOST");
    const char* mysql_hosts_env = std::getenv("ECOMMERCE_MYSQL_HOSTS");
    const auto mysql_hosts = mysql_hosts_env != nullptr && *mysql_hosts_env != '\0'
        ? splitCsv(mysql_hosts_env) : splitCsv(mysql_host);
    if (!mysql_hosts.empty()) {
        mysql_store = std::make_unique<live::storage::mysql::MySqlTransactionStore>();
        const auto mysql_status = mysql_store->connect(
            mysql_hosts,
            static_cast<std::uint16_t>(envSize("ECOMMERCE_MYSQL_PORT", 3306, 65535)),
            std::getenv("ECOMMERCE_MYSQL_USER") == nullptr ? "ecommerce" : std::getenv("ECOMMERCE_MYSQL_USER"),
            std::getenv("ECOMMERCE_MYSQL_PASSWORD") == nullptr ? "ecommerce" : std::getenv("ECOMMERCE_MYSQL_PASSWORD"),
            std::getenv("ECOMMERCE_MYSQL_DATABASE") == nullptr ? "ecommerce" : std::getenv("ECOMMERCE_MYSQL_DATABASE"),
            envSize("ECOMMERCE_MYSQL_POOL_SIZE", 4, 64));
        if (!mysql_status.ok()) {
            std::cerr << "MySQL configured but unavailable: " << mysql_status.message() << '\n';
            return 1;
        }
        if (const auto sku_status = mysql_store->initializeSku("sku-1", 100); !sku_status.ok()) {
            std::cerr << "failed to initialize MySQL inventory: " << sku_status.message() << '\n';
            return 1;
        }
    }
#endif
    live::messaging::FileOutbox outbox;
    const char* configured_outbox = std::getenv("ECOMMERCE_OUTBOX_PATH");
    if (!outbox.open(configured_outbox == nullptr ? "/tmp/ecommerce-outbox.bin" : configured_outbox).ok()) {
        std::cerr << "failed to open durable outbox\n";
        return 1;
    }
    live::messaging::OutboxPublisher outbox_publisher(&outbox);
    live::business::order::OrderService orders(&inventory, &outbox_publisher, &domain_storage,
#ifdef ECOMMERCE_HAS_MYSQL
        mysql_store.get()
#else
        nullptr
#endif
    );
    if (!orders.restore().ok()) {
        std::cerr << "failed to restore orders\n";
        return 1;
    }
#ifdef ECOMMERCE_HAS_REDIS
    live::business::user::UserService users(&domain_storage, redis.get());
#else
    live::business::user::UserService users(&domain_storage);
#endif
    if (!users.restore().ok()) {
        std::cerr << "failed to restore users\n";
        return 1;
    }
    std::unique_ptr<live::business::promotion::IAdmissionGate> promotion_gate;
    const char* configured_inventory_mode = std::getenv("ECOMMERCE_INVENTORY_MODE");
    const std::string inventory_mode = configured_inventory_mode == nullptr
#ifdef ECOMMERCE_HAS_REDIS
        ? (redis != nullptr ? "redis" : "local")
#else
        ? "local"
#endif
        : configured_inventory_mode;
    const bool use_redis_flash_sale = inventory_mode == "redis";
#ifdef ECOMMERCE_HAS_REDIS
    if (use_redis_flash_sale && redis != nullptr) promotion_gate = std::make_unique<live::business::promotion::RedisFlashSaleGate>(redis.get());
#endif
    if (use_redis_flash_sale && promotion_gate == nullptr) {
        std::cerr << "ECOMMERCE_INVENTORY_MODE=redis requires a configured Redis connection\n";
        return 1;
    }
    if (promotion_gate == nullptr) promotion_gate = std::make_unique<live::business::promotion::FlashSaleAdmissionGate>();
    live::business::promotion::PromotionService promotions(std::move(promotion_gate));
    live::observability::MetricsRegistry metrics;
#ifdef ECOMMERCE_HAS_MYSQL
    if (mysql_store != nullptr) mysql_store->setMetrics(&metrics);
#endif
    const auto http_requests_metric = metrics.counterHandle("http_requests");
    const auto http_response_2xx_metric = metrics.counterHandle("http_responses_2xx");
    const auto http_response_3xx_metric = metrics.counterHandle("http_responses_3xx");
    const auto http_response_4xx_metric = metrics.counterHandle("http_responses_4xx");
    const auto http_response_5xx_metric = metrics.counterHandle("http_responses_5xx");
    const auto http_duration_metric = metrics.histogramHandle("http_request_duration_ms");
    const auto health_requests_metric = metrics.counterHandle("http_health_requests");
    const auto product_queries_metric = metrics.counterHandle("product_query_requests");
    live::observability::StructuredLogger request_logger(&std::clog);
    live::traffic::TokenBucket flash_sale_limiter(10000.0, 2000.0);
    const char* admin_token = std::getenv("ECOMMERCE_ADMIN_TOKEN");

    live::business::product::Product existing_product;
    if (products.getProduct("sku-1", &existing_product).code() == live::common::ErrorCode::kNotFound) {
        products.addProduct({"sku-1", "C++ 高并发实战课程", 19900, true});
    }
    live::business::inventory::StockSnapshot existing_stock;
    if (inventory.queryStock("sku-1", &existing_stock).code() == live::common::ErrorCode::kNotFound) inventory.addSku("sku-1", 100);
    promotions.create({"promo-1", "sku-1", 100});
    promotions.preheat("promo-1");
    promotions.start("promo-1");

    std::unique_ptr<live::business::inventory::IInventoryGateway> flash_inventory_gateway;
    std::unique_ptr<live::messaging::IOrderQueue> flash_order_queue;
    std::unique_ptr<live::messaging::IOrderQueue> flash_dead_letter_queue;
    const char* configured_flash_queue = std::getenv("ECOMMERCE_FLASH_SALE_QUEUE");
    const std::size_t flash_consumer_workers = envSize("ECOMMERCE_FLASH_CONSUMER_WORKERS", 4, 64);
    const std::string flash_queue_mode = configured_flash_queue == nullptr
        ? (use_redis_flash_sale ? "redis" : "local")
        : configured_flash_queue;
    const bool use_redis_flash_queue = flash_queue_mode == "redis";
    if (flash_queue_mode != "redis" && flash_queue_mode != "local") {
        std::cerr << "ECOMMERCE_FLASH_SALE_QUEUE must be either redis or local\n";
        return 1;
    }
#ifdef ECOMMERCE_HAS_REDIS
    if (use_redis_flash_sale && use_redis_flash_queue) {
        auto redis_inventory = std::make_unique<live::business::inventory::RedisInventoryGateway>(redis.get());
        if (const auto status = redis_inventory->initializeSku("sku-1", 100); !status.ok()) {
            std::cerr << "failed to initialize Redis flash-sale inventory: " << status.message() << '\n';
            return 1;
        }
        flash_inventory_gateway = std::move(redis_inventory);
        auto redis_order_queue = std::make_unique<live::kv::RedisOrderQueue>(redis.get(), "flash_orders:{promo-1}");
        if (const auto status = redis_order_queue->connectBlockingConsumer(redis_host, 6379,
                                                                            std::chrono::seconds(2),
                                                                            flash_consumer_workers); !status.ok()) {
            std::cerr << "failed to create dedicated Redis queue consumer connection: " << status.message() << '\n';
            return 1;
        }
        flash_order_queue = std::move(redis_order_queue);
        flash_dead_letter_queue = std::make_unique<live::kv::RedisOrderQueue>(redis.get(), "flash_orders_dlq:{promo-1}");
    } else
#endif
    {
        if (use_redis_flash_queue) {
            std::cerr << "ECOMMERCE_FLASH_SALE_QUEUE=redis requires Redis inventory mode and connection\n";
            return 1;
        }
        flash_inventory_gateway = std::make_unique<live::business::inventory::LocalInventoryGateway>(&inventory);
        flash_order_queue = std::make_unique<live::messaging::InMemoryOrderQueue>();
        flash_dead_letter_queue = std::make_unique<live::messaging::InMemoryOrderQueue>();
    }
    live::business::flash_sale::LocalFlashSaleOrderRepository local_flash_repo(&orders);
    live::business::flash_sale::IFlashSaleOrderRepository* flash_repo = &local_flash_repo;
#ifdef ECOMMERCE_HAS_MYSQL
    if (mysql_store != nullptr) flash_repo = mysql_store.get();
#endif
    std::unique_ptr<live::business::flash_sale::FlashSaleOrderConsumer> flash_consumer;
    flash_consumer = std::make_unique<live::business::flash_sale::FlashSaleOrderConsumer>(
        flash_order_queue.get(), flash_dead_letter_queue.get(), flash_repo, flash_inventory_gateway.get(),
        promotions.admissionGate(), 5, flash_consumer_workers);
    if (!flash_consumer->start().ok()) {
        std::cerr << "failed to start flash-sale consumer\n";
        return 1;
    }
    auto flash_order_service = std::make_unique<live::business::flash_sale::FlashSaleOrderService>(
        promotions.admissionGate(), flash_inventory_gateway.get(), flash_order_queue.get());

    std::unique_ptr<live::business::inventory::InventoryReconciler> inventory_reconciler;
#ifdef ECOMMERCE_HAS_MYSQL
    if (mysql_store != nullptr) {
        inventory_reconciler = std::make_unique<live::business::inventory::InventoryReconciler>(
            mysql_store.get(), flash_inventory_gateway.get());
    }
#endif

    auto publishEvent = [&](const live::messaging::Event& event) {
#ifdef ECOMMERCE_HAS_KAFKA
        if (kafka != nullptr) return kafka->publish(event);
#endif
        return broker.publish(event);
    };
#ifdef ECOMMERCE_HAS_MYSQL
    std::atomic<bool> mysql_outbox_stop{false};
    std::thread mysql_outbox_thread;
    if (mysql_store != nullptr) {
        mysql_outbox_thread = std::thread([&] {
            while (!mysql_outbox_stop.load(std::memory_order_relaxed)) {
                (void)mysql_store->drainOutbox(publishEvent);
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            }
        });
    }
#endif
    // Replay records left by a previous process before serving new traffic.
    if (const auto replay_status = outbox.drain(publishEvent); !replay_status.ok()) {
        std::cerr << "outbox replay is pending: " << replay_status.message() << '\n';
    }
    live::messaging::AsyncOutboxDispatcher outbox_dispatcher(&outbox, publishEvent);
    if (!outbox_dispatcher.start().ok()) {
        std::cerr << "failed to start outbox dispatcher\n";
        return 1;
    }

    const std::size_t http_max_connections = envSize("ECOMMERCE_HTTP_MAX_CONNECTIONS", 10000, 100000);
    const std::size_t http_worker_count = envSize("ECOMMERCE_HTTP_WORKERS", 8, 256);
    const std::size_t http_queue_capacity = envSize("ECOMMERCE_HTTP_QUEUE_CAPACITY", 4096, 100000);
    live::gateway::http::HttpServer server(&loop, "0.0.0.0", port, http_max_connections,
        [http_requests_metric, http_response_2xx_metric, http_response_3xx_metric,
         http_response_4xx_metric, http_response_5xx_metric, http_duration_metric,
         &request_logger](const live::gateway::http::HttpRequest& request,
                   const live::gateway::http::HttpResponse& response,
                   std::uint64_t elapsed_us) {
            http_requests_metric.add();
            switch (response.status_code / 100) {
                case 2: http_response_2xx_metric.add(); break;
                case 3: http_response_3xx_metric.add(); break;
                case 4: http_response_4xx_metric.add(); break;
                case 5: http_response_5xx_metric.add(); break;
                default: break;
            }
            http_duration_metric.observe(static_cast<double>(elapsed_us) / 1000.0);
            if (response.status_code >= 400) request_logger.logHttpError(request, response, elapsed_us);
        }, http_worker_count, http_queue_capacity);
    server.router().addRoute("POST", "/users/register", [&](const auto& request) {
        live::business::user::User user;
        const auto status = users.registerUser(formValue(request.body, "username"), formValue(request.body, "password"),
                                               live::business::user::UserRole::kCustomer, &user);
        if (!status.ok()) return jsonResponse(409, "Conflict", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        return jsonResponse(201, "Created", "{\"user_id\":\"" + jsonEscape(user.id) + "\"}");
    });
    server.router().addRoute("POST", "/users/login", [&](const auto& request) {
        live::business::user::Session session;
        const auto status = users.login(formValue(request.body, "username"), formValue(request.body, "password"), &session);
        if (!status.ok()) return jsonResponse(401, "Unauthorized", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        return jsonResponse(200, "OK", "{\"token\":\"" + jsonEscape(session.token) + "\",\"user_id\":\"" + jsonEscape(session.user.id) +
            "\",\"expires_at\":" + std::to_string(session.expires_at_unix_seconds) + "}");
    });
    server.router().addRoute("POST", "/users/logout", [&](const auto& request) {
        const auto authorization = request.headers.find("authorization");
        if (authorization == request.headers.end() || authorization->second.size() <= 7) {
            return jsonResponse(401, "Unauthorized", "{\"error\":\"Bearer token is required\"}");
        }
        const auto status = users.logout(authorization->second.substr(7));
        if (!status.ok() && status.code() != live::common::ErrorCode::kNotFound) {
            return jsonResponse(401, "Unauthorized", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        }
        return jsonResponse(204, "No Content", "");
    });
    server.router().addRoute("GET", "/health", [&](const auto&) {
        health_requests_metric.add();
        return jsonResponse(200, "OK", "{\"status\":\"ok\"}");
    });
    server.router().addRoute("GET", "/products/sku-1", [&](const auto&) {
        live::business::product::Product product;
        if (!products.getProduct("sku-1", &product).ok()) return jsonResponse(404, "Not Found", "{\"error\":\"product not found\"}");
        std::ostringstream body;
        body << "{\"id\":\"" << jsonEscape(product.id) << "\",\"name\":\"" << jsonEscape(product.name)
             << "\",\"price_cents\":" << product.price_cents << ",\"on_sale\":" << (product.on_sale ? "true" : "false") << "}";
        product_queries_metric.add();
        return jsonResponse(200, "OK", body.str());
    });
    server.router().addRoute("GET", "/stock/sku-1", [&](const auto&) {
        live::business::inventory::StockSnapshot stock;
        const auto stock_status =
#ifdef ECOMMERCE_HAS_MYSQL
            mysql_store != nullptr ? mysql_store->queryStock("sku-1", &stock) : flash_inventory_gateway->queryStock("sku-1", &stock);
#else
            flash_inventory_gateway->queryStock("sku-1", &stock);
#endif
        if (!stock_status.ok()) {
            if (stock_status.code() == live::common::ErrorCode::kNotFound) {
                return jsonResponse(404, "Not Found", "{\"error\":\"sku not found\"}");
            }
            return jsonResponse(503, "Service Unavailable", "{\"error\":\"inventory dependency unavailable\"}");
        }
        std::ostringstream body;
        body << "{\"available\":" << stock.available << ",\"reserved\":" << stock.reserved << ",\"sold\":" << stock.sold << "}";
        return jsonResponse(200, "OK", body.str());
    });
    server.router().addRoute("POST", "/flash-sale/orders", [&](const auto& request) {
        live::business::user::User current;
        if (const auto auth = authenticateRequest(request, &users, &current); !auth.ok()) return unauthorizedResponse(auth);
        if (!flash_sale_limiter.tryAcquire()) return jsonResponse(429, "Too Many Requests", "{\"error\":\"rate limited\"}");
        live::business::flash_sale::FlashSaleSubmitRequest submit;
        submit.promotion_id = formValue(request.body, "promotion_id");
        submit.sku_id = formValue(request.body, "sku_id");
        submit.idempotency_key = formValue(request.body, "idempotency_key");
        if (submit.promotion_id.empty()) submit.promotion_id = "promo-1";
        if (submit.sku_id.empty()) submit.sku_id = "sku-1";
        submit.user_id = current.id;
        const auto quantity_text = formValue(request.body, "quantity");
        if (!quantity_text.empty() && !parseInt64(quantity_text, &submit.quantity)) {
            return jsonResponse(400, "Bad Request", "{\"error\":\"invalid quantity\"}");
        }
        if (submit.idempotency_key.empty()) return jsonResponse(400, "Bad Request", "{\"error\":\"idempotency_key is required\"}");
        live::business::flash_sale::FlashSaleSubmitResult result;
        const auto status = flash_order_service->submit(submit, &result);
        if (!status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        const char* state = result.code == live::business::flash_sale::SubmitCode::kQueued ? "QUEUED" :
            (result.code == live::business::flash_sale::SubmitCode::kDuplicate ? "DUPLICATE" : "REJECTED");
        const int http_status = result.code == live::business::flash_sale::SubmitCode::kQueued ? 202 :
            (result.code == live::business::flash_sale::SubmitCode::kDuplicate ? 200 : 409);
        return jsonResponse(http_status, http_status == 202 ? "Accepted" : (http_status == 200 ? "OK" : "Conflict"),
            "{\"order_id\":\"" + jsonEscape(result.order_id) + "\",\"state\":\"" + state + "\",\"message\":\"" + jsonEscape(result.message) + "\"}");
    });
    server.router().addRoute("GET", "/metrics", [&](const auto&) {
        return live::gateway::http::HttpResponse{200, "OK", {{"Content-Type", "text/plain; version=0.0.4"}}, metrics.renderPrometheus()};
    });
    server.router().addRoute("GET", "/admin/reconcile", [&](const auto& request) {
        if (admin_token == nullptr || std::string(admin_token).empty()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"admin token is not configured\"}");
        const auto token = request.headers.find("x-admin-token");
        if (token == request.headers.end() || token->second != admin_token) return jsonResponse(401, "Unauthorized", "{\"error\":\"invalid admin token\"}");
        if (inventory_reconciler != nullptr) {
            const std::string configured_skus = queryValue(request.target, "skus");
            const auto skus = splitCsv(configured_skus.empty() ? "sku-1" : configured_skus.c_str());
            live::business::inventory::ReconcileReport report;
            const auto status = inventory_reconciler->reconcile(skus, &report);
            metrics.increment(status.ok() ? "reconcile_success" : "reconcile_failure");
            if (!status.ok()) return jsonResponse(409, "Conflict", "{\"status\":\"failed\",\"error\":\"" + jsonEscape(status.message()) + "\"}");
            metrics.increment("reconcile_mismatch_total", report.mismatches);
            metrics.increment("reconcile_repaired_total", report.repaired);
            metrics.increment("reconcile_blocked_total", report.blocked);
            return jsonResponse(200, "OK", "{\"status\":\"ok\",\"checked\":" + std::to_string(report.checked) +
                ",\"mismatches\":" + std::to_string(report.mismatches) + ",\"repaired\":" + std::to_string(report.repaired) +
                ",\"blocked\":" + std::to_string(report.blocked) + "}");
        }
        const auto status = inventory.validateInvariants();
        metrics.increment(status.ok() ? "reconcile_success" : "reconcile_failure");
        if (!status.ok()) return jsonResponse(500, "Internal Server Error", "{\"status\":\"failed\",\"error\":\"" + jsonEscape(status.message()) + "\"}");
        return jsonResponse(200, "OK", "{\"status\":\"ok\"}");
    });
    server.router().addRoute("POST", "/admin/dlq/replay", [&](const auto& request) {
        if (admin_token == nullptr || std::string(admin_token).empty()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"admin token is not configured\"}");
        const auto token = request.headers.find("x-admin-token");
        if (token == request.headers.end() || token->second != admin_token) return jsonResponse(401, "Unauthorized", "{\"error\":\"invalid admin token\"}");
#ifdef ECOMMERCE_HAS_KAFKA
        if (kafka_brokers == nullptr || kafka == nullptr) return jsonResponse(503, "Service Unavailable", "{\"error\":\"Kafka is not configured\"}");
        const std::string dlq_topic = formValue(request.body, "topic").empty()
            ? (std::getenv("ECOMMERCE_FLASH_SALE_TOPIC") == nullptr ? "flash-sale-orders.DLQ" : std::string(std::getenv("ECOMMERCE_FLASH_SALE_TOPIC")) + ".DLQ")
            : formValue(request.body, "topic");
        const std::string target_topic = formValue(request.body, "target_topic").empty()
            ? (std::getenv("ECOMMERCE_FLASH_SALE_TOPIC") == nullptr ? "flash-sale-orders" : std::getenv("ECOMMERCE_FLASH_SALE_TOPIC"))
            : formValue(request.body, "target_topic");
        std::int64_t max_messages = 10;
        const auto max_text = formValue(request.body, "max_messages");
        if (!max_text.empty() && !parseInt64(max_text, &max_messages)) return jsonResponse(400, "Bad Request", "{\"error\":\"invalid max_messages\"}");
        std::size_t replayed = 0;
        const auto status = live::messaging::KafkaDlqOperator::replay(kafka_brokers, dlq_topic, target_topic,
                                                                        static_cast<std::size_t>(std::max<std::int64_t>(1, std::min<std::int64_t>(max_messages, 1000))), &replayed);
        if (!status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        metrics.increment("kafka_dlq_replayed_total", replayed);
        return jsonResponse(200, "OK", "{\"replayed\":" + std::to_string(replayed) + "}");
#else
        return jsonResponse(503, "Service Unavailable", "{\"error\":\"Kafka support is not compiled\"}");
#endif
    });
    server.router().addRoute("GET", "/orders", [&](const auto& request) {
        live::business::user::User current;
        if (const auto auth = authenticateRequest(request, &users, &current); !auth.ok()) return unauthorizedResponse(auth);
        live::business::order::Order order;
        if (!orders.getOrder(queryValue(request.target, "order_id"), &order).ok()) return jsonResponse(404, "Not Found", "{\"error\":\"order not found\"}");
        if (order.user_id != current.id) return forbiddenResponse("order belongs to another user");
        const char* state = "CREATED";
        return jsonResponse(200, "OK", "{\"order_id\":\"" + jsonEscape(order.id) + "\",\"state\":\"" + state + "\"}");
    });
    server.router().addRoute("GET", "/promotions/promo-1", [&](const auto&) {
        live::business::promotion::Promotion promotion;
        if (!promotions.get("promo-1", &promotion).ok()) return jsonResponse(404, "Not Found", "{\"error\":\"promotion not found\"}");
        const char* state = promotion.state == live::business::promotion::PromotionState::kRunning ? "RUNNING" :
            (promotion.state == live::business::promotion::PromotionState::kFinished ? "FINISHED" : "PREHEATING");
        return jsonResponse(200, "OK", "{\"promotion_id\":\"" + promotion.id + "\",\"state\":\"" + state + "\",\"limit\":" + std::to_string(promotion.participant_limit) + "}");
    });
    const auto status = server.start();
    if (!status.ok()) {
        std::cerr << "failed to start gateway: " << status.message() << '\n';
        return 1;
    }
    std::cout << "ecommerce API listening on 0.0.0.0:" << server.port() << '\n';
    std::thread loop_thread([&loop] { loop.loop(); });
    while (shutdown_requested == 0) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // Stop accepting on the event-loop thread, then drain HTTP workers while
    // the loop is still alive so their response callbacks are not lost.
    std::promise<void> accepting_stopped;
    auto accepting_stopped_future = accepting_stopped.get_future();
    loop.runInLoop([&server, &accepting_stopped] {
        server.stopAcceptingInLoop();
        accepting_stopped.set_value();
    });
    accepting_stopped_future.wait();
    server.stopWorkers();
    loop.runInLoop([&loop, &server] {
        server.stopInLoop();
        loop.quit();
    });
    loop_thread.join();
    if (flash_consumer != nullptr) flash_consumer->stop();
    outbox_dispatcher.stop();
#ifdef ECOMMERCE_HAS_MYSQL
    mysql_outbox_stop.store(true, std::memory_order_relaxed);
    if (mysql_outbox_thread.joinable()) mysql_outbox_thread.join();
#endif
    snapshot_worker.stop();
    return 0;
}
