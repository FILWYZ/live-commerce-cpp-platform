#include "storage/mysql/mysql_transaction_store.h"

#include "business/inventory/inventory_service.h"
#include "business/order/order_service.h"

#include <vector>

namespace live::storage::mysql {
namespace {

std::string mysqlError(MYSQL* connection, const char* fallback) {
    if (connection == nullptr) return fallback;
    const char* error = mysql_error(connection);
    return error == nullptr || *error == '\0' ? fallback : error;
}

::live::business::order::OrderState parseState(const char* value) {
    (void)value;
    return ::live::business::order::OrderState::kCreated;
}

common::Status readOrder(MYSQL* connection, const std::string& sql, ::live::business::order::Order* order) {
    if (connection == nullptr || order == nullptr) return common::Status::InvalidArgument("invalid MySQL order query");
    if (mysql_query(connection, sql.c_str()) != 0) return common::Status::Internal(mysqlError(connection, "MySQL order query failed"));
    MYSQL_RES* result = mysql_store_result(connection);
    if (result == nullptr) return common::Status::Internal(mysqlError(connection, "MySQL order result failed"));
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == nullptr) { mysql_free_result(result); return common::Status::NotFound("order not found"); }
    order->id = row[0] == nullptr ? "" : row[0]; order->user_id = row[1] == nullptr ? "" : row[1];
    order->idempotency_key = row[2] == nullptr ? "" : row[2]; order->state = parseState(row[3]);
    order->sku_id = row[4] == nullptr ? "" : row[4]; order->quantity = row[5] == nullptr ? 0 : std::stoll(row[5]);
    mysql_free_result(result); return common::Status::Ok();
}

}  // namespace

MySqlTransactionStore::MySqlTransactionStore() : pool_(std::make_unique<MySqlConnectionPool>()) {}
MySqlTransactionStore::~MySqlTransactionStore() { close(); }

common::Status MySqlTransactionStore::connect(const std::string& host, std::uint16_t port, const std::string& user,
                                              const std::string& password, const std::string& database) {
    return connect({host}, port, user, password, database, 4);
}
common::Status MySqlTransactionStore::connect(const std::vector<std::string>& hosts, std::uint16_t port,
                                              const std::string& user, const std::string& password,
                                              const std::string& database, std::size_t pool_size) {
    return pool_->connect(hosts, port, user, password, database, pool_size);
}
void MySqlTransactionStore::close() { if (pool_ != nullptr) pool_->close(); }
bool MySqlTransactionStore::connected() const { return pool_ != nullptr && pool_->connected(); }

common::Status MySqlTransactionStore::execute(MYSQL* connection, const std::string& sql) const {
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL connection is not available");
    if (mysql_query(connection, sql.c_str()) != 0) return common::Status::Internal(mysqlError(connection, "MySQL query failed"));
    MYSQL_RES* result = mysql_store_result(connection); if (result != nullptr) mysql_free_result(result);
    return common::Status::Ok();
}
common::Status MySqlTransactionStore::begin(MYSQL* connection) const { return execute(connection, "START TRANSACTION"); }
common::Status MySqlTransactionStore::rollback(MYSQL* connection) const { return execute(connection, "ROLLBACK"); }
common::Status MySqlTransactionStore::commit(MYSQL* connection) const { return execute(connection, "COMMIT"); }

std::string MySqlTransactionStore::quote(MYSQL* connection, const std::string& value) const {
    if (connection == nullptr) return "''";
    std::string escaped(value.size() * 2 + 1, '\0');
    const auto size = mysql_real_escape_string(connection, escaped.data(), value.data(), value.size());
    escaped.resize(size); return "'" + escaped + "'";
}

common::Status MySqlTransactionStore::loadOrder(MYSQL* connection, const std::string& order_id,
                                                ::live::business::order::Order* order) const {
    return readOrder(connection, "SELECT o.id,o.user_id,o.idempotency_key,o.state,oi.sku_id,oi.quantity FROM orders o JOIN order_items oi ON oi.order_id=o.id WHERE o.id=" + quote(connection, order_id) + " LIMIT 1", order);
}

common::Status MySqlTransactionStore::appendOutbox(MYSQL* connection, const ::live::messaging::Event& event) const {
    if (event.event_id.empty() || event.topic.empty()) return common::Status::InvalidArgument("invalid outbox event");
    return execute(connection, "INSERT INTO outbox_events(event_id,topic,event_key,payload,published) VALUES (" + quote(connection, event.event_id) + "," + quote(connection, event.topic) + "," + quote(connection, event.key) + "," + quote(connection, event.payload) + ",FALSE) ON DUPLICATE KEY UPDATE topic=VALUES(topic),event_key=VALUES(event_key),payload=VALUES(payload)");
}

common::Status MySqlTransactionStore::persistQueuedOrder(const ::live::messaging::FlashSaleOrderMessage& message,
                                                         ::live::business::order::Order* order) {
    if (!message.validate().ok() || order == nullptr) return common::Status::InvalidArgument("invalid queued order message");
    auto lease = pool_->acquire(); MYSQL* connection = lease.get(); if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    if (const auto status = begin(connection); !status.ok()) return status;
    const std::string sql = "SELECT o.id,o.user_id,o.idempotency_key,o.state,oi.sku_id,oi.quantity FROM orders o JOIN order_items oi ON oi.order_id=o.id WHERE o.idempotency_key=" + quote(connection, message.idempotency_key) + " FOR UPDATE";
    if (mysql_query(connection, sql.c_str()) != 0) { rollback(connection); return common::Status::Internal(mysqlError(connection, "MySQL flash-sale idempotency query failed")); }
    MYSQL_RES* result = mysql_store_result(connection); if (result == nullptr) { rollback(connection); return common::Status::Internal(mysqlError(connection, "MySQL flash-sale idempotency result failed")); }
    MYSQL_ROW existing = mysql_fetch_row(result);
    if (existing != nullptr) {
        order->id = existing[0] == nullptr ? "" : existing[0]; order->user_id = existing[1] == nullptr ? "" : existing[1]; order->idempotency_key = existing[2] == nullptr ? "" : existing[2]; order->state = parseState(existing[3]); order->sku_id = existing[4] == nullptr ? "" : existing[4]; order->quantity = existing[5] == nullptr ? 0 : std::stoll(existing[5]); mysql_free_result(result);
        if (order->id != message.order_id || order->user_id != message.user_id || order->sku_id != message.sku_id || order->quantity != message.quantity) { rollback(connection); return common::Status::FailedPrecondition("flash-sale message conflicts with persisted order"); }
        return commit(connection);
    }
    mysql_free_result(result);
    if (const auto status = execute(connection, "UPDATE inventory SET available=available-" + std::to_string(message.quantity) + ",reserved=reserved+" + std::to_string(message.quantity) + ",version=version+1 WHERE sku_id=" + quote(connection, message.sku_id) + " AND available>=" + std::to_string(message.quantity)); !status.ok()) { rollback(connection); return status; }
    if (mysql_affected_rows(connection) != 1) { rollback(connection); return common::Status::ResourceExhausted("insufficient or missing MySQL inventory for flash-sale order"); }
    if (const auto status = execute(connection, "INSERT INTO orders(id,user_id,idempotency_key,state) VALUES (" + quote(connection, message.order_id) + "," + quote(connection, message.user_id) + "," + quote(connection, message.idempotency_key) + ",'CREATED')"); !status.ok()) { rollback(connection); return status; }
    if (const auto status = execute(connection, "INSERT INTO order_items(order_id,sku_id,quantity) VALUES (" + quote(connection, message.order_id) + "," + quote(connection, message.sku_id) + "," + std::to_string(message.quantity) + ")"); !status.ok()) { rollback(connection); return status; }
    ::live::messaging::Event event{"flash-order-created-" + message.order_id, "OrderCreated", message.order_id, message.order_id + "|" + message.user_id + "|" + message.sku_id + "|" + std::to_string(message.quantity), 0};
    if (const auto status = appendOutbox(connection, event); !status.ok()) { rollback(connection); return status; }
    if (const auto status = commit(connection); !status.ok()) return status;
    *order = {message.order_id, message.idempotency_key, message.user_id, message.sku_id, message.quantity, ::live::business::order::OrderState::kCreated}; return common::Status::Ok();
}

common::Status MySqlTransactionStore::getOrder(const std::string& order_id, ::live::business::order::Order* order) const {
    auto lease = pool_->acquire(); return loadOrder(lease.get(), order_id, order);
}
common::Status MySqlTransactionStore::initializeSku(const std::string& sku_id, std::int64_t quantity) {
    if (sku_id.empty() || quantity <= 0) return common::Status::InvalidArgument("invalid MySQL SKU");
    auto lease = pool_->acquire(); MYSQL* connection = lease.get(); return execute(connection, "INSERT INTO inventory(sku_id,available,reserved,sold,version) VALUES (" + quote(connection, sku_id) + "," + std::to_string(quantity) + ",0,0,0) ON DUPLICATE KEY UPDATE sku_id=VALUES(sku_id)");
}
common::Status MySqlTransactionStore::queryStock(const std::string& sku_id, ::live::business::inventory::StockSnapshot* snapshot) const {
    if (sku_id.empty() || snapshot == nullptr) return common::Status::InvalidArgument("invalid MySQL stock query");
    auto lease = pool_->acquire(); MYSQL* connection = lease.get(); if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    const auto sql = "SELECT available,reserved,sold FROM inventory WHERE sku_id=" + quote(connection, sku_id);
    if (mysql_query(connection, sql.c_str()) != 0) return common::Status::Internal(mysqlError(connection, "MySQL stock query failed"));
    MYSQL_RES* result = mysql_store_result(connection); if (result == nullptr) return common::Status::Internal(mysqlError(connection, "MySQL stock result failed"));
    MYSQL_ROW row = mysql_fetch_row(result); if (row == nullptr) { mysql_free_result(result); return common::Status::NotFound("sku not found"); }
    snapshot->available = row[0] == nullptr ? 0 : std::stoll(row[0]); snapshot->reserved = row[1] == nullptr ? 0 : std::stoll(row[1]); snapshot->sold = row[2] == nullptr ? 0 : std::stoll(row[2]); mysql_free_result(result); return common::Status::Ok();
}
common::Status MySqlTransactionStore::drainOutbox(const std::function<common::Status(const ::live::messaging::Event&)>& publish, std::size_t limit) {
    if (!publish || limit == 0 || limit > 1000) return common::Status::InvalidArgument("invalid MySQL outbox drain arguments");
    std::vector<::live::messaging::Event> events; auto lease = pool_->acquire(); MYSQL* connection = lease.get(); if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    const auto sql = "SELECT event_id,topic,event_key,payload FROM outbox_events WHERE published=FALSE ORDER BY created_at,event_id LIMIT " + std::to_string(limit);
    if (mysql_query(connection, sql.c_str()) != 0) return common::Status::Internal(mysqlError(connection, "MySQL outbox query failed"));
    MYSQL_RES* result = mysql_store_result(connection); if (result == nullptr) return common::Status::Internal(mysqlError(connection, "MySQL outbox result failed")); MYSQL_ROW row = nullptr;
    while ((row = mysql_fetch_row(result)) != nullptr) { ::live::messaging::Event event; event.event_id = row[0] == nullptr ? "" : row[0]; event.topic = row[1] == nullptr ? "" : row[1]; event.key = row[2] == nullptr ? "" : row[2]; event.payload = row[3] == nullptr ? "" : row[3]; events.push_back(std::move(event)); }
    mysql_free_result(result);
    for (const auto& event : events) { if (const auto status = publish(event); !status.ok()) return status; auto update_lease = pool_->acquire(); MYSQL* update_connection = update_lease.get(); if (const auto status = execute(update_connection, "UPDATE outbox_events SET published=TRUE WHERE event_id=" + quote(update_connection, event.event_id) + " AND published=FALSE"); !status.ok()) return status; }
    return common::Status::Ok();
}

}  // namespace live::storage::mysql
